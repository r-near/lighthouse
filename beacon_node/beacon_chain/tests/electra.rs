#![cfg(not(debug_assertions))] // Tests run too slow in debug.

use beacon_chain::test_utils::BeaconChainHarness;
use state_processing::{
    per_block_processing, BlockSignatureStrategy, ConsensusContext, VerifyBlockRoot,
};
use std::sync::Arc;
use types::*;

type E = MainnetEthSpec;

#[tokio::test]
async fn signature_verify_chain_segment_pubkey_cache() {
    let initial_validator_count = 32;

    let deposit_slot = Slot::new(4 * E::slots_per_epoch() - 1);
    let pre_deposit_slot = deposit_slot - 1;
    let spec = Arc::new(ForkName::Electra.make_genesis_spec(E::default_spec()));

    let harness = BeaconChainHarness::builder(E::default())
        .spec(spec.clone())
        .logger(logging::test_logger())
        .deterministic_keypairs(initial_validator_count)
        .fresh_ephemeral_store()
        .mock_execution_layer()
        .build();

    harness
        .execution_block_generator()
        .move_to_terminal_block()
        .unwrap();

    Box::pin(harness.extend_to_slot(pre_deposit_slot)).await;

    // Create a block with a deposit for a new validator.
    let pre_deposit_state = harness.get_current_state();
    assert_eq!(pre_deposit_state.slot(), pre_deposit_slot);
    assert_eq!(pre_deposit_state.fork_name_unchecked(), ForkName::Electra);

    // FIXME: Probably need to make this deterministic?
    let new_keypair = Keypair::random();
    let new_validator_pk_bytes = PublicKeyBytes::from(&new_keypair.pk);
    let withdrawal_credentials = Hash256::ZERO;
    let amount = spec.min_per_epoch_churn_limit_electra;
    let deposit_data = harness.make_deposit_data(&new_keypair, withdrawal_credentials, amount);
    let deposit_request = DepositRequest {
        pubkey: deposit_data.pubkey,
        withdrawal_credentials: deposit_data.withdrawal_credentials,
        amount: deposit_data.amount,
        signature: deposit_data.signature,
        index: 0,
    };

    let ((jank_block, blobs), mut state) = harness
        .make_block_with_modifier(pre_deposit_state, deposit_slot, |block| {
            block
                .body_mut()
                .execution_requests_mut()
                .unwrap()
                .deposits
                .push(deposit_request)
                .unwrap();
        })
        .await;

    // Compute correct state root.
    // FIXME: this is kinda nasty
    let mut ctxt = ConsensusContext::new(jank_block.slot());
    per_block_processing(
        &mut state,
        &jank_block,
        BlockSignatureStrategy::VerifyIndividual,
        VerifyBlockRoot::True,
        &mut ctxt,
        &spec,
    )
    .unwrap();
    let (mut block, _) = (*jank_block).clone().deconstruct();
    *block.state_root_mut() = state.update_tree_hash_cache().unwrap();
    let proposer_index = block.proposer_index() as usize;
    let signed_block = Arc::new(block.sign(
        &harness.validator_keypairs[proposer_index].sk,
        &state.fork(),
        state.genesis_validators_root(),
        &spec,
    ));
    let block_root = signed_block.canonical_root();
    let block_contents = (signed_block, blobs);

    harness
        .process_block(deposit_slot, block_root, block_contents)
        .await
        .unwrap();

    let post_block_state = harness.get_current_state();
    assert_eq!(post_block_state.pending_deposits().unwrap().len(), 1);
    assert_eq!(post_block_state.validators().len(), initial_validator_count);

    // Advance to one slot before the finalization of the deposit.
    Box::pin(harness.extend_to_slot(deposit_slot + 2 * E::slots_per_epoch())).await;
    let pre_finalized_deposit_state = harness.get_current_state();
    assert_eq!(
        pre_finalized_deposit_state.validators().len(),
        initial_validator_count
    );

    // New validator should not be in the pubkey cache yet.
    assert_eq!(
        harness
            .chain
            .validator_index(&new_validator_pk_bytes)
            .unwrap(),
        None
    );
    let new_validator_index = initial_validator_count as u64;

    // Keep producing blocks (but not processing them) until we find one signed by our new
    // validator.
    // FIXME: probably need to use the harness so we can prepare payloads properly
    let mut state = pre_finalized_deposit_state;
    let mut slot = state.slot() + 1;
    let mut blocks = vec![];
    loop {
        let (block, post_state) = harness.make_block(state, slot).await;
        let proposer_index = block.0.message().proposer_index();

        blocks.push(block);

        state = post_state;
        slot = slot + 1;

        if proposer_index == new_validator_index {
            break;
        }
    }

    // New validator should still not be in the pubkey cache yet.
    assert_eq!(
        harness
            .chain
            .validator_index(&new_validator_pk_bytes)
            .unwrap(),
        None
    );
}
