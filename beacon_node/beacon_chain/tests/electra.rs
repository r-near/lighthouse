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
    let withdrawal_credentials = Hash256::ZERO;
    let amount = spec.max_effective_balance_electra;
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

    // Finalize deposit.
    // FIXME(sproul): this was intended just for testing, but it doesn't work yet
    Box::pin(harness.extend_to_slot(deposit_slot + 2 * E::slots_per_epoch() + 2)).await;
    let finalized_deposit_state = harness.get_current_state();
    assert_eq!(
        finalized_deposit_state.validators().len(),
        initial_validator_count + 1
    );
}
