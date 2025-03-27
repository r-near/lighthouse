#![cfg(not(debug_assertions))] // Tests run too slow in debug.

use beacon_chain::{
    builder::BeaconChainBuilder,
    test_utils::{get_kzg, mock_execution_layer_from_parts, BeaconChainHarness, DiskHarnessType},
    ChainConfig, MigratorConfig, StateSkipConfig,
};
use logging::test_logger;
use slot_clock::{SlotClock, TestingSlotClock};
use state_processing::{
    per_block_processing, BlockSignatureStrategy, ConsensusContext, VerifyBlockRoot,
};
use std::sync::Arc;
use std::time::Duration;
use store::{database::interface::BeaconNodeBackend, HotColdDB, StoreConfig};
use tempfile::{tempdir, TempDir};
use types::*;

type E = MainnetEthSpec;

fn get_store(
    db_path: &TempDir,
    config: StoreConfig,
    spec: Arc<ChainSpec>,
) -> Arc<HotColdDB<E, BeaconNodeBackend<E>, BeaconNodeBackend<E>>> {
    let hot_path = db_path.path().join("chain_db");
    let cold_path = db_path.path().join("freezer_db");
    let blobs_path = db_path.path().join("blobs_db");
    let log = test_logger();

    HotColdDB::open(
        &hot_path,
        &cold_path,
        &blobs_path,
        |_, _, _| Ok(()),
        config,
        spec.into(),
        log,
    )
    .expect("disk store should initialize")
}

#[tokio::test]
async fn signature_verify_chain_segment_pubkey_cache() {
    let initial_validator_count = 32;

    let deposit_slot = Slot::new(4 * E::slots_per_epoch() - 1);
    let pre_deposit_slot = deposit_slot - 1;
    let spec = Arc::new(ForkName::Electra.make_genesis_spec(E::default_spec()));

    // Keep historic states on main harness.
    let chain_config = ChainConfig {
        reconstruct_historic_states: true,
        ..ChainConfig::default()
    };
    let harness = BeaconChainHarness::builder(E::default())
        .chain_config(chain_config)
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
    let new_epoch_start_slot = pre_finalized_deposit_state.slot() + E::slots_per_epoch() + 1;

    // New validator should not be in the pubkey cache yet.
    assert_eq!(
        harness
            .chain
            .validator_index(&new_validator_pk_bytes)
            .unwrap(),
        None
    );
    let new_validator_index = initial_validator_count;

    // Produce blocks in the next epoch. Statistically one of these should be signed by our new
    // validator (99% probability).
    harness.extend_to_slot(new_epoch_start_slot).await;

    let chain_dump = harness.chain.chain_dump();

    // New validator should be in the pubkey cache now.
    assert_eq!(
        harness
            .chain
            .validator_index(&new_validator_pk_bytes)
            .unwrap(),
        Some(new_validator_index)
    );

    // Initialise a new harness using checkpoint sync, prior to the new deposit being finalized.
    let datadir = tempdir().unwrap();
    let store = get_store(&datadir, Default::default(), spec.clone());

    let kzg = get_kzg(&spec);

    let mock = mock_execution_layer_from_parts(
        harness.spec.clone(),
        harness.runtime.task_executor.clone(),
    );

    // Initialise a new beacon chain from the finalized checkpoint.
    // The slot clock must be set to a time ahead of the checkpoint state.
    let slot_clock = TestingSlotClock::new(
        Slot::new(0),
        Duration::from_secs(harness.chain.genesis_time),
        Duration::from_secs(spec.seconds_per_slot),
    );
    slot_clock.set_slot(harness.get_current_slot().as_u64());

    let checkpoint_slot = deposit_slot
        .epoch(E::slots_per_epoch())
        .start_slot(E::slots_per_epoch());
    let mut checkpoint_state = harness
        .chain
        .state_at_slot(checkpoint_slot, StateSkipConfig::WithStateRoots)
        .unwrap();
    let checkpoint_state_root = checkpoint_state.update_tree_hash_cache().unwrap();
    let checkpoint_block_root = checkpoint_state.get_latest_block_root(checkpoint_state_root);
    let checkpoint_block = harness
        .chain
        .get_block(&checkpoint_block_root)
        .await
        .unwrap()
        .unwrap();
    let checkpoint_blobs_opt = harness
        .chain
        .get_or_reconstruct_blobs(&checkpoint_block_root)
        .unwrap();
    let genesis_state = harness
        .chain
        .state_at_slot(Slot::new(0), StateSkipConfig::WithStateRoots)
        .unwrap();
    let (shutdown_tx, _shutdown_rx) = futures::channel::mpsc::channel(1);

    let beacon_chain = BeaconChainBuilder::<DiskHarnessType<E>>::new(MainnetEthSpec, kzg)
        .store(store.clone())
        .custom_spec(spec.clone())
        .task_executor(harness.chain.task_executor.clone())
        .logger(harness.runtime.log.clone())
        .weak_subjectivity_state(
            checkpoint_state,
            checkpoint_block.clone(),
            checkpoint_blobs_opt.clone(),
            genesis_state,
        )
        .unwrap()
        .shutdown_sender(shutdown_tx)
        .store_migrator_config(MigratorConfig::default().blocking())
        .dummy_eth1_backend()
        .expect("should build dummy backend")
        .slot_clock(slot_clock)
        .chain_config(ChainConfig::default())
        .execution_layer(Some(mock.el))
        .build()
        .expect("should build");
}
