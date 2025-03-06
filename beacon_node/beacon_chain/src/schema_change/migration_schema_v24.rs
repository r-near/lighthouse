use crate::{
    beacon_chain::BeaconChainTypes,
    summaries_dag::{DAGStateSummaryV22, StateSummariesDAG},
};
use slog::{debug, info, Logger};
use ssz::Decode;
use ssz_derive::{Decode, Encode};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use store::{
    get_full_state_v22, hdiff::StorageStrategy, hot_cold_store::DiffBaseStateRoot, DBColumn, Error,
    HotColdDB, HotStateSummary, KeyValueStore, KeyValueStoreOp, StoreItem,
};
use types::{EthSpec, Hash256, Slot};

#[derive(Debug, Clone, Copy, Encode, Decode)]
pub struct HotStateSummaryV22 {
    slot: Slot,
    latest_block_root: Hash256,
    epoch_boundary_state_root: Hash256,
}

pub fn upgrade_to_v24<T: BeaconChainTypes>(
    db: Arc<HotColdDB<T::EthSpec, T::HotStore, T::ColdStore>>,
    log: Logger,
) -> Result<Vec<KeyValueStoreOp>, Error> {
    let split = db.get_split_info();

    // Update anchor_slot to the current finalized state
    // TODO(hdiff): Is the anchor loaded already at this point? Should be set to split slot or to
    // the finalized state slot?
    {
        let anchor_info = db.get_anchor_info();
        let mut new_anchor_info = anchor_info.clone();
        new_anchor_info.anchor_slot = split.slot;
        db.compare_and_set_anchor_info_with_write(anchor_info, new_anchor_info)?;
    }

    let state_summaries_dag = new_dag::<T>(&db)?;

    // Sort summaries by slot so we have their ancestor diffs already stored when we store them.
    // If the summaries are sorted topologically we can insert them into the DB like if they were a
    // new state, re-using existing code. As states are likely to be sequential the diff cache
    // should kick in making the migration more efficient. If we just iterate the column of
    // summaries we may get distance state of each iteration.
    let summaries_by_slot = state_summaries_dag.summaries_by_slot_ascending();
    debug!(
        log,
        "Starting hot states migration";
        "summaries_count" => state_summaries_dag.summaries_count(),
        "slots_count" => summaries_by_slot.len(),
        "min_slot" => ?summaries_by_slot.first_key_value().map(|(slot, _)| slot),
        "max_slot" => ?summaries_by_slot.last_key_value().map(|(slot, _)| slot),
    );

    // Upgrade all hot DB state summaries to the new type:
    // - Set all summaries of boundary states as `Snapshot` type
    // - Set all others are `Replay` pointing to `epoch_boundary_state_root`

    let mut migrate_ops = vec![];
    let mut diffs_written = 0;
    let mut summaries_written = 0;
    let mut last_log_time = Instant::now();

    for (slot, old_hot_state_summaries) in summaries_by_slot {
        for (state_root, old_summary) in old_hot_state_summaries {
            // 1. Store snapshot or diff at this slot (if required).
            // TODO(hdiff): make sure lowest hot hierarchy config is >= 5 to prevent having to
            // reconstruct states.
            let storage_strategy = db.hot_storage_strategy(slot)?;
            debug!(
                log,
                "Migrating state summary";
                "slot" => slot,
                "state_root" => ?state_root,
                "storage_strategy" => ?storage_strategy,
            );

            match storage_strategy {
                StorageStrategy::DiffFrom(_) | StorageStrategy::Snapshot => {
                    // Load the full state and re-store it as a snapshot or diff.
                    let state = get_full_state_v22(&db.hot_db, &state_root, &db.spec)?
                        .ok_or(Error::MissingState(state_root))?;

                    // Store immediately so that future diffs can load and diff from it.
                    let mut ops = vec![];
                    // We must commit the hot state summary immediatelly, otherwise we can't diff
                    // against it and future writes will fail. That's why we write the new hot
                    // summaries in a different column to have both new and old data present at
                    // once. Otherwise if the process crashes during the migration the database will
                    // be broken.
                    db.store_hot_state_summary(&state_root, &state, &mut ops)?;
                    db.store_hot_state_diffs(&state_root, &state, &mut ops)?;
                    db.hot_db.do_atomically(ops)?;
                    diffs_written += 1;
                }
                StorageStrategy::ReplayFrom(_) => {
                    // Optimization: instead of having to load the state of each summary we load x32
                    // less states by manually computing the HotStateSummary roots using the
                    // computed state dag.
                    //
                    // No need to store diffs for states that will be reconstructed by replaying
                    // blocks.
                    // 2. Convert the summary to the new format.
                    let latest_block_root = old_summary.latest_block_root;
                    let previous_state_root = if state_root == split.state_root {
                        Hash256::ZERO
                    } else {
                        state_summaries_dag
                            .previous_state_root(state_root)
                            .map_err(|e| {
                                Error::MigrationError(format!(
                                    "error computing previous_state_root {e:?}"
                                ))
                            })?
                    };

                    let diff_base_state_root =
                        if let Some(diff_base_slot) = storage_strategy.diff_base_slot() {
                            DiffBaseStateRoot::new(
                                diff_base_slot,
                                state_summaries_dag
                                    .ancestor_state_root_at_slot(state_root, diff_base_slot)
                                    .map_err(|e| {
                                        Error::MigrationError(format!(
                                            "error computing ancestor_state_root_at_slot {e:?}"
                                        ))
                                    })?,
                            )
                        } else {
                            DiffBaseStateRoot::zero()
                        };

                    let new_summary = HotStateSummary {
                        slot,
                        latest_block_root,
                        previous_state_root,
                        diff_base_state_root,
                    };
                    let op = new_summary.as_kv_store_op(state_root);
                    // It's not ncessary to immediately commit the summaries of states that are
                    // ReplayFrom. However we do so for simplicity.
                    db.hot_db.do_atomically(vec![op])?;
                }
            }

            // 3. Stage old data for deletion.
            if slot % T::EthSpec::slots_per_epoch() == 0 {
                migrate_ops.push(KeyValueStoreOp::DeleteKey(
                    DBColumn::BeaconState,
                    state_root.as_slice().to_vec(),
                ));
            }

            // Delete previous summaries
            migrate_ops.push(KeyValueStoreOp::DeleteKey(
                DBColumn::BeaconStateSummary,
                state_root.as_slice().to_vec(),
            ));

            summaries_written += 1;
            if last_log_time.elapsed() > Duration::from_secs(5) {
                last_log_time = Instant::now();
                // TODO(hdiff): Display the slot distance between head and finalized, and head-tracker count
                info!(
                    log,
                    "Hot states migration in progress";
                    "diff_written" => diffs_written,
                    "summaries_written" => summaries_written,
                );
            }
        }
    }

    // TODO(hdiff): Should run hot DB compaction after deleting potentially a lot of states. Or should wait
    // for the next finality event?
    info!(
        log,
        "Hot states migration complete";
        "diff_written" => diffs_written,
        "summaries_written" => summaries_written,
    );

    Ok(migrate_ops)
}

pub fn downgrade_from_v24<T: BeaconChainTypes>(
    _db: Arc<HotColdDB<T::EthSpec, T::HotStore, T::ColdStore>>,
    _log: Logger,
) -> Result<Vec<KeyValueStoreOp>, Error> {
    panic!("downgrade not supported");
}

fn new_dag<T: BeaconChainTypes>(
    db: &HotColdDB<T::EthSpec, T::HotStore, T::ColdStore>,
) -> Result<StateSummariesDAG, Error> {
    // Collect all sumaries for unfinalized states
    let state_summaries_v22 = db
        .hot_db
        // Collect summaries from the legacy V22 column BeaconStateSummary
        .iter_column::<Hash256>(DBColumn::BeaconStateSummary)
        .map(|res| {
            let (key, value) = res?;
            let state_root: Hash256 = key;
            let summary = HotStateSummaryV22::from_ssz_bytes(&value)?;
            let block_root = summary.latest_block_root;
            let block = db
                .get_blinded_block(&block_root)?
                .ok_or(Error::MissingBlock(block_root))?;

            Ok((
                state_root,
                DAGStateSummaryV22 {
                    slot: summary.slot,
                    latest_block_root: summary.latest_block_root,
                    block_slot: block.slot(),
                    block_parent_root: block.parent_root(),
                },
            ))
        })
        .collect::<Result<Vec<_>, Error>>()?;

    let block_roots = HashSet::<Hash256>::from_iter(
        state_summaries_v22
            .iter()
            .map(|(_, summary)| summary.latest_block_root),
    );

    // Construct block root to parent block root mapping.
    let mut parent_block_roots = HashMap::new();
    for block_root in block_roots {
        let blinded_block = db
            .get_blinded_block(&block_root)?
            .ok_or(Error::MissingBlock(block_root))?;
        let parent_root = blinded_block.parent_root();
        parent_block_roots.insert(block_root, parent_root);
    }

    StateSummariesDAG::new_from_v22(state_summaries_v22)
        .map_err(|e| Error::MigrationError(format!("error computing states summaries dag {e:?}")))
}
