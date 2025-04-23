pub mod cli;

use crate::cli::DatabaseManager;
use crate::cli::Migrate;
use crate::cli::PruneStates;
use beacon_chain::{
    builder::Witness, eth1_chain::CachingEth1Backend, schema_change::migrate_schema,
    slot_clock::SystemTimeSlotClock,
};
use beacon_node::{get_data_dir, ClientConfig};
use clap::ArgMatches;
use clap::ValueEnum;
use cli::{Compact, Inspect};
use environment::{Environment, RuntimeContext};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use store::{
    database::interface::BeaconNodeBackend,
    errors::Error,
    hot_cold_store::HotColdDBError,
    metadata::{SchemaVersion, CURRENT_SCHEMA_VERSION},
    BlobSidecarListFromRoot, DBColumn, HotColdDB, KeyValueStore, KeyValueStoreOp,
};
use strum::{EnumString, EnumVariantNames};
use tracing::{debug, info, warn};
use types::{BeaconState, BlobSidecarList, EthSpec, Hash256, Slot};

fn parse_client_config<E: EthSpec>(
    cli_args: &ArgMatches,
    database_manager_config: &DatabaseManager,
    _env: &Environment<E>,
) -> Result<ClientConfig, String> {
    let mut client_config = ClientConfig::default();

    client_config.set_data_dir(get_data_dir(cli_args));
    client_config
        .freezer_db_path
        .clone_from(&database_manager_config.freezer_dir);
    client_config
        .blobs_db_path
        .clone_from(&database_manager_config.blobs_dir);
    client_config.store.blob_prune_margin_epochs = database_manager_config.blob_prune_margin_epochs;
    client_config.store.hierarchy_config = database_manager_config.hierarchy_exponents.clone();
    client_config.store.backend = database_manager_config.backend;
    Ok(client_config)
}

pub fn display_db_version<E: EthSpec>(
    client_config: ClientConfig,
    runtime_context: &RuntimeContext<E>,
) -> Result<(), Error> {
    let spec = runtime_context.eth2_config.spec.clone();
    let hot_path = client_config.get_db_path();
    let cold_path = client_config.get_freezer_db_path();
    let blobs_path = client_config.get_blobs_db_path();

    let mut version = CURRENT_SCHEMA_VERSION;
    HotColdDB::<E, BeaconNodeBackend<E>, BeaconNodeBackend<E>>::open(
        &hot_path,
        &cold_path,
        &blobs_path,
        |_, from, _| {
            version = from;
            Ok(())
        },
        client_config.store,
        spec,
    )?;

    info!(version = version.as_u64(), "Database");

    if version != CURRENT_SCHEMA_VERSION {
        info!(
            current_schema_version = CURRENT_SCHEMA_VERSION.as_u64(),
            "Latest schema"
        );
    }

    Ok(())
}

#[derive(
    Debug, PartialEq, Eq, Clone, EnumString, Deserialize, Serialize, EnumVariantNames, ValueEnum,
)]
pub enum InspectTarget {
    #[strum(serialize = "sizes")]
    #[clap(name = "sizes")]
    ValueSizes,
    #[strum(serialize = "total")]
    #[clap(name = "total")]
    ValueTotal,
    #[strum(serialize = "values")]
    #[clap(name = "values")]
    Values,
    #[strum(serialize = "gaps")]
    #[clap(name = "gaps")]
    Gaps,
}

pub struct InspectConfig {
    column: DBColumn,
    target: InspectTarget,
    skip: Option<usize>,
    limit: Option<usize>,
    freezer: bool,
    blobs_db: bool,
    /// Configures where the inspect output should be stored.
    output_dir: PathBuf,
}

fn parse_inspect_config(inspect_config: &Inspect) -> Result<InspectConfig, String> {
    let column: DBColumn = inspect_config
        .column
        .parse()
        .map_err(|e| format!("Unable to parse column flag: {e:?}"))?;
    let target: InspectTarget = inspect_config.output.clone();
    let skip = inspect_config.skip;
    let limit = inspect_config.limit;
    let freezer = inspect_config.freezer;
    let blobs_db = inspect_config.blobs_db;

    let output_dir: PathBuf = inspect_config.output_dir.clone().unwrap_or_default();
    Ok(InspectConfig {
        column,
        target,
        skip,
        limit,
        freezer,
        blobs_db,
        output_dir,
    })
}

pub fn inspect_db<E: EthSpec>(
    inspect_config: InspectConfig,
    client_config: ClientConfig,
) -> Result<(), String> {
    let hot_path = client_config.get_db_path();
    let cold_path = client_config.get_freezer_db_path();
    let blobs_path = client_config.get_blobs_db_path();

    let mut total = 0;
    let mut num_keys = 0;

    let sub_db = if inspect_config.freezer {
        BeaconNodeBackend::<E>::open(&client_config.store, &cold_path)
            .map_err(|e| format!("Unable to open freezer DB: {e:?}"))?
    } else if inspect_config.blobs_db {
        BeaconNodeBackend::<E>::open(&client_config.store, &blobs_path)
            .map_err(|e| format!("Unable to open blobs DB: {e:?}"))?
    } else {
        BeaconNodeBackend::<E>::open(&client_config.store, &hot_path)
            .map_err(|e| format!("Unable to open hot DB: {e:?}"))?
    };

    let skip = inspect_config.skip.unwrap_or(0);
    let limit = inspect_config.limit.unwrap_or(usize::MAX);

    let mut prev_key = 0;
    let mut found_gaps = false;

    let base_path = &inspect_config.output_dir;

    if let InspectTarget::Values = inspect_config.target {
        fs::create_dir_all(base_path)
            .map_err(|e| format!("Unable to create import directory: {:?}", e))?;
    }

    for res in sub_db
        .iter_column::<Vec<u8>>(inspect_config.column)
        .skip(skip)
        .take(limit)
    {
        let (key, value) = res.map_err(|e| format!("{:?}", e))?;

        match inspect_config.target {
            InspectTarget::ValueSizes => {
                println!("{}: {} bytes", hex::encode(&key), value.len());
            }
            InspectTarget::Gaps => {
                // Convert last 8 bytes of key to u64.
                let numeric_key = u64::from_be_bytes(
                    key[key.len() - 8..]
                        .try_into()
                        .expect("key is at least 8 bytes"),
                );

                if numeric_key > prev_key + 1 {
                    println!(
                        "gap between keys {} and {} (offset: {})",
                        prev_key, numeric_key, num_keys,
                    );
                    found_gaps = true;
                }
                prev_key = numeric_key;
            }
            InspectTarget::ValueTotal => (),
            InspectTarget::Values => {
                let file_path = base_path.join(format!(
                    "{}_{}.ssz",
                    inspect_config.column.as_str(),
                    hex::encode(&key)
                ));

                let write_result = fs::OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&file_path)
                    .map_err(|e| format!("Failed to open file: {:?}", e))
                    .map(|mut file| {
                        file.write_all(&value)
                            .map_err(|e| format!("Failed to write file: {:?}", e))
                    });
                if let Err(e) = write_result {
                    println!("Error writing values to file {:?}: {:?}", file_path, e);
                } else {
                    println!("Successfully saved values to file: {:?}", file_path);
                }
            }
        }
        total += value.len();
        num_keys += 1;
    }

    if inspect_config.target == InspectTarget::Gaps && !found_gaps {
        println!("No gaps found!");
    }

    println!("Num keys: {}", num_keys);
    println!("Total: {} bytes", total);

    Ok(())
}

pub struct CompactConfig {
    column: DBColumn,
    freezer: bool,
    blobs_db: bool,
}

fn parse_compact_config(compact_config: &Compact) -> Result<CompactConfig, String> {
    let column: DBColumn = compact_config
        .column
        .parse()
        .expect("column is a required field");
    let freezer = compact_config.freezer;
    let blobs_db = compact_config.blobs_db;
    Ok(CompactConfig {
        column,
        freezer,
        blobs_db,
    })
}

pub fn compact_db<E: EthSpec>(
    compact_config: CompactConfig,
    client_config: ClientConfig,
) -> Result<(), Error> {
    let hot_path = client_config.get_db_path();
    let cold_path = client_config.get_freezer_db_path();
    let blobs_path = client_config.get_blobs_db_path();
    let column = compact_config.column;

    let (sub_db, db_name) = if compact_config.freezer {
        (
            BeaconNodeBackend::<E>::open(&client_config.store, &cold_path)?,
            "freezer_db",
        )
    } else if compact_config.blobs_db {
        (
            BeaconNodeBackend::<E>::open(&client_config.store, &blobs_path)?,
            "blobs_db",
        )
    } else {
        (
            BeaconNodeBackend::<E>::open(&client_config.store, &hot_path)?,
            "hot_db",
        )
    };
    info!(
        db = db_name,
        column = ?column,
        "Compacting database"
    );
    sub_db.compact_column(column)?;
    Ok(())
}

pub struct MigrateConfig {
    to: SchemaVersion,
}

fn parse_migrate_config(migrate_config: &Migrate) -> Result<MigrateConfig, String> {
    let to = SchemaVersion(migrate_config.to);

    Ok(MigrateConfig { to })
}

pub fn migrate_db<E: EthSpec>(
    migrate_config: MigrateConfig,
    client_config: ClientConfig,
    mut genesis_state: BeaconState<E>,
    runtime_context: &RuntimeContext<E>,
) -> Result<(), Error> {
    let spec = runtime_context.eth2_config.spec.clone();
    let hot_path = client_config.get_db_path();
    let cold_path = client_config.get_freezer_db_path();
    let blobs_path = client_config.get_blobs_db_path();

    let mut from = CURRENT_SCHEMA_VERSION;
    let to = migrate_config.to;
    let db = HotColdDB::<E, BeaconNodeBackend<E>, BeaconNodeBackend<E>>::open(
        &hot_path,
        &cold_path,
        &blobs_path,
        |_, db_initial_version, _| {
            from = db_initial_version;
            Ok(())
        },
        client_config.store.clone(),
        spec.clone(),
    )?;

    info!(
        from = from.as_u64(),
        to = to.as_u64(),
        "Migrating database schema"
    );

    let genesis_state_root = genesis_state.canonical_root()?;
    migrate_schema::<Witness<SystemTimeSlotClock, CachingEth1Backend<E>, _, _, _>>(
        db,
        Some(genesis_state_root),
        from,
        to,
    )
}

pub fn prune_payloads<E: EthSpec>(
    client_config: ClientConfig,
    runtime_context: &RuntimeContext<E>,
) -> Result<(), Error> {
    let spec = &runtime_context.eth2_config.spec;
    let hot_path = client_config.get_db_path();
    let cold_path = client_config.get_freezer_db_path();
    let blobs_path = client_config.get_blobs_db_path();

    let db = HotColdDB::<E, BeaconNodeBackend<E>, BeaconNodeBackend<E>>::open(
        &hot_path,
        &cold_path,
        &blobs_path,
        |_, _, _| Ok(()),
        client_config.store,
        spec.clone(),
    )?;

    // If we're trigging a prune manually then ignore the check on the split's parent that bails
    // out early.
    let force = true;
    db.try_prune_execution_payloads(force)
}

pub fn prune_blobs<E: EthSpec>(
    client_config: ClientConfig,
    runtime_context: &RuntimeContext<E>,
) -> Result<(), Error> {
    let spec = &runtime_context.eth2_config.spec;
    let hot_path = client_config.get_db_path();
    let cold_path = client_config.get_freezer_db_path();
    let blobs_path = client_config.get_blobs_db_path();

    let db = HotColdDB::<E, BeaconNodeBackend<E>, BeaconNodeBackend<E>>::open(
        &hot_path,
        &cold_path,
        &blobs_path,
        |_, _, _| Ok(()),
        client_config.store,
        spec.clone(),
    )?;

    // If we're triggering a prune manually then ignore the check on `epochs_per_blob_prune` that
    // bails out early by passing true to the force parameter.
    db.try_prune_most_blobs(true)
}

pub struct PruneStatesConfig {
    confirm: bool,
}
fn parse_prune_states_config(
    prune_states_config: &PruneStates,
) -> Result<PruneStatesConfig, String> {
    let confirm = prune_states_config.confirm;
    Ok(PruneStatesConfig { confirm })
}

pub fn prune_states<E: EthSpec>(
    client_config: ClientConfig,
    prune_config: PruneStatesConfig,
    mut genesis_state: BeaconState<E>,
    runtime_context: &RuntimeContext<E>,
) -> Result<(), String> {
    let spec = &runtime_context.eth2_config.spec;
    let hot_path = client_config.get_db_path();
    let cold_path = client_config.get_freezer_db_path();
    let blobs_path = client_config.get_blobs_db_path();

    let db = HotColdDB::<E, BeaconNodeBackend<E>, BeaconNodeBackend<E>>::open(
        &hot_path,
        &cold_path,
        &blobs_path,
        |_, _, _| Ok(()),
        client_config.store,
        spec.clone(),
    )
    .map_err(|e| format!("Unable to open database: {e:?}"))?;

    // Load the genesis state from the database to ensure we're deleting states for the
    // correct network, and that we don't end up storing the wrong genesis state.
    let genesis_from_db = db
        .load_cold_state_by_slot(Slot::new(0))
        .map_err(|e| format!("Error reading genesis state: {e:?}"))?;

    if genesis_from_db.genesis_validators_root() != genesis_state.genesis_validators_root() {
        return Err(format!(
            "Error: Wrong network. Genesis state in DB does not match {} genesis.",
            spec.config_name.as_deref().unwrap_or("<unknown network>")
        ));
    }

    // Check that the user has confirmed they want to proceed.
    if !prune_config.confirm {
        if db.get_anchor_info().full_state_pruning_enabled() {
            info!("States have already been pruned");
            return Ok(());
        }

        info!("Ready to prune states");
        warn!("Pruning states is irreversible");
        warn!("Re-run this command with --confirm to commit to state deletion");
        info!("Nothing has been pruned on this run");
        return Err("Error: confirmation flag required".into());
    }

    // Delete all historic state data and *re-store* the genesis state.
    let genesis_state_root = genesis_state
        .update_tree_hash_cache()
        .map_err(|e| format!("Error computing genesis state root: {e:?}"))?;
    db.prune_historic_states(genesis_state_root, &genesis_state)
        .map_err(|e| format!("Failed to prune due to error: {e:?}"))?;

    info!("Historic states pruned successfully");
    Ok(())
}

fn set_oldest_blob_slot<E: EthSpec>(
    slot: Slot,
    client_config: ClientConfig,
    runtime_context: &RuntimeContext<E>,
) -> Result<(), Error> {
    let spec = &runtime_context.eth2_config.spec;
    let hot_path = client_config.get_db_path();
    let cold_path = client_config.get_freezer_db_path();
    let blobs_path = client_config.get_blobs_db_path();

    let db = HotColdDB::<E, BeaconNodeBackend<E>, BeaconNodeBackend<E>>::open(
        &hot_path,
        &cold_path,
        &blobs_path,
        |_, _, _| Ok(()),
        client_config.store,
        spec.clone(),
    )?;

    let old_blob_info = db.get_blob_info();
    let mut new_blob_info = old_blob_info.clone();
    new_blob_info.oldest_blob_slot = Some(slot);

    info!(
        previous = ?old_blob_info.oldest_blob_slot,
        new = ?slot,
        "Updating oldest blob slot"
    );

    db.compare_and_set_blob_info_with_write(old_blob_info, new_blob_info)
}

fn inspect_blobs<E: EthSpec>(
    _verify: bool,
    client_config: ClientConfig,
    runtime_context: &RuntimeContext<E>,
) -> Result<(), Error> {
    let spec = &runtime_context.eth2_config.spec;
    let hot_path = client_config.get_db_path();
    let cold_path = client_config.get_freezer_db_path();
    let blobs_path = client_config.get_blobs_db_path();

    let db = HotColdDB::<E, BeaconNodeBackend<E>, BeaconNodeBackend<E>>::open(
        &hot_path,
        &cold_path,
        &blobs_path,
        |_, _, _| Ok(()),
        client_config.store,
        spec.clone(),
    )?;

    let split = db.get_split_info();
    let oldest_block_slot = db.get_oldest_block_slot();
    let deneb_start_slot = spec
        .deneb_fork_epoch
        .map_or(Slot::new(0), |epoch| epoch.start_slot(E::slots_per_epoch()));
    let start_slot = oldest_block_slot.max(deneb_start_slot);

    if oldest_block_slot > deneb_start_slot {
        info!(
            start = %deneb_start_slot,
            end = %(oldest_block_slot - 1),
            "Missing blobs AND blocks"
        );
    }

    let mut last_block_root = Hash256::ZERO;

    for res in db.forwards_block_roots_iterator_until(start_slot, split.slot, || {
        db.get_advanced_hot_state(split.block_root, split.slot, split.state_root)?
            .ok_or(HotColdDBError::MissingSplitState(split.state_root, split.slot).into())
            .map(|(_, split_state)| (split_state, split.block_root))
    })? {
        let (block_root, slot) = res?;

        if last_block_root == block_root {
            info!("Slot {}: no block", slot);
        } else if let BlobSidecarListFromRoot::Blobs(blobs) = db.get_blobs(&block_root)? {
            // FIXME(sproul): do verification here
            info!("Slot {}: {} blobs stored", slot, blobs.len());
        } else {
            // Check whether blobs are expected.
            let block = db
                .get_blinded_block(&block_root)?
                .ok_or(Error::BlockNotFound(block_root))?;

            let num_expected_blobs = block
                .message()
                .body()
                .blob_kzg_commitments()
                .map_or(0, |blobs| blobs.len());
            if num_expected_blobs > 0 {
                warn!(
                    "Slot {}: {} blobs missing ({:?})",
                    slot, num_expected_blobs, block_root
                );
            } else {
                info!("Slot {}: block with 0 blobs", slot);
            }
        }
        last_block_root = block_root;
    }

    Ok(())
}

fn import_blobs<E: EthSpec>(
    source_path: &Path,
    client_config: ClientConfig,
    runtime_context: &RuntimeContext<E>,
) -> Result<(), Error> {
    let spec = &runtime_context.eth2_config.spec;
    let hot_path = client_config.get_db_path();
    let cold_path = client_config.get_freezer_db_path();
    let blobs_path = client_config.get_blobs_db_path();

    let db = HotColdDB::<E, BeaconNodeBackend<E>, BeaconNodeBackend<E>>::open(
        &hot_path,
        &cold_path,
        &blobs_path,
        |_, _, _| Ok(()),
        client_config.store.clone(),
        spec.clone(),
    )?;

    let source_db = BeaconNodeBackend::<E>::open(&client_config.store, source_path)?;

    let prev_blob_info = db.get_blob_info();
    let mut oldest_blob_slot = prev_blob_info
        .oldest_blob_slot
        .unwrap_or(Slot::new(u64::MAX));

    let mut num_already_known = 0;
    let mut num_imported = 0;

    let mut ops = vec![];
    let batch_size = 1024;

    for res in source_db.iter_column(DBColumn::BeaconBlob) {
        let (block_root, blob_bytes) = res?;

        if db.get_blobs(&block_root)?.len() > 0 {
            num_already_known += 1;
        } else {
            // FIXME(sproul): max len?
            let blobs = BlobSidecarList::<E>::from_ssz_bytes(&blob_bytes, 64)?;
            ops.push(KeyValueStoreOp::PutKeyValue(
                DBColumn::BeaconBlob,
                block_root.to_vec(),
                blob_bytes,
            ));

            if let Some(blob) = blobs.first() {
                oldest_blob_slot = oldest_blob_slot.min(blob.slot());
                debug!("Imported blobs for slot {} ({:?})", blob.slot(), block_root);
            }
            num_imported += 1;

            if ops.len() >= batch_size {
                db.blobs_db.do_atomically(std::mem::take(&mut ops))?;
            }
        }
    }
    db.blobs_db.do_atomically(ops)?;

    let mut new_blob_info = prev_blob_info.clone();
    new_blob_info.oldest_blob_slot = Some(oldest_blob_slot);
    db.compare_and_set_blob_info_with_write(prev_blob_info, new_blob_info)?;

    info!(
        imported = num_imported,
        already_known = num_already_known,
        "Blobs imported"
    );

    Ok(())
}

/// Run the database manager, returning an error string if the operation did not succeed.
pub fn run<E: EthSpec>(
    cli_args: &ArgMatches,
    db_manager_config: &DatabaseManager,
    env: Environment<E>,
) -> Result<(), String> {
    let client_config = parse_client_config(cli_args, db_manager_config, &env)?;
    let context = env.core_context();
    let format_err = |e| format!("Fatal error: {:?}", e);

    let get_genesis_state = || {
        let executor = env.core_context().executor;
        let network_config = context
            .eth2_network_config
            .clone()
            .ok_or("Missing network config")?;

        executor
            .block_on_dangerous(
                network_config.genesis_state::<E>(
                    client_config.genesis_state_url.as_deref(),
                    client_config.genesis_state_url_timeout,
                ),
                "get_genesis_state",
            )
            .ok_or("Shutting down")?
            .map_err(|e| format!("Error getting genesis state: {e}"))?
            .ok_or("Genesis state missing".to_string())
    };

    match &db_manager_config.subcommand {
        cli::DatabaseManagerSubcommand::Migrate(migrate_config) => {
            let migrate_config = parse_migrate_config(migrate_config)?;
            let genesis_state = get_genesis_state()?;
            migrate_db(migrate_config, client_config, genesis_state, &context).map_err(format_err)
        }
        cli::DatabaseManagerSubcommand::Inspect(inspect_config) => {
            let inspect_config = parse_inspect_config(inspect_config)?;
            inspect_db::<E>(inspect_config, client_config)
        }
        cli::DatabaseManagerSubcommand::Version(_) => {
            display_db_version(client_config, &context).map_err(format_err)
        }
        cli::DatabaseManagerSubcommand::PrunePayloads(_) => {
            prune_payloads(client_config, &context).map_err(format_err)
        }
        cli::DatabaseManagerSubcommand::PruneBlobs(_) => {
            prune_blobs(client_config, &context).map_err(format_err)
        }
        cli::DatabaseManagerSubcommand::PruneStates(prune_states_config) => {
            let prune_config = parse_prune_states_config(prune_states_config)?;
            let genesis_state = get_genesis_state()?;
            prune_states(client_config, prune_config, genesis_state, &context)
        }
        cli::DatabaseManagerSubcommand::Compact(compact_config) => {
            let compact_config = parse_compact_config(compact_config)?;
            compact_db::<E>(compact_config, client_config).map_err(format_err)
        }
        cli::DatabaseManagerSubcommand::SetOldestBlobSlot(blob_slot_config) => {
            set_oldest_blob_slot(blob_slot_config.slot, client_config, &context).map_err(format_err)
        }
        cli::DatabaseManagerSubcommand::InspectBlobs(_) => {
            inspect_blobs(false, client_config, &context).map_err(format_err)
        }
        cli::DatabaseManagerSubcommand::ImportBlobs(config) => {
            import_blobs(&config.source_db, client_config, &context).map_err(format_err)
        }
    }
}
