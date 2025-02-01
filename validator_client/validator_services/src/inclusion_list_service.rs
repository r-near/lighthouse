use crate::duties_service::DutiesService;
use beacon_node_fallback::{ApiTopic, BeaconNodeFallback};
use environment::RuntimeContext;
use eth2::types::InclusionListDutyData;
use futures::future::join_all;
use slog::{crit, error, info, trace, warn};
use slot_clock::SlotClock;
use std::ops::Deref;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use types::{ChainSpec, EthSpec, Slot};
use validator_store::{Error as ValidatorStoreError, ValidatorStore};

/// Helper to minimise `Arc` usage.
pub struct Inner<T, E: EthSpec> {
    duties_service: Arc<DutiesService<T, E>>,
    validator_store: Arc<ValidatorStore<T, E>>,
    slot_clock: T,
    beacon_nodes: Arc<BeaconNodeFallback<T, E>>,
    context: RuntimeContext<E>,
}

/// Attempts to produce inclusion lists for all known validators 3/4 of the way through each slot.
pub struct InclusionListService<T, E: EthSpec> {
    inner: Arc<Inner<T, E>>,
}

impl<T, E: EthSpec> Clone for InclusionListService<T, E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T, E: EthSpec> Deref for InclusionListService<T, E> {
    type Target = Inner<T, E>;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

impl<T: SlotClock + 'static, E: EthSpec> InclusionListService<T, E> {
    pub fn new(
        duties_service: Arc<DutiesService<T, E>>,
        validator_store: Arc<ValidatorStore<T, E>>,
        slot_clock: T,
        beacon_nodes: Arc<BeaconNodeFallback<T, E>>,
        context: RuntimeContext<E>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                duties_service,
                validator_store,
                slot_clock,
                beacon_nodes,
                context,
            }),
        }
    }

    /// Starts the service which periodically produces inclusion lists.
    pub fn start_update_service(self, spec: &ChainSpec) -> Result<(), String> {
        let log = self.context.log().clone();

        let slot_duration = Duration::from_secs(spec.seconds_per_slot);
        let duration_to_next_slot = self
            .slot_clock
            .duration_to_next_slot()
            .ok_or("Unable to determine duration to next slot")?;

        info!(
            log,
            "Inclusion list production service started";
            "next_update_millis" => duration_to_next_slot.as_millis()
        );

        let executor = self.context.executor.clone();

        let interval_fut = async move {
            loop {
                if let Some(duration_to_next_slot) = self.slot_clock.duration_to_next_slot() {
                    // 3/4 of the way into the slot
                    sleep(duration_to_next_slot + (slot_duration * 3 / 4)).await;
                    let log = self.context.log();

                    if let Err(e) = self.spawn_inclusion_list_task(slot_duration) {
                        crit!(
                            log,
                            "Failed to spawn inclusion list task";
                            "error" => e
                        )
                    } else {
                        trace!(
                            log,
                            "Spawned inclusion list task";
                        )
                    }
                } else {
                    error!(log, "Failed to read slot clock");
                    // If we can't read the slot clock, just wait another slot.
                    sleep(slot_duration).await;
                    continue;
                }
            }
        };

        executor.spawn(interval_fut, "inclusion_list_service");
        Ok(())
    }

    /// Spawn a new task that downloads, signs and uploads the inclusion lists to the beacon node.
    // TODO(focil) I don't think we need `slot_duration` here, unless we need to make some calculation
    // related to the freeze deadline.
    fn spawn_inclusion_list_task(&self, _slot_duration: Duration) -> Result<(), String> {
        let slot = self.slot_clock.now().ok_or("Failed to read slot clock")?;

        // TODO(focil) unused variable
        let _duration_to_next_slot = self
            .slot_clock
            .duration_to_next_slot()
            .ok_or("Unable to determine duration to next slot")?;

        let inclusion_list_duties = self.duties_service.inclusion_list_duties(slot);
        self.inner.context.executor.spawn_ignoring_error(
            self.clone()
                .produce_and_publish_inclusion_lists(slot, inclusion_list_duties),
            "inclusion list publish",
        );

        Ok(())
    }

    /// Downloads inclusion list objects, signs them, and returns them to the validator.
    ///
    /// ## Detail
    ///
    /// The given `validator_duties` should already be filtered to only contain those that match
    /// `slot`. Critical errors will be logged if this is not the case.
    ///
    /// Only one `InclusionList` is downloaded from the BN. It is then cloned and signed by each
    /// validator and the list of individually-signed `SignedInclusionList` objects is returned to
    /// the BN.
    async fn produce_and_publish_inclusion_lists(
        self,
        slot: Slot,
        validator_duties: Vec<InclusionListDutyData>,
    ) -> Result<(), ()> {
        let log = self.context.log();
        let validator_store = self.validator_store.clone();

        if validator_duties.is_empty() {
            return Ok(());
        }

        let current_epoch = self
            .slot_clock
            .now()
            .ok_or("Unable to determine current slot from clock")
            .map(|slot| slot.epoch(E::slots_per_epoch()));
        // TODO(focil) unused variable
        let _current_epoch = current_epoch.map_err(|e| {
            crit!(
                log,
                "Error during inclusion list routine";
                "error" => format!("{:?}", e),
                "slot" => slot.as_u64(),
            )
        })?;

        let inclusion_list = self
            .beacon_nodes
            .first_success(|beacon_node| async move {
                // TODO(focil) add timer metric
                beacon_node
                    .get_validator_inclusion_list(slot)
                    .await
                    .map_err(|e| format!("Failed to produce inclusion list: {:?}", e))
                    .map(|result| result.ok_or("Inclusion list unavailable".to_string()))?
                    .map(|result| result.data)
            })
            .await
            .map_err(|e| {
                crit!(
                    log,
                    "Error during inclusion list routine";
                    "error" => format!("{}", e),
                    "slot" => slot.as_u64(),
                )
            })?;

        // Create futures to produce signed `InclusionList` objects.
        let signing_futures = validator_duties.iter().map(|duty| {
            let inclusion_list = inclusion_list.clone();
            let validator_store = Arc::clone(&validator_store);
            async move {
                // Ensure that the inclusion list matches the duties.
                //
                // TODO: do we need to check any other fields here?
                if inclusion_list.slot != duty.slot {
                    crit!(
                        log,
                        "Inconsistent validator duties during signing";
                        "validator" => ?duty.pubkey,
                        "duty_slot" => duty.slot,
                        "inclusion_list_slot" => inclusion_list.slot,
                    );
                    return None;
                }

                match validator_store
                    .sign_inclusion_list(duty.pubkey, inclusion_list)
                    .await
                {
                    Ok(il) => Some((il, duty.validator_index)),
                    Err(ValidatorStoreError::UnknownPubkey(pubkey)) => {
                        // A pubkey can be missing when a validator was recently
                        // removed via the API.
                        warn!(
                            log,
                            "Missing pubkey for inclusion list";
                            "info" => "a validator may have recently been removed from this VC",
                            "pubkey" => ?pubkey,
                            "validator" => ?duty.pubkey,
                            "slot" => slot.as_u64(),
                        );
                        None
                    }
                    Err(e) => {
                        crit!(
                            log,
                            "Failed to sign inclusion list";
                            "error" => ?e,
                            "validator" => ?duty.pubkey,
                            "slot" => slot.as_u64(),
                        );
                        None
                    }
                }
            }
        });

        // Execute all the futures in parallel, collecting any successful results.
        let (ref inclusion_lists, ref validator_indices): (Vec<_>, Vec<_>) =
            join_all(signing_futures)
                .await
                .into_iter()
                .flatten()
                .unzip();

        if inclusion_lists.is_empty() {
            warn!(log, "No inclusion lists were published");
            return Ok(());
        }

        // Post the inclusion lists to the BN.
        match self
            .beacon_nodes
            .request(ApiTopic::InclusionLists, |beacon_node| async move {
                // TODO: add timer metric
                beacon_node
                    .post_beacon_pool_inclusion_lists(inclusion_lists)
                    .await
            })
            .await
        {
            Ok(()) => info!(
                log,
                "Successfully published inclusion lists";
                "count" => inclusion_lists.len(),
                "validator_indices" => ?validator_indices,
                "slot" => slot.as_u64(),
            ),
            Err(e) => error!(
                log,
                "Unable to publish inclusion lists";
                "error" => %e,
                "slot" => slot.as_u64(),
            ),
        }

        Ok(())
    }
}
