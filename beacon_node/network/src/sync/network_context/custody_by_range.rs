use super::custody_by_root::{ColumnRequest, Error};
use beacon_chain::validator_monitor::timestamp_now;
use beacon_chain::BeaconChainTypes;
use fnv::FnvHashMap;
use lighthouse_network::rpc::{methods::DataColumnsByRangeRequest, BlocksByRangeRequest};
use lighthouse_network::service::api_types::{
    CustodyByRangeRequestId, DataColumnsByRangeRequestId,
};
use lighthouse_network::{PeerAction, PeerId};
use lru_cache::LRUTimeCache;
use parking_lot::RwLock;
use rand::Rng;
use std::collections::HashSet;
use std::time::{Duration, Instant};
use std::{collections::HashMap, marker::PhantomData, sync::Arc};
use tracing::{debug, warn};
use types::{
    data_column_sidecar::ColumnIndex, DataColumnSidecar, DataColumnSidecarList, Hash256,
    SignedBeaconBlockHeader, Slot,
};

use super::{PeerGroup, RpcResponseResult, SyncNetworkContext};

const FAILED_PEERS_EXPIRY_SECONDS: u64 = 15;
const REQUEST_EXPIRY_SECONDS: u64 = 300;

pub struct ActiveCustodyByRangeRequest<T: BeaconChainTypes> {
    start_time: Instant,
    id: CustodyByRangeRequestId,
    request: BlocksByRangeRequest,
    /// Blocks that we expect peers to serve data columns for
    blocks_with_data: Vec<SignedBeaconBlockHeader>,
    /// List of column indices this request needs to download to complete successfully
    column_requests: FnvHashMap<
        ColumnIndex,
        ColumnRequest<DataColumnsByRangeRequestId, DataColumnSidecarList<T::EthSpec>>,
    >,
    /// Active requests for 1 or more columns each
    active_batch_columns_requests:
        FnvHashMap<DataColumnsByRangeRequestId, ActiveBatchColumnsRequest>,
    /// Peers that have recently failed to successfully respond to a columns by root request.
    /// Having a LRUTimeCache allows this request to not have to track disconnecting peers.
    failed_peers: LRUTimeCache<PeerId>,
    /// Set of peers that claim to have imported this block and their custody columns
    lookup_peers: Arc<RwLock<HashSet<PeerId>>>,

    _phantom: PhantomData<T>,
}

struct ActiveBatchColumnsRequest {
    indices: Vec<ColumnIndex>,
}

pub type CustodyByRangeRequestResult<E> =
    Result<Option<(DataColumnSidecarList<E>, PeerGroup, Duration)>, Error>;

enum ColumnResponseError {
    NonMatchingColumn {
        slot: Slot,
        actual_block_root: Hash256,
        expected_block_root: Hash256,
    },
    MissingColumn(Slot),
}

impl<T: BeaconChainTypes> ActiveCustodyByRangeRequest<T> {
    pub(crate) fn new(
        id: CustodyByRangeRequestId,
        request: BlocksByRangeRequest,
        blocks_with_data: Vec<SignedBeaconBlockHeader>,
        column_indices: &[ColumnIndex],
        lookup_peers: Arc<RwLock<HashSet<PeerId>>>,
    ) -> Self {
        Self {
            start_time: Instant::now(),
            id,
            request,
            blocks_with_data,
            column_requests: HashMap::from_iter(
                column_indices
                    .iter()
                    .map(|index| (*index, ColumnRequest::new())),
            ),
            active_batch_columns_requests: <_>::default(),
            failed_peers: LRUTimeCache::new(Duration::from_secs(FAILED_PEERS_EXPIRY_SECONDS)),
            lookup_peers,
            _phantom: PhantomData,
        }
    }

    /// Insert a downloaded column into an active custody request. Then make progress on the
    /// entire request.
    ///
    /// ### Returns
    ///
    /// - `Err`: Custody request has failed and will be dropped
    /// - `Ok(Some)`: Custody request has successfully completed and will be dropped
    /// - `Ok(None)`: Custody request still active
    pub(crate) fn on_data_column_downloaded(
        &mut self,
        peer_id: PeerId,
        req_id: DataColumnsByRangeRequestId,
        resp: RpcResponseResult<DataColumnSidecarList<T::EthSpec>>,
        cx: &mut SyncNetworkContext<T>,
    ) -> CustodyByRangeRequestResult<T::EthSpec> {
        let Some(batch_request) = self.active_batch_columns_requests.get_mut(&req_id) else {
            warn!(
                id = %self.id,
                %req_id,
                "Received custody by range response for unrequested index"
            );
            return Ok(None);
        };

        match resp {
            Ok((data_columns, seen_timestamp)) => {
                // Map columns by index as an optimization to not loop the returned list on each
                // requested index. The worse case is 128 loops over a 128 item vec + mutation to
                // drop the consumed columns.
                let mut data_columns_by_index =
                    HashMap::<(ColumnIndex, Slot), Arc<DataColumnSidecar<T::EthSpec>>>::new();
                for data_column in data_columns {
                    data_columns_by_index
                        .insert((data_column.index, data_column.slot()), data_column);
                }

                // Accumulate columns that the peer does not have to issue a single log per request
                let mut missing_column_indices = vec![];
                let mut incorrect_column_indices = vec![];
                let mut imported_column_indices = vec![];

                for index in &batch_request.indices {
                    let column_request =
                        self.column_requests
                            .get_mut(index)
                            .ok_or(Error::InternalError(format!(
                                "unknown column_index {index}"
                            )))?;

                    let columns_at_index = self
                        .blocks_with_data
                        .iter()
                        .map(|block| {
                            let slot = block.message.slot;
                            if let Some(data_column) = data_columns_by_index.remove(&(*index, slot))
                            {
                                let actual_block_root =
                                    data_column.signed_block_header.message.canonical_root();
                                let expected_block_root = block.message.canonical_root();
                                if actual_block_root != expected_block_root {
                                    Err(ColumnResponseError::NonMatchingColumn {
                                        slot,
                                        actual_block_root: data_column
                                            .signed_block_header
                                            .message
                                            .canonical_root(),
                                        expected_block_root: block.message.canonical_root(),
                                    })
                                } else {
                                    Ok(data_column)
                                }
                            } else {
                                // The following three statements are true:
                                // - block at `slot` is not missed, and has data
                                // - peer custodies this column `index`
                                // - peer claims to be synced to at least `slot`
                                //
                                // Then we penalize the faulty peer, mark it as failed and try with
                                // another.
                                Err(ColumnResponseError::MissingColumn(slot))
                            }
                        })
                        .collect::<Result<Vec<_>, _>>();

                    match columns_at_index {
                        Ok(columns_at_index) => {
                            column_request.on_download_success(
                                req_id,
                                peer_id,
                                columns_at_index,
                                seen_timestamp,
                            )?;

                            imported_column_indices.push(index);
                        }
                        Err(e) => {
                            column_request.on_download_error(req_id)?;

                            match e {
                                ColumnResponseError::NonMatchingColumn {
                                    slot,
                                    actual_block_root,
                                    expected_block_root,
                                } => {
                                    incorrect_column_indices.push((
                                        index,
                                        slot,
                                        actual_block_root,
                                        expected_block_root,
                                    ));
                                }
                                ColumnResponseError::MissingColumn(slot) => {
                                    missing_column_indices.push((index, slot));
                                }
                            }
                        }
                    }
                }

                // Log `imported_column_indices`, `missing_column_indexes` and
                // `incorrect_column_indices` once per request to make the logs less noisy.
                if !imported_column_indices.is_empty() {
                    // TODO(das): this log may be redundant. We already log on DataColumnsByRange
                    // completed, and on DataColumnsByRange sent we log the column indices
                    // ```
                    // Sync RPC request sent method="DataColumnsByRange" slots=8 epoch=4 columns=[52] peer=16Uiu2HAmEooeoHzHDYS35TSHrJDSfmREecPyFskrLPYm9Gm1EURj id=493/399/10/RangeSync/4/1
                    // Sync RPC request completed id=493/399/10/RangeSync/4/1 method="DataColumnsByRange" count=1
                    // ```
                    // Which can be traced to this custody by range request, and the initial log
                    debug!(
                        id = %self.id,
                        data_columns_by_range_req_id = %req_id,
                        %peer_id,
                        count = imported_column_indices.len(),
                        "Custody by range request download imported columns"
                    );
                }

                if !incorrect_column_indices.is_empty() {
                    debug!(
                        id = %self.id,
                        data_columns_by_range_req_id = %req_id,
                        %peer_id,
                        ?incorrect_column_indices,
                        "Custody by range peer returned non-matching columns"
                    );

                    // Returning a non-canonical column is not a permanent fault. We should not
                    // retry the peer for some time but the peer may return a canonical column in
                    // the future.
                    self.failed_peers.insert(peer_id);
                    cx.report_peer(
                        peer_id,
                        PeerAction::MidToleranceError,
                        "non-matching data column",
                    );
                }

                if !missing_column_indices.is_empty() {
                    debug!(
                        id = %self.id,
                        data_columns_by_range_req_id = %req_id,
                        %peer_id,
                        ?missing_column_indices,
                        "Custody by range peer claims to not have some data"
                    );

                    // Not having columns is not a permanent fault. The peer may be backfilling.
                    self.failed_peers.insert(peer_id);
                    cx.report_peer(peer_id, PeerAction::MidToleranceError, "custody_failure");
                }
            }
            Err(err) => {
                debug!(
                    id = %self.id,
                    %req_id,
                    %peer_id,
                    error = ?err,
                    "Custody by range download error"
                );

                for column_index in &batch_request.indices {
                    self.column_requests
                        .get_mut(column_index)
                        .ok_or(Error::InternalError("unknown column_index".to_owned()))?
                        .on_download_error_and_mark_failure(req_id, err.clone())?;
                }

                // An RpcResponseError is already downscored in network_context
                self.failed_peers.insert(peer_id);
            }
        };

        self.continue_requests(cx)
    }

    pub(crate) fn continue_requests(
        &mut self,
        cx: &mut SyncNetworkContext<T>,
    ) -> CustodyByRangeRequestResult<T::EthSpec> {
        if self.column_requests.values().all(|r| r.is_downloaded()) {
            // All requests have completed successfully.
            let mut peers = HashMap::<PeerId, Vec<usize>>::new();
            let mut seen_timestamps = vec![];
            let columns = std::mem::take(&mut self.column_requests)
                .into_values()
                .map(|request| {
                    let (peer, data_columns, seen_timestamp) = request.complete()?;

                    for data_column in &data_columns {
                        let columns_by_peer = peers.entry(peer).or_default();
                        if !columns_by_peer.contains(&(data_column.index as usize)) {
                            columns_by_peer.push(data_column.index as usize);
                        }
                    }

                    seen_timestamps.push(seen_timestamp);

                    Ok(data_columns)
                })
                .collect::<Result<Vec<_>, _>>()?
                // Flatten Vec<Vec<Columns>> to Vec<Columns>
                .into_iter()
                .flatten()
                .collect();

            let peer_group = PeerGroup::from_set(peers);
            let max_seen_timestamp = seen_timestamps.into_iter().max().unwrap_or(timestamp_now());
            return Ok(Some((columns, peer_group, max_seen_timestamp)));
        }

        let active_request_count_by_peer = cx.active_request_count_by_peer();
        let mut columns_to_request_by_peer = HashMap::<PeerId, Vec<ColumnIndex>>::new();
        let lookup_peers = self.lookup_peers.read();

        // Need to:
        // - track how many active requests a peer has for load balancing
        // - which peers have failures to attempt others
        // - which peer returned what to have PeerGroup attributability

        for (column_index, request) in self.column_requests.iter_mut() {
            if request.is_awaiting_download() {
                if let Some(last_error) = request.too_many_failures() {
                    return Err(Error::TooManyDownloadErrors(last_error));
                }

                // TODO(das): We should only query peers that are likely to know about this block.
                // For by_range requests, only peers in the SyncingChain peer set. Else consider a
                // fallback to the peers that are synced up to the epoch we want to query.
                let custodial_peers = cx.get_custodial_peers(*column_index);

                // We draw from the total set of peers, but prioritize those peers who we have
                // received an attestation / status / block message claiming to have imported the
                // lookup. The frequency of those messages is low, so drawing only from lookup_peers
                // could cause many lookups to take much longer or fail as they don't have enough
                // custody peers on a given column
                let mut priorized_peers = custodial_peers
                    .iter()
                    .filter(|peer| {
                        // Do not request faulty peers for some time
                        !self.failed_peers.contains(peer)
                    })
                    .map(|peer| {
                        (
                            // Prioritize peers that claim to know have imported this block
                            if lookup_peers.contains(peer) { 0 } else { 1 },
                            // Prefer peers with fewer requests to load balance across peers.
                            // We batch requests to the same peer, so count existence in the
                            // `columns_to_request_by_peer` as a single 1 request.
                            active_request_count_by_peer.get(peer).copied().unwrap_or(0)
                                + columns_to_request_by_peer.get(peer).map(|_| 1).unwrap_or(0),
                            // Random factor to break ties, otherwise the PeerID breaks ties
                            rand::thread_rng().gen::<u32>(),
                            *peer,
                        )
                    })
                    .collect::<Vec<_>>();
                priorized_peers.sort_unstable();

                if let Some((_, _, _, peer_id)) = priorized_peers.first() {
                    columns_to_request_by_peer
                        .entry(*peer_id)
                        .or_default()
                        .push(*column_index);
                } else {
                    // Do not issue requests if there is no custody peer on this column. The request
                    // will sit idle without making progress. The only way to make to progress is:
                    // - Add a new peer that custodies the missing columns
                    // - Call `continue_requests`
                    //
                    // Otherwise this request will be dropped and failed after some time.
                }
            }
        }

        for (peer_id, indices) in columns_to_request_by_peer.into_iter() {
            let req_id = cx
                .send_data_columns_by_range_request(
                    peer_id,
                    DataColumnsByRangeRequest {
                        start_slot: *self.request.start_slot(),
                        count: *self.request.count(),
                        columns: indices.clone(),
                    },
                    self.id,
                )
                .map_err(|e| Error::InternalError(format!("send failed {e}")))?;

            for column_index in &indices {
                let column_request = self
                    .column_requests
                    .get_mut(column_index)
                    // Should never happen: column_index is iterated from column_requests
                    .ok_or(Error::InternalError(format!(
                        "Unknown column_request {column_index}"
                    )))?;

                column_request.on_download_start(req_id)?;
            }

            self.active_batch_columns_requests
                .insert(req_id, ActiveBatchColumnsRequest { indices });
        }

        if self.start_time.elapsed() > Duration::from_secs(REQUEST_EXPIRY_SECONDS)
            && !self.column_requests.values().any(|r| r.is_downloading())
        {
            let awaiting_peers_indicies = self
                .column_requests
                .iter()
                .filter(|(_, r)| r.is_awaiting_download())
                .map(|(id, _)| *id)
                .collect::<Vec<_>>();
            return Err(Error::ExpiredNoCustodyPeers(awaiting_peers_indicies));
        }

        Ok(None)
    }
}
