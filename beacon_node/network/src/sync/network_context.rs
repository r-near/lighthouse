//! Provides network functionality for the Syncing thread. This fundamentally wraps a network
//! channel and stores a global RPC ID to perform requests.

use self::custody_by_range::ActiveCustodyByRangeRequest;
use self::custody_by_root::ActiveCustodyByRootRequest;
pub use self::requests::{BlocksByRootSingleRequest, DataColumnsByRootSingleBlockRequest};
use super::manager::BlockProcessType;
use super::range_sync::BatchPeers;
use super::SyncMessage;
use crate::metrics;
use crate::network_beacon_processor::NetworkBeaconProcessor;
#[cfg(test)]
use crate::network_beacon_processor::TestBeaconChainType;
use crate::service::NetworkMessage;
use crate::status::ToStatusMessage;
use crate::sync::block_lookups::SingleLookupId;
use crate::sync::network_context::requests::BlobsByRootSingleBlockRequest;
use beacon_chain::block_verification_types::RpcBlock;
use beacon_chain::{BeaconChain, BeaconChainTypes, BlockProcessStatus, EngineState};
pub use block_components_by_range::BlockComponentsByRangeRequest;
#[cfg(test)]
pub use block_components_by_range::BlockComponentsByRangeRequestStep;
use fnv::FnvHashMap;
use lighthouse_network::rpc::methods::{BlobsByRangeRequest, DataColumnsByRangeRequest};
use lighthouse_network::rpc::{BlocksByRangeRequest, GoodbyeReason, RPCError, RequestType};
pub use lighthouse_network::service::api_types::RangeRequestId;
use lighthouse_network::service::api_types::{
    AppRequestId, BlobsByRangeRequestId, BlocksByRangeRequestId, ComponentsByRangeRequestId,
    CustodyByRangeRequestId, CustodyId, CustodyRequester, DataColumnsByRangeRequestId,
    DataColumnsByRootRequestId, DataColumnsByRootRequester, Id, SingleLookupReqId, SyncRequestId,
};
use lighthouse_network::{Client, NetworkGlobals, PeerAction, PeerId, ReportSource};
use parking_lot::RwLock;
pub use requests::LookupVerifyError;
use requests::{
    ActiveRequests, BlobsByRangeRequestItems, BlobsByRootRequestItems, BlocksByRangeRequestItems,
    BlocksByRootRequestItems, DataColumnsByRangeRequestItems, DataColumnsByRootRequestItems,
};
#[cfg(test)]
use slot_clock::SlotClock;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
#[cfg(test)]
use task_executor::TaskExecutor;
use tokio::sync::mpsc;
use tracing::{debug, error, span, warn, Level};
use types::blob_sidecar::FixedBlobSidecarList;
use types::{
    BlobSidecar, ChainSpec, ColumnIndex, DataColumnSidecar, DataColumnSidecarList, Epoch, EthSpec,
    ForkContext, Hash256, SignedBeaconBlock, SignedBeaconBlockHeader, Slot,
};

pub mod block_components_by_range;
pub mod custody_by_range;
pub mod custody_by_root;
mod requests;

#[derive(Debug)]
pub enum RpcEvent<T> {
    StreamTermination,
    Response(T, Duration),
    RPCError(RPCError),
}

impl<T> RpcEvent<T> {
    pub fn from_chunk(chunk: Option<T>, seen_timestamp: Duration) -> Self {
        match chunk {
            Some(item) => RpcEvent::Response(item, seen_timestamp),
            None => RpcEvent::StreamTermination,
        }
    }
}

pub type RpcResponseResult<T> = Result<(T, Duration), RpcResponseError>;

/// Duration = latest seen timestamp of all received data columns
pub type RpcResponseBatchResult<T> = Result<(T, PeerGroup, Duration), RpcResponseError>;

/// Common result type for `custody_by_root` and `custody_by_range` requests. The peers are part of
/// the `Ok` response since they are not known until the entire request succeeds.
pub type CustodyRequestResult<E> = RpcResponseBatchResult<DataColumnSidecarList<E>>;

#[derive(Debug, Clone)]
pub enum RpcResponseError {
    RpcError(#[allow(dead_code)] RPCError),
    VerifyError(LookupVerifyError),
    RequestExpired(String),
    InternalError(#[allow(dead_code)] String),
}

#[derive(Debug, PartialEq, Eq)]
pub enum RpcRequestSendError {
    /// These errors should never happen, including unreachable custody errors or network send
    /// errors.
    InternalError(String),
    // If RpcRequestSendError has a single variant `InternalError` it's to signal to downstream
    // consumers that sends are expected to be infallible. If this assumption changes in the future,
    // add a new variant.
}

#[derive(Debug, PartialEq, Eq)]
pub enum SendErrorProcessor {
    SendError,
    ProcessorNotAvailable,
}

impl From<RPCError> for RpcResponseError {
    fn from(e: RPCError) -> Self {
        RpcResponseError::RpcError(e)
    }
}

impl From<LookupVerifyError> for RpcResponseError {
    fn from(e: LookupVerifyError) -> Self {
        RpcResponseError::VerifyError(e)
    }
}

/// Represents a group of peers that served a block component.
#[derive(Clone, Debug)]
pub struct PeerGroup {
    /// Peers group by which indexed section of the block component they served. For example:
    /// - PeerA served = [blob index 0, blob index 2]
    /// - PeerA served = [blob index 1]
    peers: HashMap<PeerId, Vec<usize>>,
}

impl PeerGroup {
    /// Return a peer group where a single peer returned all parts of a block component. For
    /// example, a block has a single component (the block = index 0/1).
    pub fn from_single(peer: PeerId) -> Self {
        Self {
            peers: HashMap::from_iter([(peer, vec![0])]),
        }
    }
    pub fn from_set(peers: HashMap<PeerId, Vec<usize>>) -> Self {
        Self { peers }
    }
    pub fn all(&self) -> impl Iterator<Item = &PeerId> + '_ {
        self.peers.keys()
    }
    pub fn of_index(&self, index: usize) -> impl Iterator<Item = &PeerId> + '_ {
        self.peers.iter().filter_map(move |(peer, indices)| {
            if indices.contains(&index) {
                Some(peer)
            } else {
                None
            }
        })
    }

    pub fn as_reversed_map(&self) -> HashMap<u64, PeerId> {
        // TODO(das): should we change PeerGroup to hold this map?
        let mut index_to_peer = HashMap::<u64, PeerId>::new();
        for (peer, indices) in self.peers.iter() {
            for &index in indices {
                index_to_peer.insert(index as u64, *peer);
            }
        }
        index_to_peer
    }
}

/// Sequential ID that uniquely identifies ReqResp outgoing requests
pub type ReqId = u32;

pub enum LookupRequestResult<I = ReqId> {
    /// A request is sent. Sync MUST receive an event from the network in the future for either:
    /// completed response or failed request
    RequestSent(I),
    /// No request is sent, and no further action is necessary to consider this request completed.
    /// Includes a reason why this request is not needed.
    NoRequestNeeded(&'static str),
    /// No request is sent, but the request is not completed. Sync MUST receive some future event
    /// that makes progress on the request. For example: request is processing from a different
    /// source (i.e. block received from gossip) and sync MUST receive an event with that processing
    /// result.
    Pending(&'static str),
}

/// Wraps a Network channel to employ various RPC related network functionality for the Sync manager. This includes management of a global RPC request Id.
pub struct SyncNetworkContext<T: BeaconChainTypes> {
    /// The network channel to relay messages to the Network service.
    network_send: mpsc::UnboundedSender<NetworkMessage<T::EthSpec>>,

    /// A sequential ID for all RPC requests.
    request_id: Id,

    /// A mapping of active BlocksByRoot requests, including both current slot and parent lookups.
    blocks_by_root_requests:
        ActiveRequests<SingleLookupReqId, BlocksByRootRequestItems<T::EthSpec>>,
    /// A mapping of active BlobsByRoot requests, including both current slot and parent lookups.
    blobs_by_root_requests: ActiveRequests<SingleLookupReqId, BlobsByRootRequestItems<T::EthSpec>>,
    /// A mapping of active DataColumnsByRoot requests
    data_columns_by_root_requests:
        ActiveRequests<DataColumnsByRootRequestId, DataColumnsByRootRequestItems<T::EthSpec>>,
    /// A mapping of active BlocksByRange requests
    blocks_by_range_requests:
        ActiveRequests<BlocksByRangeRequestId, BlocksByRangeRequestItems<T::EthSpec>>,
    /// A mapping of active BlobsByRange requests
    blobs_by_range_requests:
        ActiveRequests<BlobsByRangeRequestId, BlobsByRangeRequestItems<T::EthSpec>>,
    /// A mapping of active DataColumnsByRange requests
    data_columns_by_range_requests:
        ActiveRequests<DataColumnsByRangeRequestId, DataColumnsByRangeRequestItems<T::EthSpec>>,

    /// Mapping of active custody column by root requests for a block root
    custody_by_root_requests: FnvHashMap<CustodyRequester, ActiveCustodyByRootRequest<T>>,

    /// Mapping of active custody column by range requests
    custody_by_range_requests: FnvHashMap<CustodyByRangeRequestId, ActiveCustodyByRangeRequest<T>>,

    /// BlocksByRange requests paired with other ByRange requests for data components
    block_components_by_range_requests:
        FnvHashMap<ComponentsByRangeRequestId, BlockComponentsByRangeRequest<T>>,

    /// Whether the ee is online. If it's not, we don't allow access to the
    /// `beacon_processor_send`.
    execution_engine_state: EngineState,

    /// Sends work to the beacon processor via a channel.
    network_beacon_processor: Arc<NetworkBeaconProcessor<T>>,

    pub chain: Arc<BeaconChain<T>>,

    fork_context: Arc<ForkContext>,
}

/// Small enumeration to make dealing with block and blob requests easier.
pub enum RangeBlockComponent<E: EthSpec> {
    Block(
        BlocksByRangeRequestId,
        RpcResponseResult<Vec<Arc<SignedBeaconBlock<E>>>>,
        PeerId,
    ),
    Blob(
        BlobsByRangeRequestId,
        RpcResponseResult<Vec<Arc<BlobSidecar<E>>>>,
        PeerId,
    ),
    CustodyColumns(
        CustodyByRangeRequestId,
        RpcResponseResult<Vec<Arc<DataColumnSidecar<E>>>>,
        PeerGroup,
    ),
}

#[cfg(test)]
impl<E: EthSpec> SyncNetworkContext<TestBeaconChainType<E>> {
    pub fn new_for_testing(
        beacon_chain: Arc<BeaconChain<TestBeaconChainType<E>>>,
        network_globals: Arc<NetworkGlobals<E>>,
        task_executor: TaskExecutor,
    ) -> Self {
        let fork_context = Arc::new(ForkContext::new::<E>(
            beacon_chain.slot_clock.now().unwrap_or(Slot::new(0)),
            beacon_chain.genesis_validators_root,
            &beacon_chain.spec,
        ));
        let (network_tx, _network_rx) = mpsc::unbounded_channel();
        let (beacon_processor, _) = NetworkBeaconProcessor::null_for_testing(
            network_globals,
            mpsc::unbounded_channel().0,
            beacon_chain.clone(),
            task_executor,
        );

        SyncNetworkContext::new(
            network_tx,
            Arc::new(beacon_processor),
            beacon_chain,
            fork_context,
        )
    }
}

impl<T: BeaconChainTypes> SyncNetworkContext<T> {
    pub fn new(
        network_send: mpsc::UnboundedSender<NetworkMessage<T::EthSpec>>,
        network_beacon_processor: Arc<NetworkBeaconProcessor<T>>,
        chain: Arc<BeaconChain<T>>,
        fork_context: Arc<ForkContext>,
    ) -> Self {
        let span = span!(
            Level::INFO,
            "SyncNetworkContext",
            service = "network_context"
        );
        let _enter = span.enter();
        SyncNetworkContext {
            network_send,
            execution_engine_state: EngineState::Online, // always assume `Online` at the start
            request_id: 1,
            blocks_by_root_requests: ActiveRequests::new("blocks_by_root"),
            blobs_by_root_requests: ActiveRequests::new("blobs_by_root"),
            data_columns_by_root_requests: ActiveRequests::new("data_columns_by_root"),
            blocks_by_range_requests: ActiveRequests::new("blocks_by_range"),
            blobs_by_range_requests: ActiveRequests::new("blobs_by_range"),
            data_columns_by_range_requests: ActiveRequests::new("data_columns_by_range"),
            custody_by_root_requests: <_>::default(),
            custody_by_range_requests: <_>::default(),
            block_components_by_range_requests: <_>::default(),
            network_beacon_processor,
            chain,
            fork_context,
        }
    }

    pub fn send_sync_message(&mut self, sync_message: SyncMessage<T::EthSpec>) {
        self.network_beacon_processor
            .send_sync_message(sync_message);
    }

    /// Returns the ids of all the requests made to the given peer_id.
    pub fn peer_disconnected(&mut self, peer_id: &PeerId) -> Vec<SyncRequestId> {
        self.active_requests()
            .filter(|(_, request_peer)| *request_peer == peer_id)
            .map(|(id, _)| id)
            .collect()
    }

    /// Returns the ids of all active requests
    pub fn active_requests(&mut self) -> impl Iterator<Item = (SyncRequestId, &PeerId)> {
        // Note: using destructuring pattern without a default case to make sure we don't forget to
        // add new request types to this function. Otherwise, lookup sync can break and lookups
        // will get stuck if a peer disconnects during an active requests.
        let Self {
            network_send: _,
            request_id: _,
            blocks_by_root_requests,
            blobs_by_root_requests,
            data_columns_by_root_requests,
            blocks_by_range_requests,
            blobs_by_range_requests,
            data_columns_by_range_requests,
            // custody_by_root_requests is a meta request of data_columns_by_root_requests
            custody_by_root_requests: _,
            custody_by_range_requests: _,
            // components_by_range_requests is a meta request of various _by_range requests
            block_components_by_range_requests: _,
            execution_engine_state: _,
            network_beacon_processor: _,
            chain: _,
            fork_context: _,
        } = self;

        let blocks_by_root_ids = blocks_by_root_requests
            .active_requests()
            .map(|(id, peer)| (SyncRequestId::SingleBlock { id: *id }, peer));
        let blobs_by_root_ids = blobs_by_root_requests
            .active_requests()
            .map(|(id, peer)| (SyncRequestId::SingleBlob { id: *id }, peer));
        let data_column_by_root_ids = data_columns_by_root_requests
            .active_requests()
            .map(|(id, peer)| (SyncRequestId::DataColumnsByRoot(*id), peer));
        let blocks_by_range_ids = blocks_by_range_requests
            .active_requests()
            .map(|(id, peer)| (SyncRequestId::BlocksByRange(*id), peer));
        let blobs_by_range_ids = blobs_by_range_requests
            .active_requests()
            .map(|(id, peer)| (SyncRequestId::BlobsByRange(*id), peer));
        let data_column_by_range_ids = data_columns_by_range_requests
            .active_requests()
            .map(|(id, peer)| (SyncRequestId::DataColumnsByRange(*id), peer));

        blocks_by_root_ids
            .chain(blobs_by_root_ids)
            .chain(data_column_by_root_ids)
            .chain(blocks_by_range_ids)
            .chain(blobs_by_range_ids)
            .chain(data_column_by_range_ids)
    }

    #[cfg(test)]
    pub fn active_block_components_by_range_requests(
        &self,
    ) -> Vec<(
        ComponentsByRangeRequestId,
        BlockComponentsByRangeRequestStep,
    )> {
        self.block_components_by_range_requests
            .iter()
            .map(|(id, req)| (*id, req.state_step()))
            .collect()
    }

    pub fn get_custodial_peers(&self, column_index: ColumnIndex) -> Vec<PeerId> {
        self.network_globals()
            .custody_peers_for_column(column_index)
    }

    pub fn network_globals(&self) -> &NetworkGlobals<T::EthSpec> {
        &self.network_beacon_processor.network_globals
    }

    pub fn spec(&self) -> &ChainSpec {
        &self.chain.spec
    }

    /// Returns the Client type of the peer if known
    pub fn client_type(&self, peer_id: &PeerId) -> Client {
        self.network_globals()
            .peers
            .read()
            .peer_info(peer_id)
            .map(|info| info.client().clone())
            .unwrap_or_default()
    }

    pub fn status_peers<C: ToStatusMessage>(&self, chain: &C, peers: impl Iterator<Item = PeerId>) {
        let span = span!(
            Level::INFO,
            "SyncNetworkContext",
            service = "network_context"
        );
        let _enter = span.enter();

        let status_message = chain.status_message();
        for peer_id in peers {
            debug!(
                peer = %peer_id,
                fork_digest = ?status_message.fork_digest,
                finalized_root = ?status_message.finalized_root,
                finalized_epoch = ?status_message.finalized_epoch,
                head_root = %status_message.head_root,
                head_slot = %status_message.head_slot,
                "Sending Status Request"
            );

            let request = RequestType::Status(status_message.clone());
            let app_request_id = AppRequestId::Router;
            let _ = self.send_network_msg(NetworkMessage::SendRequest {
                peer_id,
                request,
                app_request_id,
            });
        }
    }

    fn active_request_count_by_peer(&self) -> HashMap<PeerId, usize> {
        let Self {
            network_send: _,
            request_id: _,
            blocks_by_root_requests,
            blobs_by_root_requests,
            data_columns_by_root_requests,
            blocks_by_range_requests,
            blobs_by_range_requests,
            data_columns_by_range_requests,
            // custody_by_root_requests is a meta request of data_columns_by_root_requests
            custody_by_root_requests: _,
            custody_by_range_requests: _,
            // components_by_range_requests is a meta request of various _by_range requests
            block_components_by_range_requests: _,
            execution_engine_state: _,
            network_beacon_processor: _,
            chain: _,
            fork_context: _,
            // Don't use a fallback match. We want to be sure that all requests are considered when
            // adding new ones
        } = self;

        let mut active_request_count_by_peer = HashMap::<PeerId, usize>::new();

        for peer_id in blocks_by_root_requests
            .iter_request_peers()
            .chain(blobs_by_root_requests.iter_request_peers())
            .chain(data_columns_by_root_requests.iter_request_peers())
            .chain(blocks_by_range_requests.iter_request_peers())
            .chain(blobs_by_range_requests.iter_request_peers())
            .chain(data_columns_by_range_requests.iter_request_peers())
        {
            *active_request_count_by_peer.entry(peer_id).or_default() += 1;
        }

        active_request_count_by_peer
    }

    /// A blocks by range request sent by the range sync algorithm
    pub fn block_components_by_range_request(
        &mut self,
        request: BlocksByRangeRequest,
        requester: RangeRequestId,
        peers: &HashSet<PeerId>,
        peers_to_deprioritize: &HashSet<PeerId>,
        total_requests_per_peer: &HashMap<PeerId, usize>,
    ) -> Result<Id, RpcRequestSendError> {
        let id = ComponentsByRangeRequestId {
            id: self.next_id(),
            requester,
        };

        let req = BlockComponentsByRangeRequest::new(
            id,
            request,
            peers,
            peers_to_deprioritize,
            total_requests_per_peer,
            self,
        )?;

        self.block_components_by_range_requests.insert(id, req);

        // TODO: use ID
        Ok(id.id)
    }

    /// Request block of `block_root` if necessary by checking:
    /// - If the da_checker has a pending block from gossip or a previous request
    ///
    /// Returns false if no request was made, because the block is already imported
    pub fn block_lookup_request(
        &mut self,
        lookup_id: SingleLookupId,
        lookup_peers: Arc<RwLock<HashSet<PeerId>>>,
        block_root: Hash256,
    ) -> Result<LookupRequestResult, RpcRequestSendError> {
        let active_request_count_by_peer = self.active_request_count_by_peer();
        let Some(peer_id) = lookup_peers
            .read()
            .iter()
            .map(|peer| {
                (
                    // Prefer peers with less overall requests
                    active_request_count_by_peer.get(peer).copied().unwrap_or(0),
                    // Random factor to break ties, otherwise the PeerID breaks ties
                    rand::random::<u32>(),
                    peer,
                )
            })
            .min()
            .map(|(_, _, peer)| *peer)
        else {
            // Allow lookup to not have any peers and do nothing. This is an optimization to not
            // lose progress of lookups created from a block with unknown parent before we receive
            // attestations for said block.
            // Lookup sync event safety: If a lookup requires peers to make progress, and does
            // not receive any new peers for some time it will be dropped. If it receives a new
            // peer it must attempt to make progress.
            return Ok(LookupRequestResult::Pending("no peers"));
        };

        let span = span!(
            Level::INFO,
            "SyncNetworkContext",
            service = "network_context"
        );
        let _enter = span.enter();

        match self.chain.get_block_process_status(&block_root) {
            // Unknown block, continue request to download
            BlockProcessStatus::Unknown => {}
            // Block is known are currently processing, expect a future event with the result of
            // processing.
            BlockProcessStatus::NotValidated { .. } => {
                // Lookup sync event safety: If the block is currently in the processing cache, we
                // are guaranteed to receive a `SyncMessage::GossipBlockProcessResult` that will
                // make progress on this lookup
                return Ok(LookupRequestResult::Pending("block in processing cache"));
            }
            // Block is fully validated. If it's not yet imported it's waiting for missing block
            // components. Consider this request completed and do nothing.
            BlockProcessStatus::ExecutionValidated { .. } => {
                return Ok(LookupRequestResult::NoRequestNeeded(
                    "block execution validated",
                ))
            }
        }

        let id = SingleLookupReqId {
            lookup_id,
            req_id: self.next_id(),
        };

        let request = BlocksByRootSingleRequest(block_root);

        // Lookup sync event safety: If network_send.send() returns Ok(_) we are guaranteed that
        // eventually at least one this 3 events will be received:
        // - StreamTermination(request_id): handled by `Self::on_single_block_response`
        // - RPCError(request_id): handled by `Self::on_single_block_response`
        // - Disconnect(peer_id) handled by `Self::peer_disconnected``which converts it to a
        // ` RPCError(request_id)`event handled by the above method
        self.network_send
            .send(NetworkMessage::SendRequest {
                peer_id,
                request: RequestType::BlocksByRoot(request.into_request(&self.fork_context)),
                app_request_id: AppRequestId::Sync(SyncRequestId::SingleBlock { id }),
            })
            .map_err(|_| RpcRequestSendError::InternalError("network send error".to_owned()))?;

        debug!(
            method = "BlocksByRoot",
            ?block_root,
            peer = %peer_id,
            %id,
            "Sync RPC request sent"
        );

        self.blocks_by_root_requests.insert(
            id,
            peer_id,
            // true = enforce max_requests as returned for blocks_by_root. We always request a single
            // block and the peer must have it.
            true,
            BlocksByRootRequestItems::new(request),
        );

        Ok(LookupRequestResult::RequestSent(id.req_id))
    }

    /// Request necessary blobs for `block_root`. Requests only the necessary blobs by checking:
    /// - If we have a downloaded but not yet processed block
    /// - If the da_checker has a pending block
    /// - If the da_checker has pending blobs from gossip
    ///
    /// Returns false if no request was made, because we don't need to import (more) blobs.
    pub fn blob_lookup_request(
        &mut self,
        lookup_id: SingleLookupId,
        lookup_peers: Arc<RwLock<HashSet<PeerId>>>,
        block_root: Hash256,
        expected_blobs: usize,
    ) -> Result<LookupRequestResult, RpcRequestSendError> {
        let active_request_count_by_peer = self.active_request_count_by_peer();
        let Some(peer_id) = lookup_peers
            .read()
            .iter()
            .map(|peer| {
                (
                    // Prefer peers with less overall requests
                    active_request_count_by_peer.get(peer).copied().unwrap_or(0),
                    // Random factor to break ties, otherwise the PeerID breaks ties
                    rand::random::<u32>(),
                    peer,
                )
            })
            .min()
            .map(|(_, _, peer)| *peer)
        else {
            // Allow lookup to not have any peers and do nothing. This is an optimization to not
            // lose progress of lookups created from a block with unknown parent before we receive
            // attestations for said block.
            // Lookup sync event safety: If a lookup requires peers to make progress, and does
            // not receive any new peers for some time it will be dropped. If it receives a new
            // peer it must attempt to make progress.
            return Ok(LookupRequestResult::Pending("no peers"));
        };

        let span = span!(
            Level::INFO,
            "SyncNetworkContext",
            service = "network_context"
        );
        let _enter = span.enter();

        let imported_blob_indexes = self
            .chain
            .data_availability_checker
            .cached_blob_indexes(&block_root)
            .unwrap_or_default();
        // Include only the blob indexes not yet imported (received through gossip)
        let indices = (0..expected_blobs as u64)
            .filter(|index| !imported_blob_indexes.contains(index))
            .collect::<Vec<_>>();

        if indices.is_empty() {
            // No blobs required, do not issue any request
            return Ok(LookupRequestResult::NoRequestNeeded("no indices to fetch"));
        }

        let id = SingleLookupReqId {
            lookup_id,
            req_id: self.next_id(),
        };

        let request = BlobsByRootSingleBlockRequest {
            block_root,
            indices: indices.clone(),
        };

        // Lookup sync event safety: Refer to `Self::block_lookup_request` `network_send.send` call
        self.network_send
            .send(NetworkMessage::SendRequest {
                peer_id,
                request: RequestType::BlobsByRoot(request.clone().into_request(&self.fork_context)),
                app_request_id: AppRequestId::Sync(SyncRequestId::SingleBlob { id }),
            })
            .map_err(|_| RpcRequestSendError::InternalError("network send error".to_owned()))?;

        debug!(
            method = "BlobsByRoot",
            ?block_root,
            blob_indices = ?indices,
            peer = %peer_id,
            %id,
            "Sync RPC request sent"
        );

        self.blobs_by_root_requests.insert(
            id,
            peer_id,
            // true = enforce max_requests are returned for blobs_by_root. We only issue requests for
            // blocks after we know the block has data, and only request peers after they claim to
            // have imported the block+blobs.
            true,
            BlobsByRootRequestItems::new(request),
        );

        Ok(LookupRequestResult::RequestSent(id.req_id))
    }

    /// Request to send a single `data_columns_by_root` request to the network.
    pub fn data_columns_by_root_request(
        &mut self,
        requester: DataColumnsByRootRequester,
        peer_id: PeerId,
        request: DataColumnsByRootSingleBlockRequest,
        expect_max_responses: bool,
    ) -> Result<LookupRequestResult<DataColumnsByRootRequestId>, &'static str> {
        let span = span!(
            Level::INFO,
            "SyncNetworkContext",
            service = "network_context"
        );
        let _enter = span.enter();

        let id = DataColumnsByRootRequestId {
            id: self.next_id(),
            requester,
        };

        self.send_network_msg(NetworkMessage::SendRequest {
            peer_id,
            request: RequestType::DataColumnsByRoot(
                request
                    .clone()
                    .try_into_request(self.fork_context.current_fork(), &self.chain.spec)?,
            ),
            app_request_id: AppRequestId::Sync(SyncRequestId::DataColumnsByRoot(id)),
        })?;

        debug!(
            method = "DataColumnsByRoot",
            block_root = ?request.block_root,
            indices = ?request.indices,
            peer = %peer_id,
            %id,
            "Sync RPC request sent"
        );

        self.data_columns_by_root_requests.insert(
            id,
            peer_id,
            expect_max_responses,
            DataColumnsByRootRequestItems::new(request),
        );

        Ok(LookupRequestResult::RequestSent(id))
    }

    /// Request to fetch all needed custody columns of a specific block. This function may not send
    /// any request to the network if no columns have to be fetched based on the import state of the
    /// node. A custody request is a "super request" that may trigger 0 or more `data_columns_by_root`
    /// requests.
    pub fn custody_lookup_request(
        &mut self,
        lookup_id: SingleLookupId,
        block_root: Hash256,
        lookup_peers: Arc<RwLock<HashSet<PeerId>>>,
    ) -> Result<LookupRequestResult, RpcRequestSendError> {
        let span = span!(
            Level::INFO,
            "SyncNetworkContext",
            service = "network_context"
        );
        let _enter = span.enter();

        let custody_indexes_imported = self
            .chain
            .data_availability_checker
            .cached_data_column_indexes(&block_root)
            .unwrap_or_default();

        // Include only the blob indexes not yet imported (received through gossip)
        let custody_indexes_to_fetch = self
            .network_globals()
            .sampling_columns
            .clone()
            .into_iter()
            .filter(|index| !custody_indexes_imported.contains(index))
            .collect::<Vec<_>>();

        if custody_indexes_to_fetch.is_empty() {
            // No indexes required, do not issue any request
            return Ok(LookupRequestResult::NoRequestNeeded("no indices to fetch"));
        }

        let id = SingleLookupReqId {
            lookup_id,
            req_id: self.next_id(),
        };

        debug!(
            ?block_root,
            indices = ?custody_indexes_to_fetch,
            %id,
            "Starting custody columns request"
        );

        let requester = CustodyRequester(id);
        let mut request = ActiveCustodyByRootRequest::new(
            block_root,
            CustodyId { requester },
            &custody_indexes_to_fetch,
            lookup_peers,
        );

        // Note that you can only send, but not handle a response here
        match request.continue_requests(self) {
            Ok(_) => {
                // Ignoring the result of `continue_requests` is okay. A request that has just been
                // created cannot return data immediately, it must send some request to the network
                // first. And there must exist some request, `custody_indexes_to_fetch` is not empty.
                self.custody_by_root_requests.insert(requester, request);
                Ok(LookupRequestResult::RequestSent(id.req_id))
            }
            Err(e) => Err(e.into()),
        }
    }

    fn send_blocks_by_range_request(
        &mut self,
        peer_id: PeerId,
        request: BlocksByRangeRequest,
        parent_request_id: ComponentsByRangeRequestId,
    ) -> Result<BlocksByRangeRequestId, RpcRequestSendError> {
        let id = BlocksByRangeRequestId {
            id: self.next_id(),
            parent_request_id,
        };
        self.network_send
            .send(NetworkMessage::SendRequest {
                peer_id,
                request: RequestType::BlocksByRange(request.clone().into()),
                app_request_id: AppRequestId::Sync(SyncRequestId::BlocksByRange(id)),
            })
            .map_err(|_| RpcRequestSendError::InternalError("network send error".to_owned()))?;

        debug!(
            method = "BlocksByRange",
            slots = request.count(),
            epoch = %Slot::new(*request.start_slot()).epoch(T::EthSpec::slots_per_epoch()),
            peer = %peer_id,
            %id,
            "Sync RPC request sent"
        );

        self.blocks_by_range_requests.insert(
            id,
            peer_id,
            // false = do not enforce max_requests are returned for *_by_range methods. We don't
            // know if there are missed blocks.
            false,
            BlocksByRangeRequestItems::new(request),
        );
        Ok(id)
    }

    fn send_blobs_by_range_request(
        &mut self,
        peer_id: PeerId,
        request: BlobsByRangeRequest,
        parent_request_id: ComponentsByRangeRequestId,
    ) -> Result<BlobsByRangeRequestId, RpcRequestSendError> {
        let id = BlobsByRangeRequestId {
            id: self.next_id(),
            parent_request_id,
        };
        let request_epoch = Slot::new(request.start_slot).epoch(T::EthSpec::slots_per_epoch());

        // Create the blob request based on the blocks request.
        self.network_send
            .send(NetworkMessage::SendRequest {
                peer_id,
                request: RequestType::BlobsByRange(request.clone()),
                app_request_id: AppRequestId::Sync(SyncRequestId::BlobsByRange(id)),
            })
            .map_err(|_| RpcRequestSendError::InternalError("network send error".to_owned()))?;

        debug!(
            method = "BlobsByRange",
            slots = request.count,
            epoch = %request_epoch,
            peer = %peer_id,
            %id,
            "Sync RPC request sent"
        );

        let max_blobs_per_block = self.chain.spec.max_blobs_per_block(request_epoch);
        self.blobs_by_range_requests.insert(
            id,
            peer_id,
            // false = do not enforce max_requests are returned for *_by_range methods. We don't
            // know if there are missed blocks.
            false,
            BlobsByRangeRequestItems::new(request, max_blobs_per_block),
        );
        Ok(id)
    }

    fn send_data_columns_by_range_request(
        &mut self,
        peer_id: PeerId,
        request: DataColumnsByRangeRequest,
        parent_request_id: CustodyByRangeRequestId,
    ) -> Result<DataColumnsByRangeRequestId, &'static str> {
        let id = DataColumnsByRangeRequestId {
            id: self.next_id(),
            parent_request_id,
        };

        self.send_network_msg(NetworkMessage::SendRequest {
            peer_id,
            request: RequestType::DataColumnsByRange(request.clone()),
            app_request_id: AppRequestId::Sync(SyncRequestId::DataColumnsByRange(id)),
        })
        .map_err(|_| "network send error")?;

        debug!(
            method = "DataColumnsByRange",
            slots = request.count,
            epoch = %Slot::new(request.start_slot).epoch(T::EthSpec::slots_per_epoch()),
            columns = ?request.columns,
            peer = %peer_id,
            %id,
            "Sync RPC request sent"
        );

        self.data_columns_by_range_requests.insert(
            id,
            peer_id,
            // false = do not enforce max_requests are returned for *_by_range methods. We don't
            // know if there are missed blocks.
            false,
            DataColumnsByRangeRequestItems::new(request),
        );
        Ok(id)
    }

    /// Request to fetch all needed custody columns of a range of slot. This function may not send
    /// any request to the network if no columns have to be fetched based on the import state of the
    /// node. A custody request is a "super request" that may trigger 0 or more `data_columns_by_range`
    /// requests.
    pub fn send_custody_by_range_request(
        &mut self,
        parent_id: ComponentsByRangeRequestId,
        blocks_with_data: Vec<SignedBeaconBlockHeader>,
        epoch: Epoch,
        column_indices: Vec<ColumnIndex>,
        lookup_peers: Arc<RwLock<HashSet<PeerId>>>,
    ) -> Result<CustodyByRangeRequestId, RpcRequestSendError> {
        let id = CustodyByRangeRequestId {
            id: self.next_id(),
            parent_request_id: parent_id,
        };

        debug!(
            indices = ?column_indices,
            %id,
            "Starting custody columns by range request"
        );

        let mut request = ActiveCustodyByRangeRequest::new(
            id,
            epoch,
            blocks_with_data,
            &column_indices,
            lookup_peers,
        );

        // Note that you can only send, but not handle a response here
        match request.continue_requests(self) {
            Ok(_) => {
                // Ignoring the result of `continue_requests` is okay. A request that has just been
                // created cannot return data immediately, it must send some request to the network
                // first. And there must exist some request, `custody_indexes_to_fetch` is not empty.
                self.custody_by_range_requests.insert(id, request);
                Ok(id)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn is_execution_engine_online(&self) -> bool {
        self.execution_engine_state == EngineState::Online
    }

    pub fn update_execution_engine_state(&mut self, engine_state: EngineState) {
        let span = span!(
            Level::INFO,
            "SyncNetworkContext",
            service = "network_context"
        );
        let _enter = span.enter();

        debug!(past_state = ?self.execution_engine_state, new_state = ?engine_state, "Sync's view on execution engine state updated");
        self.execution_engine_state = engine_state;
    }

    /// Terminates the connection with the peer and bans them.
    pub fn goodbye_peer(&mut self, peer_id: PeerId, reason: GoodbyeReason) {
        let span = span!(
            Level::INFO,
            "SyncNetworkContext",
            service = "network_context"
        );
        let _enter = span.enter();

        self.network_send
            .send(NetworkMessage::GoodbyePeer {
                peer_id,
                reason,
                source: ReportSource::SyncService,
            })
            .unwrap_or_else(|_| {
                warn!("Could not report peer: channel failed");
            });
    }

    /// Reports to the scoring algorithm the behaviour of a peer.
    pub fn report_peer(&self, peer_id: PeerId, action: PeerAction, msg: &'static str) {
        let span = span!(
            Level::INFO,
            "SyncNetworkContext",
            service = "network_context"
        );
        let _enter = span.enter();

        debug!(%peer_id, %action, %msg, client = %self.client_type(&peer_id), "Sync reporting peer");
        self.network_send
            .send(NetworkMessage::ReportPeer {
                peer_id,
                action,
                source: ReportSource::SyncService,
                msg,
            })
            .unwrap_or_else(|e| {
                warn!(error = %e, "Could not report peer: channel failed");
            });
    }

    /// Subscribes to core topics.
    pub fn subscribe_core_topics(&self) {
        let span = span!(
            Level::INFO,
            "SyncNetworkContext",
            service = "network_context"
        );
        let _enter = span.enter();

        self.network_send
            .send(NetworkMessage::SubscribeCoreTopics)
            .unwrap_or_else(|e| {
                warn!(error = %e, "Could not subscribe to core topics.");
            });
    }

    /// Sends an arbitrary network message.
    fn send_network_msg(&self, msg: NetworkMessage<T::EthSpec>) -> Result<(), &'static str> {
        let span = span!(
            Level::INFO,
            "SyncNetworkContext",
            service = "network_context"
        );
        let _enter = span.enter();

        self.network_send.send(msg).map_err(|_| {
            debug!("Could not send message to the network service");
            "Network channel send Failed"
        })
    }

    pub fn beacon_processor_if_enabled(&self) -> Option<&Arc<NetworkBeaconProcessor<T>>> {
        self.is_execution_engine_online()
            .then_some(&self.network_beacon_processor)
    }

    pub fn beacon_processor(&self) -> &Arc<NetworkBeaconProcessor<T>> {
        &self.network_beacon_processor
    }

    pub fn next_id(&mut self) -> Id {
        let id = self.request_id;
        self.request_id += 1;
        id
    }

    /// Attempt to make progress on all custody_by_root requests. Some request may be stale waiting
    /// for custody peers. Returns a Vec of results as zero or more requests may fail in this
    /// attempt.
    pub fn continue_custody_by_root_requests(
        &mut self,
    ) -> Vec<(CustodyRequester, CustodyRequestResult<T::EthSpec>)> {
        let ids = self
            .custody_by_root_requests
            .keys()
            .copied()
            .collect::<Vec<_>>();

        // Need to collect ids and results in separate steps to re-borrow self.
        ids.into_iter()
            .filter_map(|id| {
                let mut request = self
                    .custody_by_root_requests
                    .remove(&id)
                    .expect("key of hashmap");
                let result = request
                    .continue_requests(self)
                    .map_err(Into::<RpcResponseError>::into)
                    .transpose();
                self.handle_custody_by_root_result(id, request, result)
                    .map(|result| (id, result))
            })
            .collect()
    }

    /// Attempt to make progress on all custody_by_range requests. Some request may be stale waiting
    /// for custody peers. Returns a Vec of results as zero or more requests may fail in this
    /// attempt.
    pub fn continue_custody_by_range_requests(
        &mut self,
    ) -> Vec<(CustodyByRangeRequestId, CustodyRequestResult<T::EthSpec>)> {
        let ids = self
            .custody_by_range_requests
            .keys()
            .copied()
            .collect::<Vec<_>>();

        // Need to collect ids and results in separate steps to re-borrow self.
        ids.into_iter()
            .filter_map(|id| {
                let mut request = self
                    .custody_by_range_requests
                    .remove(&id)
                    .expect("key of hashmap");
                let result = request
                    .continue_requests(self)
                    .map_err(Into::<RpcResponseError>::into)
                    .transpose();
                self.handle_custody_by_range_result(id, request, result)
                    .map(|result| (id, result))
            })
            .collect()
    }

    // Request handlers

    /// Processes a single `RpcEvent` blocks_by_root RPC request.
    /// Same logic as [`on_blocks_by_range_response`] but it converts a `Vec<Block>` into a `Block`
    pub(crate) fn on_single_block_response(
        &mut self,
        id: SingleLookupReqId,
        peer_id: PeerId,
        rpc_event: RpcEvent<Arc<SignedBeaconBlock<T::EthSpec>>>,
    ) -> Option<RpcResponseResult<Arc<SignedBeaconBlock<T::EthSpec>>>> {
        let resp = self.blocks_by_root_requests.on_response(id, rpc_event);
        let resp = resp.map(|res| {
            res.and_then(|(mut blocks, seen_timestamp)| {
                // Enforce that exactly one chunk = one block is returned. ReqResp behavior limits the
                // response count to at most 1.
                match blocks.pop() {
                    Some(block) => Ok((block, seen_timestamp)),
                    // Should never happen, `blocks_by_root_requests` enforces that we receive at least
                    // 1 chunk.
                    None => Err(LookupVerifyError::NotEnoughResponsesReturned { actual: 0 }.into()),
                }
            })
        });
        self.on_rpc_response_result(id, "BlocksByRoot", resp, peer_id, |_| 1)
    }

    /// Processes a single `RpcEvent` blobs_by_root RPC request.
    /// Same logic as [`on_blocks_by_range_response`]
    pub(crate) fn on_single_blob_response(
        &mut self,
        id: SingleLookupReqId,
        peer_id: PeerId,
        rpc_event: RpcEvent<Arc<BlobSidecar<T::EthSpec>>>,
    ) -> Option<RpcResponseResult<FixedBlobSidecarList<T::EthSpec>>> {
        let resp = self.blobs_by_root_requests.on_response(id, rpc_event);
        let resp = resp.map(|res| {
            res.and_then(|(blobs, seen_timestamp)| {
                if let Some(max_len) = blobs
                    .first()
                    .map(|blob| self.chain.spec.max_blobs_per_block(blob.epoch()) as usize)
                {
                    match to_fixed_blob_sidecar_list(blobs, max_len) {
                        Ok(blobs) => Ok((blobs, seen_timestamp)),
                        Err(e) => Err(e.into()),
                    }
                } else {
                    Err(RpcResponseError::VerifyError(
                        LookupVerifyError::InternalError(
                            "Requested blobs for a block that has no blobs".to_string(),
                        ),
                    ))
                }
            })
        });
        self.on_rpc_response_result(id, "BlobsByRoot", resp, peer_id, |_| 1)
    }

    /// Processes a single `RpcEvent` for a data_columns_by_root RPC request.
    /// Same logic as [`on_blocks_by_range_response`]
    #[allow(clippy::type_complexity)]
    pub(crate) fn on_data_columns_by_root_response(
        &mut self,
        id: DataColumnsByRootRequestId,
        peer_id: PeerId,
        rpc_event: RpcEvent<Arc<DataColumnSidecar<T::EthSpec>>>,
    ) -> Option<RpcResponseResult<Vec<Arc<DataColumnSidecar<T::EthSpec>>>>> {
        let resp = self
            .data_columns_by_root_requests
            .on_response(id, rpc_event);
        self.on_rpc_response_result(id, "DataColumnsByRoot", resp, peer_id, |_| 1)
    }

    /// Processes a single `RpcEvent` for a blocks_by_range RPC request.
    /// - If the event completes the request, it returns `Some(Ok)` with a vec of blocks
    /// - If the event is an error it fails the request and returns `Some(Err)`
    /// - else it appends the response chunk to the active request state and returns `None`
    #[allow(clippy::type_complexity)]
    pub(crate) fn on_blocks_by_range_response(
        &mut self,
        id: BlocksByRangeRequestId,
        peer_id: PeerId,
        rpc_event: RpcEvent<Arc<SignedBeaconBlock<T::EthSpec>>>,
    ) -> Option<RpcResponseResult<Vec<Arc<SignedBeaconBlock<T::EthSpec>>>>> {
        let resp = self.blocks_by_range_requests.on_response(id, rpc_event);
        self.on_rpc_response_result(id, "BlocksByRange", resp, peer_id, |b| b.len())
    }

    /// Processes a single `RpcEvent` for a blobs_by_range RPC request.
    /// Same logic as [`on_blocks_by_range_response`]
    #[allow(clippy::type_complexity)]
    pub(crate) fn on_blobs_by_range_response(
        &mut self,
        id: BlobsByRangeRequestId,
        peer_id: PeerId,
        rpc_event: RpcEvent<Arc<BlobSidecar<T::EthSpec>>>,
    ) -> Option<RpcResponseResult<Vec<Arc<BlobSidecar<T::EthSpec>>>>> {
        let resp = self.blobs_by_range_requests.on_response(id, rpc_event);
        self.on_rpc_response_result(id, "BlobsByRangeRequest", resp, peer_id, |b| b.len())
    }

    /// Processes a single `RpcEvent` for a data_columns_by_range RPC request.
    /// Same logic as [`on_blocks_by_range_response`]
    #[allow(clippy::type_complexity)]
    pub(crate) fn on_data_columns_by_range_response(
        &mut self,
        id: DataColumnsByRangeRequestId,
        peer_id: PeerId,
        rpc_event: RpcEvent<Arc<DataColumnSidecar<T::EthSpec>>>,
    ) -> Option<RpcResponseResult<DataColumnSidecarList<T::EthSpec>>> {
        let resp = self
            .data_columns_by_range_requests
            .on_response(id, rpc_event);
        self.on_rpc_response_result(id, "DataColumnsByRange", resp, peer_id, |d| d.len())
    }

    /// Common logic for `on_*_response` handlers. Ensures we have consistent logging and metrics
    /// and peer reporting for all request types.
    fn on_rpc_response_result<I: std::fmt::Display, R, F: FnOnce(&R) -> usize>(
        &mut self,
        id: I,
        method: &'static str,
        resp: Option<RpcResponseResult<R>>,
        peer_id: PeerId,
        get_count: F,
    ) -> Option<RpcResponseResult<R>> {
        match &resp {
            None => {}
            Some(Ok((v, _))) => {
                debug!(
                    %id,
                    method,
                    count = get_count(v),
                    "Sync RPC request completed"
                );
            }
            Some(Err(e)) => {
                debug!(
                    %id,
                    method,
                    error = ?e,
                    "Sync RPC request error"
                );
            }
        }
        if let Some(Err(RpcResponseError::VerifyError(e))) = &resp {
            self.report_peer(peer_id, PeerAction::LowToleranceError, e.into());
        }
        resp
    }

    /// Insert a downloaded column into an active custody request. Then make progress on the
    /// entire request.
    ///
    /// ### Returns
    ///
    /// - `Some`: Request completed, won't make more progress. Expect requester to act on the result.
    /// - `None`: Request still active, requester should do no action
    #[allow(clippy::type_complexity)]
    pub fn on_custody_by_root_response(
        &mut self,
        id: CustodyId,
        req_id: DataColumnsByRootRequestId,
        peer_id: PeerId,
        resp: RpcResponseResult<Vec<Arc<DataColumnSidecar<T::EthSpec>>>>,
    ) -> Option<CustodyRequestResult<T::EthSpec>> {
        let span = span!(
            Level::INFO,
            "SyncNetworkContext",
            service = "network_context"
        );
        let _enter = span.enter();

        // Note: need to remove the request to borrow self again below. Otherwise we can't
        // do nested requests
        let Some(mut request) = self.custody_by_root_requests.remove(&id.requester) else {
            metrics::inc_counter_vec(
                &metrics::SYNC_UNKNOWN_NETWORK_REQUESTS,
                &["custody_by_root"],
            );
            return None;
        };

        let result = request
            .on_data_column_downloaded(peer_id, req_id, resp, self)
            .map_err(Into::<RpcResponseError>::into)
            .transpose();

        self.handle_custody_by_root_result(id.requester, request, result)
    }

    fn handle_custody_by_root_result(
        &mut self,
        id: CustodyRequester,
        request: ActiveCustodyByRootRequest<T>,
        result: Option<CustodyRequestResult<T::EthSpec>>,
    ) -> Option<CustodyRequestResult<T::EthSpec>> {
        let span = span!(
            Level::INFO,
            "SyncNetworkContext",
            service = "network_context"
        );
        let _enter = span.enter();

        match &result {
            Some(Ok((columns, peer_group, _))) => {
                debug!(%id, count = columns.len(), peers = ?peer_group, "Custody by root request success, removing")
            }
            Some(Err(e)) => {
                debug!(%id, error = ?e, "Custody by root request failure, removing")
            }
            None => {
                self.custody_by_root_requests.insert(id, request);
            }
        }
        result
    }

    /// Insert a downloaded column into an active custody request. Then make progress on the
    /// entire request.
    ///
    /// ### Returns
    ///
    /// - `Some`: Request completed, won't make more progress. Expect requester to act on the result.
    /// - `None`: Request still active, requester should do no action
    #[allow(clippy::type_complexity)]
    pub fn on_custody_by_range_response(
        &mut self,
        id: CustodyByRangeRequestId,
        req_id: DataColumnsByRangeRequestId,
        peer_id: PeerId,
        resp: RpcResponseResult<Vec<Arc<DataColumnSidecar<T::EthSpec>>>>,
    ) -> Option<CustodyRequestResult<T::EthSpec>> {
        // Note: need to remove the request to borrow self again below. Otherwise we can't
        // do nested requests
        let Some(mut request) = self.custody_by_range_requests.remove(&id) else {
            metrics::inc_counter_vec(
                &metrics::SYNC_UNKNOWN_NETWORK_REQUESTS,
                &["custody_by_range"],
            );
            return None;
        };

        let result = request
            .on_data_column_downloaded(peer_id, req_id, resp, self)
            .map_err(Into::<RpcResponseError>::into)
            .transpose();

        self.handle_custody_by_range_result(id, request, result)
    }

    fn handle_custody_by_range_result(
        &mut self,
        id: CustodyByRangeRequestId,
        request: ActiveCustodyByRangeRequest<T>,
        result: Option<CustodyRequestResult<T::EthSpec>>,
    ) -> Option<CustodyRequestResult<T::EthSpec>> {
        match &result {
            Some(Ok((columns, _peer_group, _))) => {
                // Don't log the peer_group here, it's very long (could be up to 128 peers). If you
                // want to trace which peer sent the column at index X, search for the log:
                // `Sync RPC request sent method="DataColumnsByRange" ...`
                debug!(%id, count = columns.len(), "Custody by range request success, removing")
            }
            Some(Err(e)) => {
                debug!(%id, error = ?e, "Custody by range request failure, removing")
            }
            None => {
                self.custody_by_range_requests.insert(id, request);
            }
        }
        result
    }

    /// Processes the result of an `*_by_range` RPC request issued by a
    /// block_components_by_range_request.
    ///
    /// - If the result completes the request, it returns `Some(Ok)` with a vec of coupled RpcBlocks
    /// - If the result fails the request, it returns `Some(Err)`. Note that a failed request may
    ///   not fail the block_components_by_range_request as it implements retries.
    /// - else it appends the result to the active request state and returns `None`
    #[allow(clippy::type_complexity)]
    pub fn on_block_components_by_range_response(
        &mut self,
        id: ComponentsByRangeRequestId,
        range_block_component: RangeBlockComponent<T::EthSpec>,
    ) -> Option<Result<(Vec<RpcBlock<T::EthSpec>>, BatchPeers), RpcResponseError>> {
        // Note: need to remove the request to borrow self again below. Otherwise we can't
        // do nested requests
        let Some(mut request) = self.block_components_by_range_requests.remove(&id) else {
            metrics::inc_counter_vec(
                &metrics::SYNC_UNKNOWN_NETWORK_REQUESTS,
                &["block_components_by_range"],
            );
            return None;
        };

        let result = match range_block_component {
            RangeBlockComponent::Block(req_id, resp, peer_id) => resp.and_then(|(blocks, _)| {
                request
                    .on_blocks_by_range_result(req_id, blocks, peer_id, self)
                    .map_err(Into::<RpcResponseError>::into)
            }),
            RangeBlockComponent::Blob(req_id, resp, peer_id) => resp.and_then(|(blobs, _)| {
                request
                    .on_blobs_by_range_result(req_id, blobs, peer_id, self)
                    .map_err(Into::<RpcResponseError>::into)
            }),
            RangeBlockComponent::CustodyColumns(req_id, resp, peers) => {
                resp.and_then(|(custody_columns, _)| {
                    request
                        .on_custody_by_range_result(req_id, custody_columns, peers, self)
                        .map_err(Into::<RpcResponseError>::into)
                })
            }
        }
        // Convert a result from internal format of `ActiveCustodyRequest` (error first to use ?) to
        // an Option first to use in an `if let Some() { act on result }` block.
        .transpose();

        match result.as_ref() {
            Some(Ok((blocks, peer_group))) => {
                let blocks_with_data = blocks
                    .iter()
                    .filter(|block| block.as_block().has_data())
                    .count();
                // Don't log the peer_group here, it's very long (could be up to 128 peers). If you
                // want to trace which peer sent the column at index X, search for the log:
                // `Sync RPC request sent method="DataColumnsByRange" ...`
                debug!(
                    %id,
                    blocks = blocks.len(),
                    blocks_with_data,
                    block_peer = ?peer_group.block(),
                    "Block components by range request success, removing"
                )
            }
            Some(Err(e)) => {
                debug!(%id, error = ?e, "Block components by range request failure, removing" )
            }
            None => {
                self.block_components_by_range_requests.insert(id, request);
            }
        }
        result
    }

    pub fn send_block_for_processing(
        &self,
        id: Id,
        block_root: Hash256,
        block: Arc<SignedBeaconBlock<T::EthSpec>>,
        seen_timestamp: Duration,
    ) -> Result<(), SendErrorProcessor> {
        let span = span!(
            Level::INFO,
            "SyncNetworkContext",
            service = "network_context"
        );
        let _enter = span.enter();

        let beacon_processor = self
            .beacon_processor_if_enabled()
            .ok_or(SendErrorProcessor::ProcessorNotAvailable)?;

        let block = RpcBlock::new_without_blobs(
            Some(block_root),
            block,
            self.network_globals().custody_columns_count() as usize,
        );

        debug!(block = ?block_root, id, "Sending block for processing");
        // Lookup sync event safety: If `beacon_processor.send_rpc_beacon_block` returns Ok() sync
        // must receive a single `SyncMessage::BlockComponentProcessed` with this process type
        beacon_processor
            .send_rpc_beacon_block(
                block_root,
                block,
                seen_timestamp,
                BlockProcessType::SingleBlock { id },
            )
            .map_err(|e| {
                error!(
                    error = ?e,
                    "Failed to send sync block to processor"
                );
                SendErrorProcessor::SendError
            })
    }

    pub fn send_blobs_for_processing(
        &self,
        id: Id,
        block_root: Hash256,
        blobs: FixedBlobSidecarList<T::EthSpec>,
        seen_timestamp: Duration,
    ) -> Result<(), SendErrorProcessor> {
        let span = span!(
            Level::INFO,
            "SyncNetworkContext",
            service = "network_context"
        );
        let _enter = span.enter();

        let beacon_processor = self
            .beacon_processor_if_enabled()
            .ok_or(SendErrorProcessor::ProcessorNotAvailable)?;

        debug!(?block_root, %id, "Sending blobs for processing");
        // Lookup sync event safety: If `beacon_processor.send_rpc_blobs` returns Ok() sync
        // must receive a single `SyncMessage::BlockComponentProcessed` event with this process type
        beacon_processor
            .send_rpc_blobs(
                block_root,
                blobs,
                seen_timestamp,
                BlockProcessType::SingleBlob { id },
            )
            .map_err(|e| {
                error!(
                    error = ?e,
                    "Failed to send sync blobs to processor"
                );
                SendErrorProcessor::SendError
            })
    }

    pub fn send_custody_columns_for_processing(
        &self,
        _id: Id,
        block_root: Hash256,
        custody_columns: DataColumnSidecarList<T::EthSpec>,
        seen_timestamp: Duration,
        process_type: BlockProcessType,
    ) -> Result<(), SendErrorProcessor> {
        let span = span!(
            Level::INFO,
            "SyncNetworkContext",
            service = "network_context"
        );
        let _enter = span.enter();

        let beacon_processor = self
            .beacon_processor_if_enabled()
            .ok_or(SendErrorProcessor::ProcessorNotAvailable)?;

        debug!(
            ?block_root,
            ?process_type,
            "Sending custody columns for processing"
        );

        beacon_processor
            .send_rpc_custody_columns(block_root, custody_columns, seen_timestamp, process_type)
            .map_err(|e| {
                error!(
                    error = ?e,
                    "Failed to send sync custody columns to processor"
                );
                SendErrorProcessor::SendError
            })
    }

    pub(crate) fn register_metrics(&self) {
        for (id, count) in [
            ("blocks_by_root", self.blocks_by_root_requests.len()),
            ("blobs_by_root", self.blobs_by_root_requests.len()),
            (
                "data_columns_by_root",
                self.data_columns_by_root_requests.len(),
            ),
            ("blocks_by_range", self.blocks_by_range_requests.len()),
            ("blobs_by_range", self.blobs_by_range_requests.len()),
            (
                "data_columns_by_range",
                self.data_columns_by_range_requests.len(),
            ),
            ("custody_by_root", self.custody_by_root_requests.len()),
            (
                "block_components_by_range",
                self.block_components_by_range_requests.len(),
            ),
        ] {
            metrics::set_gauge_vec(&metrics::SYNC_ACTIVE_NETWORK_REQUESTS, &[id], count as i64);
        }
    }
}

fn to_fixed_blob_sidecar_list<E: EthSpec>(
    blobs: Vec<Arc<BlobSidecar<E>>>,
    max_len: usize,
) -> Result<FixedBlobSidecarList<E>, LookupVerifyError> {
    let mut fixed_list = FixedBlobSidecarList::new(vec![None; max_len]);
    for blob in blobs.into_iter() {
        let index = blob.index as usize;
        *fixed_list
            .get_mut(index)
            .ok_or(LookupVerifyError::UnrequestedIndex(index as u64))? = Some(blob)
    }
    Ok(fixed_list)
}
