use crate::sync::network_context::{
    PeerGroup, RpcRequestSendError, RpcResponseError, SyncNetworkContext,
};
use crate::sync::range_sync::BatchPeers;
use beacon_chain::block_verification_types::RpcBlock;
use beacon_chain::data_column_verification::CustodyDataColumn;
use beacon_chain::{get_block_root, BeaconChainTypes};
use lighthouse_network::rpc::methods::{BlobsByRangeRequest, BlocksByRangeRequest};
use lighthouse_network::service::api_types::{
    BlobsByRangeRequestId, BlocksByRangeRequestId, ComponentsByRangeRequestId,
    CustodyByRangeRequestId,
};
use lighthouse_network::PeerId;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use types::{
    BlobSidecar, ChainSpec, ColumnIndex, DataColumnSidecar, EthSpec, Hash256, RuntimeVariableList,
    SignedBeaconBlock, Slot,
};

pub struct BlockComponentsByRangeRequest<T: BeaconChainTypes> {
    id: ComponentsByRangeRequestId,
    peers: Arc<RwLock<HashSet<PeerId>>>,
    request: BlocksByRangeRequest,
    state: State<T::EthSpec>,
}

enum State<E: EthSpec> {
    Base {
        blocks_by_range_request:
            ByRangeRequest<BlocksByRangeRequestId, Vec<Arc<SignedBeaconBlock<E>>>>,
    },
    // Two single concurrent requests for block + blobs
    DenebEnabled {
        blocks_by_range_request:
            ByRangeRequest<BlocksByRangeRequestId, Vec<Arc<SignedBeaconBlock<E>>>>,
        blobs_by_range_request: ByRangeRequest<BlobsByRangeRequestId, Vec<Arc<BlobSidecar<E>>>>,
    },
    // Request blocks first, then columns
    FuluEnabled(FuluEnabledState<E>),
}

enum FuluEnabledState<E: EthSpec> {
    BlockRequest {
        blocks_by_range_request:
            ByRangeRequest<BlocksByRangeRequestId, Vec<Arc<SignedBeaconBlock<E>>>>,
    },
    CustodyRequest {
        blocks: Vec<Arc<SignedBeaconBlock<E>>>,
        block_peer: PeerId,
        custody_by_range_request:
            ByRangeRequest<CustodyByRangeRequestId, Vec<Arc<DataColumnSidecar<E>>>, PeerGroup>,
    },
}

enum ByRangeRequest<I: PartialEq + std::fmt::Display, T, P = PeerId> {
    /// Active(RequestIndex)
    Active(I),
    /// Complete(DownloadedData, Peers)
    Complete(T, P),
}

pub type BlockComponentsByRangeRequestResult<E> =
    Result<Option<(Vec<RpcBlock<E>>, BatchPeers)>, Error>;

pub enum Error {
    InternalError(String),
}

impl From<Error> for RpcResponseError {
    fn from(e: Error) -> Self {
        match e {
            Error::InternalError(e) => RpcResponseError::InternalError(e),
        }
    }
}

impl From<Error> for RpcRequestSendError {
    fn from(e: Error) -> Self {
        match e {
            Error::InternalError(e) => RpcRequestSendError::InternalError(e),
        }
    }
}

/// FOR TESTING ONLY
#[cfg(test)]
#[derive(Debug)]
pub enum BlockComponentsByRangeRequestStep {
    BlocksRequest,
    CustodyRequest,
}

impl<T: BeaconChainTypes> BlockComponentsByRangeRequest<T> {
    pub fn new(
        id: ComponentsByRangeRequestId,
        request: BlocksByRangeRequest,
        peers: &HashSet<PeerId>,
        peers_to_deprioritize: &HashSet<PeerId>,
        total_requests_per_peer: &HashMap<PeerId, usize>,
        cx: &mut SyncNetworkContext<T>,
    ) -> Result<Self, RpcRequestSendError> {
        // Induces a compile time panic if this doesn't hold true.
        #[allow(clippy::assertions_on_constants)]
        const _: () = assert!(
            super::super::backfill_sync::BACKFILL_EPOCHS_PER_BATCH == 1
                && super::super::range_sync::EPOCHS_PER_BATCH == 1,
            "To deal with alignment with deneb boundaries, batches need to be of just one epoch"
        );
        // The assertion above ensures each batch is in one single epoch
        let batch_epoch = Slot::new(*request.start_slot()).epoch(T::EthSpec::slots_per_epoch());
        let batch_fork = cx.spec().fork_name_at_epoch(batch_epoch);

        // TODO(das): a change of behaviour here is that if the SyncingChain has a single peer we
        // will request all blocks for the first 5 epochs to that same single peer. Before we would
        // query only idle peers in the syncing chain.
        let Some(block_peer) = peers
            .iter()
            .map(|peer| {
                (
                    // If contains -> 1 (order after), not contains -> 0 (order first)
                    peers_to_deprioritize.contains(peer),
                    // TODO(das): Should we use active_request_count_by_peer?
                    // Prefer peers with less overall requests
                    // active_request_count_by_peer.get(peer).copied().unwrap_or(0),
                    // Prefer peers with less total cummulative requests, so we fetch data from a
                    // diverse set of peers
                    total_requests_per_peer.get(peer).copied().unwrap_or(0),
                    // Random factor to break ties, otherwise the PeerID breaks ties
                    rand::random::<u32>(),
                    peer,
                )
            })
            .min()
            .map(|(_, _, _, peer)| *peer)
        else {
            // When a peer disconnects and is removed from the SyncingChain peer set, if the set
            // reaches zero the SyncingChain is removed.
            // TODO(das): add test for this.
            return Err(RpcRequestSendError::InternalError(
                "A batch peer set should never be empty".to_string(),
            ));
        };

        let blocks_req_id = cx.send_blocks_by_range_request(block_peer, request.clone(), id)?;

        let state = if batch_fork.fulu_enabled() {
            State::FuluEnabled(FuluEnabledState::BlockRequest {
                blocks_by_range_request: ByRangeRequest::Active(blocks_req_id),
            })
        } else if batch_fork.deneb_enabled() {
            // TODO(deneb): is it okay to send blobs_by_range requests outside the DA window? I
            // would like the beacon processor / da_checker to be the one that decides if an
            // RpcBlock is valid or not with respect to containing blobs. Having sync not even
            // attempt a requests seems like an added limitation.
            let blobs_req_id = cx.send_blobs_by_range_request(
                block_peer,
                BlobsByRangeRequest {
                    start_slot: *request.start_slot(),
                    count: *request.count(),
                },
                id,
            )?;
            State::DenebEnabled {
                blocks_by_range_request: ByRangeRequest::Active(blocks_req_id),
                blobs_by_range_request: ByRangeRequest::Active(blobs_req_id),
            }
        } else {
            State::Base {
                blocks_by_range_request: ByRangeRequest::Active(blocks_req_id),
            }
        };

        Ok(Self {
            id,
            // TODO(das): share the rwlock with the range sync batch. Are peers added to the batch
            // after being created?
            peers: Arc::new(RwLock::new(peers.clone())),
            request,
            state,
        })
    }

    pub fn continue_requests(
        &mut self,
        cx: &mut SyncNetworkContext<T>,
    ) -> BlockComponentsByRangeRequestResult<T::EthSpec> {
        match &mut self.state {
            State::Base {
                blocks_by_range_request,
            } => {
                if let Some((blocks, block_peer)) = blocks_by_range_request.to_finished() {
                    // TODO(das): use the peer group
                    let peer_group = BatchPeers::new_from_block_peer(*block_peer);
                    let rpc_blocks = couple_blocks_base(
                        blocks.to_vec(),
                        cx.network_globals().sampling_columns.len(),
                    );
                    Ok(Some((rpc_blocks, peer_group)))
                } else {
                    // Wait for blocks_by_range requests to complete
                    Ok(None)
                }
            }
            State::DenebEnabled {
                blocks_by_range_request,
                blobs_by_range_request,
            } => {
                if let (Some((blocks, block_peer)), Some((blobs, _))) = (
                    blocks_by_range_request.to_finished(),
                    blobs_by_range_request.to_finished(),
                ) {
                    // We use the same block_peer for the blobs request
                    let peer_group = BatchPeers::new_from_block_peer(*block_peer);
                    let rpc_blocks =
                        couple_blocks_deneb(blocks.to_vec(), blobs.to_vec(), cx.spec())?;
                    Ok(Some((rpc_blocks, peer_group)))
                } else {
                    // Wait for blocks_by_range and blobs_by_range requests to complete
                    Ok(None)
                }
            }
            State::FuluEnabled(state) => match state {
                FuluEnabledState::BlockRequest {
                    blocks_by_range_request,
                } => {
                    if let Some((blocks, block_peer)) = blocks_by_range_request.to_finished() {
                        // TODO(das): use the peer group
                        let blocks_with_data = blocks
                            .iter()
                            .filter(|block| block.has_data())
                            .map(|block| block.signed_block_header())
                            .collect::<Vec<_>>();

                        if blocks_with_data.is_empty() {
                            let custody_column_indices = cx
                                .network_globals()
                                .sampling_columns
                                .clone()
                                .iter()
                                .copied()
                                .collect();

                            // Done, we got blocks and no columns needed
                            let peer_group = BatchPeers::new_from_block_peer(*block_peer);
                            let rpc_blocks = couple_blocks_fulu(
                                blocks.to_vec(),
                                vec![],
                                custody_column_indices,
                                cx.spec(),
                            )?;
                            Ok(Some((rpc_blocks, peer_group)))
                        } else {
                            let mut column_indices = cx
                                .network_globals()
                                .sampling_columns
                                .clone()
                                .iter()
                                .copied()
                                .collect::<Vec<_>>();
                            column_indices.sort_unstable();

                            let req_id = cx
                                .send_custody_by_range_request(
                                    self.id,
                                    blocks_with_data,
                                    Slot::new(*self.request.start_slot())
                                        .epoch(T::EthSpec::slots_per_epoch()),
                                    column_indices,
                                    self.peers.clone(),
                                )
                                .map_err(|e| match e {
                                    RpcRequestSendError::InternalError(e) => {
                                        Error::InternalError(e)
                                    }
                                })?;

                            *state = FuluEnabledState::CustodyRequest {
                                blocks: blocks.to_vec(),
                                block_peer: *block_peer,
                                custody_by_range_request: ByRangeRequest::Active(req_id),
                            };

                            // Wait for the new custody_by_range request to complete
                            Ok(None)
                        }
                    } else {
                        // Wait for the block request to complete
                        Ok(None)
                    }
                }
                FuluEnabledState::CustodyRequest {
                    blocks,
                    block_peer,
                    custody_by_range_request,
                } => {
                    if let Some((columns, column_peers)) = custody_by_range_request.to_finished() {
                        let custody_column_indices = cx
                            .network_globals()
                            .sampling_columns
                            .clone()
                            .iter()
                            .copied()
                            .collect();

                        let peer_group =
                            BatchPeers::new(*block_peer, column_peers.as_reversed_map());
                        let rpc_blocks = couple_blocks_fulu(
                            blocks.to_vec(),
                            columns.to_vec(),
                            custody_column_indices,
                            cx.spec(),
                        )?;
                        Ok(Some((rpc_blocks, peer_group)))
                    } else {
                        // Wait for the custody_by_range request to complete
                        Ok(None)
                    }
                }
            },
        }
    }

    pub fn on_blocks_by_range_result(
        &mut self,
        id: BlocksByRangeRequestId,
        data: Vec<Arc<SignedBeaconBlock<T::EthSpec>>>,
        peer_id: PeerId,
        cx: &mut SyncNetworkContext<T>,
    ) -> BlockComponentsByRangeRequestResult<T::EthSpec> {
        match &mut self.state {
            State::Base {
                blocks_by_range_request,
            }
            | State::DenebEnabled {
                blocks_by_range_request,
                ..
            }
            | State::FuluEnabled(FuluEnabledState::BlockRequest {
                blocks_by_range_request,
            }) => {
                blocks_by_range_request.finish(id, data, peer_id)?;
            }
            State::FuluEnabled(FuluEnabledState::CustodyRequest { .. }) => {
                return Err(Error::InternalError(
                    "Received blocks_by_range response expecting custody_by_range".to_string(),
                ))
            }
        }

        self.continue_requests(cx)
    }

    pub fn on_blobs_by_range_result(
        &mut self,
        id: BlobsByRangeRequestId,
        data: Vec<Arc<BlobSidecar<T::EthSpec>>>,
        peer_id: PeerId,
        cx: &mut SyncNetworkContext<T>,
    ) -> BlockComponentsByRangeRequestResult<T::EthSpec> {
        match &mut self.state {
            State::Base { .. } => {
                return Err(Error::InternalError(
                    "Received blobs_by_range response before Deneb".to_string(),
                ))
            }
            State::DenebEnabled {
                blobs_by_range_request,
                ..
            } => {
                blobs_by_range_request.finish(id, data, peer_id)?;
            }
            State::FuluEnabled(_) => {
                return Err(Error::InternalError(
                    "Received blobs_by_range response after PeerDAS".to_string(),
                ))
            }
        }

        self.continue_requests(cx)
    }

    pub fn on_custody_by_range_result(
        &mut self,
        id: CustodyByRangeRequestId,
        data: Vec<Arc<DataColumnSidecar<T::EthSpec>>>,
        peers: PeerGroup,
        cx: &mut SyncNetworkContext<T>,
    ) -> BlockComponentsByRangeRequestResult<T::EthSpec> {
        match &mut self.state {
            State::Base { .. } | State::DenebEnabled { .. } => {
                return Err(Error::InternalError(
                    "Received custody_by_range response before PeerDAS".to_string(),
                ))
            }
            State::FuluEnabled(state) => match state {
                FuluEnabledState::BlockRequest { .. } => {
                    return Err(Error::InternalError(
                        "Received custody_by_range expecting blocks_by_range".to_string(),
                    ));
                }
                FuluEnabledState::CustodyRequest {
                    custody_by_range_request,
                    ..
                } => {
                    custody_by_range_request.finish(id, data, peers)?;
                }
            },
        }

        self.continue_requests(cx)
    }

    #[cfg(test)]
    pub fn state_step(&self) -> BlockComponentsByRangeRequestStep {
        match &self.state {
            State::Base { .. } => BlockComponentsByRangeRequestStep::BlocksRequest,
            State::DenebEnabled { .. } => BlockComponentsByRangeRequestStep::BlocksRequest,
            State::FuluEnabled(state) => match state {
                FuluEnabledState::BlockRequest { .. } => {
                    BlockComponentsByRangeRequestStep::BlocksRequest
                }
                FuluEnabledState::CustodyRequest { .. } => {
                    BlockComponentsByRangeRequestStep::CustodyRequest
                }
            },
        }
    }
}

fn couple_blocks_base<E: EthSpec>(
    blocks: Vec<Arc<SignedBeaconBlock<E>>>,
    custody_columns_count: usize,
) -> Vec<RpcBlock<E>> {
    blocks
        .into_iter()
        .map(|block| RpcBlock::new_without_blobs(None, block, custody_columns_count))
        .collect()
}

fn couple_blocks_deneb<E: EthSpec>(
    blocks: Vec<Arc<SignedBeaconBlock<E>>>,
    blobs: Vec<Arc<BlobSidecar<E>>>,
    spec: &ChainSpec,
) -> Result<Vec<RpcBlock<E>>, Error> {
    let mut blobs_by_block = HashMap::<Hash256, Vec<Arc<BlobSidecar<E>>>>::new();
    for blob in blobs {
        let block_root = blob.block_root();
        blobs_by_block.entry(block_root).or_default().push(blob);
    }

    // Now collect all blobs that match to the block by block root. BlobsByRange request checks
    // the inclusion proof so we know that the commitment is the expected.
    //
    // BlobsByRange request handler ensures that we don't receive more blobs than possible.
    // If the peer serving the request sends us blobs that don't pair well we'll send to the
    // processor blocks without expected blobs, resulting in a downscoring event. A serving peer
    // could serve fake blobs for blocks that don't have data, but it would gain nothing by it
    // wasting theirs and our bandwidth 1:1. Therefore blobs that don't pair well are just ignored.
    //
    // RpcBlock::new ensures that the count of blobs is consistent with the block
    blocks
        .into_iter()
        .map(|block| {
            let block_root = get_block_root(&block);
            let max_blobs_per_block = spec.max_blobs_per_block(block.epoch()) as usize;
            let blobs = blobs_by_block.remove(&block_root).unwrap_or_default();
            // BlobsByRange request handler enforces that blobs are sorted by index
            let blobs = RuntimeVariableList::new(blobs, max_blobs_per_block).map_err(|_| {
                Error::InternalError("Blobs returned exceeds max length".to_string())
            })?;
            Ok(RpcBlock::new(Some(block_root), block, Some(blobs))
                .expect("TODO: don't do matching here"))
        })
        .collect::<Result<Vec<RpcBlock<E>>, Error>>()
}

fn couple_blocks_fulu<E: EthSpec>(
    blocks: Vec<Arc<SignedBeaconBlock<E>>>,
    data_columns: Vec<Arc<DataColumnSidecar<E>>>,
    custody_column_indices: Vec<ColumnIndex>,
    spec: &ChainSpec,
) -> Result<Vec<RpcBlock<E>>, Error> {
    // Group data columns by block_root and index
    let mut custody_columns_by_block = HashMap::<Hash256, Vec<CustodyDataColumn<E>>>::new();

    for column in data_columns {
        let block_root = column.block_root();

        if custody_column_indices.contains(&column.index) {
            custody_columns_by_block
                .entry(block_root)
                .or_default()
                // Safe to convert to `CustodyDataColumn`: we have asserted that the index of
                // this column is in the set of `expects_custody_columns` and with the expected
                // block root, so for the expected epoch of this batch.
                .push(CustodyDataColumn::from_asserted_custody(column));
        }
    }

    // Now iterate all blocks ensuring that the block roots of each block and data column match,
    blocks
        .into_iter()
        .map(|block| {
            let block_root = get_block_root(&block);
            let data_columns_with_block_root = custody_columns_by_block
                // Remove to only use columns once
                .remove(&block_root)
                .unwrap_or_default();

            // TODO(das): Change RpcBlock to holding a Vec of DataColumnSidecars so we don't need
            // the spec here.
            RpcBlock::new_with_custody_columns(
                Some(block_root),
                block,
                data_columns_with_block_root,
                custody_column_indices.clone(),
                spec,
            )
            .map_err(Error::InternalError)
        })
        .collect::<Result<Vec<_>, _>>()
}

impl<I: PartialEq + std::fmt::Display, T, P> ByRangeRequest<I, T, P> {
    fn finish(&mut self, id: I, data: T, peer_id: P) -> Result<(), Error> {
        match self {
            Self::Active(expected_id) => {
                if expected_id != &id {
                    return Err(Error::InternalError(format!(
                        "unexpected req_id expected {expected_id} got {id}"
                    )));
                }
                *self = Self::Complete(data, peer_id);
                Ok(())
            }
            Self::Complete(_, _) => Err(Error::InternalError(format!(
                "request already complete {id}"
            ))),
        }
    }

    fn to_finished(&self) -> Option<(&T, &P)> {
        match self {
            Self::Active(_) => None,
            Self::Complete(data, peer_id) => Some((data, peer_id)),
        }
    }
}
