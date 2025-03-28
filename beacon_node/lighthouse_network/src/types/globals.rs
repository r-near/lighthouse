//! A collection of variables that are accessible outside of the network thread itself.
use super::TopicConfig;
use crate::discovery::enr::Eth2Enr;
use crate::peer_manager::peerdb::PeerDB;
use crate::rpc::{MetaData, MetaDataV2, MetaDataV3};
use crate::types::{BackFillState, SyncState};
use crate::{Client, Enr, EnrExt, GossipTopic, Multiaddr, NetworkConfig, PeerId};
use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::Arc;
use types::data_column_custody_group::{
    compute_columns_for_custody_group, compute_subnets_from_custody_group, get_custody_groups,
};
use types::{ChainSpec, ColumnIndex, DataColumnSubnetId, EthSpec, Slot};

pub struct NetworkGlobals<E: EthSpec> {
    /// The current local ENR.
    local_enr: RwLock<Enr>,
    /// The local peer_id.
    pub peer_id: RwLock<PeerId>,
    /// Listening multiaddrs.
    pub listen_multiaddrs: RwLock<Vec<Multiaddr>>,
    /// The collection of known peers.
    pub peers: RwLock<PeerDB<E>>,
    /// The current gossipsub topic subscriptions.
    pub gossipsub_subscriptions: RwLock<HashSet<GossipTopic>>,
    /// The current sync status of the node.
    pub sync_state: RwLock<SyncState>,
    /// The current state of the backfill sync.
    pub backfill_state: RwLock<BackFillState>,
    /// The computed sampling subnets and columns is stored to avoid re-computing.
    all_sampling_subnets: Vec<DataColumnSubnetId>,
    all_sampling_columns: Vec<ColumnIndex>,
    /// Dynamic custody group count (CGC)
    cgc_updates: RwLock<CGCUpdates>,
    /// Network-related configuration. Immutable after initialization.
    pub config: Arc<NetworkConfig>,
    /// Ethereum chain configuration. Immutable after initialization.
    pub spec: Arc<ChainSpec>,
}

pub struct CGCUpdates {
    initial_value: u64,
    updates: Vec<(Slot, u64)>,
    // TODO(das): Track backfilled CGC
}

impl<E: EthSpec> NetworkGlobals<E> {
    pub fn new(
        enr: Enr,
        cgc_updates: CGCUpdates,
        trusted_peers: Vec<PeerId>,
        disable_peer_scoring: bool,
        config: Arc<NetworkConfig>,
        spec: Arc<ChainSpec>,
    ) -> Self {
        let node_id = enr.node_id().raw();

        // The below `expect` calls will panic on start up if the chain spec config values used
        // are invalid
        let custody_groups = get_custody_groups(node_id, spec.number_of_custody_groups, &spec)
            .expect("should compute node custody groups");

        let mut all_sampling_subnets = vec![];
        for custody_index in &custody_groups {
            let subnets = compute_subnets_from_custody_group(*custody_index, &spec)
                .expect("should compute custody subnets for node");
            all_sampling_subnets.extend(subnets);
        }

        let mut all_sampling_columns = vec![];
        for custody_index in &custody_groups {
            let columns = compute_columns_for_custody_group(*custody_index, &spec)
                .expect("should compute custody columns for node");
            all_sampling_columns.extend(columns);
        }

        NetworkGlobals {
            local_enr: RwLock::new(enr.clone()),
            peer_id: RwLock::new(enr.peer_id()),
            listen_multiaddrs: RwLock::new(Vec::new()),
            peers: RwLock::new(PeerDB::new(trusted_peers, disable_peer_scoring)),
            gossipsub_subscriptions: RwLock::new(HashSet::new()),
            sync_state: RwLock::new(SyncState::Stalled),
            backfill_state: RwLock::new(BackFillState::Paused),
            all_sampling_subnets,
            all_sampling_columns,
            cgc_updates: RwLock::new(cgc_updates),
            config,
            spec,
        }
    }

    /// Returns the local ENR from the underlying Discv5 behaviour that external peers may connect
    /// to.
    /// TODO: This contains duplicate metadata. Test who is consuming this method
    pub fn local_enr(&self) -> Enr {
        self.local_enr.read().clone()
    }

    pub fn set_enr(&self, enr: Enr) {
        *self.local_enr.write() = enr;
    }

    /// Returns the local libp2p PeerID.
    pub fn local_peer_id(&self) -> PeerId {
        *self.peer_id.read()
    }

    pub fn local_metadata(&self) -> MetaData<E> {
        let enr = self.local_enr();
        let attnets = enr
            .attestation_bitfield::<E>()
            .unwrap_or(Default::default());
        let syncnets = enr
            .sync_committee_bitfield::<E>()
            .unwrap_or(Default::default());

        if self.spec.is_peer_das_scheduled() {
            MetaData::V3(MetaDataV3 {
                seq_number: enr.seq(),
                attnets,
                syncnets,
                custody_group_count: self.public_custody_group_count(),
            })
        } else {
            MetaData::V2(MetaDataV2 {
                seq_number: enr.seq(),
                attnets,
                syncnets,
            })
        }
    }

    /// Returns the list of `Multiaddr` that the underlying libp2p instance is listening on.
    pub fn listen_multiaddrs(&self) -> Vec<Multiaddr> {
        self.listen_multiaddrs.read().clone()
    }

    /// Returns true if this node is configured as a PeerDAS supernode
    pub fn is_supernode(&self, slot: Slot) -> bool {
        self.custody_group_count(slot) == self.spec.number_of_custody_groups
    }

    pub fn sampling_subnets(&self, slot: Slot) -> &[DataColumnSubnetId] {
        let cgc = self.custody_group_count(slot) as usize;
        // Returns as many elements as possible, can't panic as it's upper bounded by len
        &self.all_sampling_subnets[..self.all_sampling_subnets.len().min(cgc)]
    }

    pub fn sampling_columns(&self, slot: Slot) -> &[ColumnIndex] {
        let cgc = self.custody_group_count(slot) as usize;
        // Returns as many elements as possible, can't panic as it's upper bounded by len
        &self.all_sampling_columns[..self.all_sampling_columns.len().min(cgc)]
    }

    fn public_custody_group_count(&self) -> u64 {
        todo!();
    }

    /// Returns the custody group count (CGC)
    fn custody_group_count(&self, slot: Slot) -> u64 {
        self.cgc_updates.read().at_slot(slot)
    }

    /// Returns the count of custody columns this node must sample for block import
    pub fn custody_columns_count(&self, slot: Slot) -> u64 {
        // This only panics if the chain spec contains invalid values
        self.spec
            .sampling_size(self.custody_group_count(slot))
            .expect("should compute node sampling size from valid chain spec")
    }

    /// Adds a new CGC value update
    pub fn add_cgc_update(&self, update: (Slot, u64)) {
        self.cgc_updates.write().add_latest_update(update);
    }

    /// Returns the number of libp2p connected peers.
    pub fn connected_peers(&self) -> usize {
        self.peers.read().connected_peer_ids().count()
    }

    /// Returns the number of libp2p connected peers with outbound-only connections.
    pub fn connected_outbound_only_peers(&self) -> usize {
        self.peers.read().connected_outbound_only_peers().count()
    }

    /// Returns the number of libp2p peers that are either connected or being dialed.
    pub fn connected_or_dialing_peers(&self) -> usize {
        self.peers.read().connected_or_dialing_peers().count()
    }

    /// Returns in the node is syncing.
    pub fn is_syncing(&self) -> bool {
        self.sync_state.read().is_syncing()
    }

    /// Returns the current sync state of the peer.
    pub fn sync_state(&self) -> SyncState {
        self.sync_state.read().clone()
    }

    /// Returns the current backfill state.
    pub fn backfill_state(&self) -> BackFillState {
        self.backfill_state.read().clone()
    }

    /// Returns a `Client` type if one is known for the `PeerId`.
    pub fn client(&self, peer_id: &PeerId) -> Client {
        self.peers
            .read()
            .peer_info(peer_id)
            .map(|info| info.client().clone())
            .unwrap_or_default()
    }

    /// Updates the syncing state of the node.
    ///
    /// The old state is returned
    pub fn set_sync_state(&self, new_state: SyncState) -> SyncState {
        std::mem::replace(&mut *self.sync_state.write(), new_state)
    }

    /// Returns a connected peer that:
    /// 1. is connected
    /// 2. assigned to custody the column based on it's `custody_subnet_count` from ENR or metadata
    /// 3. has a good score
    pub fn custody_peers_for_column(&self, column_index: ColumnIndex) -> Vec<PeerId> {
        self.peers
            .read()
            .good_custody_subnet_peer(DataColumnSubnetId::from_column_index(
                column_index,
                &self.spec,
            ))
            .cloned()
            .collect::<Vec<_>>()
    }

    /// Returns the TopicConfig to compute the set of Gossip topics for a given fork
    pub fn as_topic_config(&self, slot: Slot) -> TopicConfig {
        TopicConfig {
            enable_light_client_server: self.config.enable_light_client_server,
            subscribe_all_subnets: self.config.subscribe_all_subnets,
            subscribe_all_data_column_subnets: self.config.subscribe_all_data_column_subnets,
            sampling_subnets: self.sampling_subnets(slot),
        }
    }

    /// TESTING ONLY. Build a dummy NetworkGlobals instance.
    pub fn new_test_globals(
        trusted_peers: Vec<PeerId>,
        config: Arc<NetworkConfig>,
        spec: Arc<ChainSpec>,
    ) -> NetworkGlobals<E> {
        let metadata = MetaData::V3(MetaDataV3 {
            seq_number: 0,
            attnets: Default::default(),
            syncnets: Default::default(),
            custody_group_count: spec.custody_requirement,
        });
        Self::new_test_globals_with_metadata(trusted_peers, metadata, config, spec)
    }

    pub(crate) fn new_test_globals_with_metadata(
        trusted_peers: Vec<PeerId>,
        // TODO: todo! Apply to enr
        _metadata: MetaData<E>,
        config: Arc<NetworkConfig>,
        spec: Arc<ChainSpec>,
    ) -> NetworkGlobals<E> {
        use crate::CombinedKeyExt;
        let keypair = libp2p::identity::secp256k1::Keypair::generate();
        let enr_key: discv5::enr::CombinedKey = discv5::enr::CombinedKey::from_secp256k1(&keypair);
        let enr = discv5::enr::Enr::builder().build(&enr_key).unwrap();
        let cgc_updates = CGCUpdates::new(spec.custody_requirement);
        NetworkGlobals::new(enr, cgc_updates, trusted_peers, false, config, spec)
    }
}

impl CGCUpdates {
    pub fn new(initial_value: u64) -> Self {
        Self {
            initial_value,
            updates: vec![],
        }
    }

    fn at_slot(&self, slot: Slot) -> u64 {
        // TODO: Test and fix logic
        for (update_slot, cgc) in &self.updates {
            if slot > *update_slot {
                return *cgc;
            }
        }

        self.initial_value
    }

    fn add_latest_update(&mut self, update: (Slot, u64)) {
        self.updates.push(update);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use logging::create_test_tracing_subscriber;
    use types::{Epoch, EthSpec, MainnetEthSpec as E};

    #[test]
    fn test_sampling_subnets() {
        create_test_tracing_subscriber();
        let mut spec = E::default_spec();
        spec.fulu_fork_epoch = Some(Epoch::new(0));

        let custody_group_count = spec.number_of_custody_groups / 2;
        let subnet_sampling_size = spec.sampling_size(custody_group_count).unwrap();
        let metadata = get_metadata(custody_group_count);
        let config = Arc::new(NetworkConfig::default());
        let slot = Slot::new(0);

        let globals = NetworkGlobals::<E>::new_test_globals_with_metadata(
            vec![],
            metadata,
            config,
            Arc::new(spec),
        );
        assert_eq!(
            globals.sampling_subnets(slot).len(),
            subnet_sampling_size as usize
        );
    }

    #[test]
    fn test_sampling_columns() {
        create_test_tracing_subscriber();
        let mut spec = E::default_spec();
        spec.fulu_fork_epoch = Some(Epoch::new(0));

        let custody_group_count = spec.number_of_custody_groups / 2;
        let subnet_sampling_size = spec.sampling_size(custody_group_count).unwrap();
        let metadata = get_metadata(custody_group_count);
        let config = Arc::new(NetworkConfig::default());
        let slot = Slot::new(0);

        let globals = NetworkGlobals::<E>::new_test_globals_with_metadata(
            vec![],
            metadata,
            config,
            Arc::new(spec),
        );
        assert_eq!(
            globals.sampling_columns(slot).len(),
            subnet_sampling_size as usize
        );
    }

    fn get_metadata(custody_group_count: u64) -> MetaData<E> {
        MetaData::V3(MetaDataV3 {
            seq_number: 0,
            attnets: Default::default(),
            syncnets: Default::default(),
            custody_group_count,
        })
    }
}
