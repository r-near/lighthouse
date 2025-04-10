//! A collection of variables that are accessible outside of the network thread itself.
use super::TopicConfig;
use crate::peer_manager::peerdb::PeerDB;
use crate::rpc::MetaData;
use crate::types::{BackFillState, SyncState};
use crate::{Client, Enr, EnrExt, GossipTopic, Multiaddr, NetworkConfig, PeerId};
use local_metadata::LocalMetadata;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;
use types::data_column_custody_group::{
    compute_columns_from_custody_groups, compute_subnets_from_custody_groups, get_custody_groups,
};
use types::{CGCUpdates, ChainSpec, ColumnIndex, DataColumnSubnetId, EthSpec, Slot};

pub struct NetworkGlobals<E: EthSpec> {
    /// The current local ENR.
    local_enr: RwLock<LocalMetadata<E>>,
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
    /// The computed custody groups cached to avoid re-computing.
    custody_groups_max_cgc: Vec<u64>,
    sampling_columns_max_cgc: Vec<ColumnIndex>,
    sampling_subnets_max_cgc: Vec<DataColumnSubnetId>,
    /// Dynamic custody group count (CGC)
    cgc_updates: RwLock<CGCUpdates>,
    /// Network-related configuration. Immutable after initialization.
    pub config: Arc<NetworkConfig>,
    /// Ethereum chain configuration. Immutable after initialization.
    pub spec: Arc<ChainSpec>,
}

impl<E: EthSpec> NetworkGlobals<E> {
    pub fn new(
        enr: Enr,
        cgc_updates: CGCUpdates,
        trusted_peers: Vec<PeerId>,
        disable_peer_scoring: bool,
        config: Arc<NetworkConfig>,
        spec: Arc<ChainSpec>,
    ) -> Result<Self, String> {
        let node_id = enr.node_id().raw();

        // The below `expect` calls will panic on start up if the chain spec config values used
        // are invalid
        let custody_groups_max_cgc =
            get_custody_groups(node_id, spec.number_of_custody_groups, &spec)
                .expect("should compute node custody groups");
        let sampling_columns_max_cgc =
            compute_columns_from_custody_groups(&custody_groups_max_cgc, &spec).collect::<Vec<_>>();
        let sampling_subnets_max_cgc =
            compute_subnets_from_custody_groups(&custody_groups_max_cgc, &spec).collect::<Vec<_>>();

        Ok(NetworkGlobals {
            local_enr: RwLock::new(LocalMetadata::new(enr.clone(), &spec)?),
            peer_id: RwLock::new(enr.peer_id()),
            listen_multiaddrs: RwLock::new(Vec::new()),
            peers: RwLock::new(PeerDB::new(trusted_peers, disable_peer_scoring)),
            gossipsub_subscriptions: RwLock::new(HashSet::new()),
            sync_state: RwLock::new(SyncState::Stalled),
            backfill_state: RwLock::new(BackFillState::Paused),
            custody_groups_max_cgc,
            sampling_columns_max_cgc,
            sampling_subnets_max_cgc,
            cgc_updates: RwLock::new(cgc_updates),
            config,
            spec,
        })
    }

    /// Returns the local ENR from the underlying Discv5 behaviour that external peers may connect
    /// to.
    pub fn local_enr(&self) -> Enr {
        self.local_enr.read().enr().clone()
    }

    pub fn set_enr(&self, enr: Enr) -> Result<(), String> {
        *self.local_enr.write() = LocalMetadata::new(enr, &self.spec)?;
        Ok(())
    }

    /// Returns the local libp2p PeerID.
    pub fn local_peer_id(&self) -> PeerId {
        *self.peer_id.read()
    }

    // Returns MetaData based on the cached local ENR fields. Local ENR from discv5 is the source of
    // truth for the announced CGC value and the attnets and syncnets bitfields.
    pub fn local_metadata(&self) -> MetaData<E> {
        self.local_enr.read().metadata().clone()
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
        let cgc = self.custody_group_count(slot);
        self.sampling_subnets_for_cgc(cgc)
    }

    pub fn sampling_columns(&self, slot: Slot) -> &[ColumnIndex] {
        let cgc = self.custody_group_count(slot);
        self.sampling_columns_for_cgc(cgc)
    }

    pub fn custody_groups_for_cgc(&self, cgc: u64) -> &[u64] {
        &self.custody_groups_max_cgc[..self.custody_groups_max_cgc.len().min(cgc as usize)]
    }

    pub fn sampling_subnets_for_cgc(&self, cgc: u64) -> &[DataColumnSubnetId] {
        // TODO(das): scale this index if custody_groups != subnet_count != column_count
        let index = cgc as usize;
        &self.sampling_subnets_max_cgc[..self.sampling_subnets_max_cgc.len().min(index)]
    }

    pub fn sampling_columns_for_cgc(&self, cgc: u64) -> &[ColumnIndex] {
        // TODO(das): scale this index if custody_groups != subnet_count != column_count
        let index = cgc as usize;
        &self.sampling_columns_max_cgc[..self.sampling_columns_max_cgc.len().min(index)]
    }

    /// Returns the custody group count (CGC)
    pub fn custody_group_count(&self, slot: Slot) -> u64 {
        self.cgc_updates.read().at_slot(slot)
    }

    /// Returns the minimum CGC value in the range of slots `range`. If the range is empty,
    /// i.e. `3..1` returns the CGC value at `range.start`.
    pub fn min_custody_group_count_at_range(&self, slot_range: Range<Slot>) -> u64 {
        self.cgc_updates.read().min_at_slot_range(slot_range)
    }

    /// Returns the count of custody columns this node must sample for block import
    pub fn custody_columns_count(&self, slot: Slot) -> u64 {
        // This only panics if the chain spec contains invalid values
        self.spec
            .sampling_size(self.custody_group_count(slot))
            .expect("should compute node sampling size from valid chain spec")
    }

    /// Adds a new CGC value update
    pub fn add_cgc_update(&self, update_start_slot: Slot, cgc: u64) -> Result<(), String> {
        self.cgc_updates
            .write()
            .add_latest_update((update_start_slot, cgc))
    }

    pub fn prune_cgc_updates_older_than(&self, slot: Slot) {
        self.cgc_updates.write().prune_updates_older_than(slot);
    }

    pub fn dump_cgc_updates(&self) -> CGCUpdates {
        self.cgc_updates.read().clone()
    }

    pub fn cgc_updates_len(&self) -> usize {
        self.cgc_updates.read().len()
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
        let initial_cgc = spec.custody_requirement;
        Self::new_test_globals_with_initial_cgc(trusted_peers, initial_cgc, config, spec)
    }

    pub(crate) fn new_test_globals_with_initial_cgc(
        trusted_peers: Vec<PeerId>,
        initial_cgc: u64,
        config: Arc<NetworkConfig>,
        spec: Arc<ChainSpec>,
    ) -> NetworkGlobals<E> {
        use crate::CombinedKeyExt;
        let keypair = libp2p::identity::secp256k1::Keypair::generate();
        let enr_key: discv5::enr::CombinedKey = discv5::enr::CombinedKey::from_secp256k1(&keypair);
        let enr = discv5::enr::Enr::builder().build(&enr_key).unwrap();
        let cgc_updates = CGCUpdates::new(initial_cgc);
        NetworkGlobals::new(enr, cgc_updates, trusted_peers, false, config, spec).unwrap()
    }
}

mod local_metadata {
    use crate::discovery::enr::Eth2Enr;
    use crate::rpc::{MetaData, MetaDataV2, MetaDataV3};
    use crate::Enr;
    use types::{ChainSpec, EthSpec};

    /// Ensures that the cached local ENR and its parsed MetaData are updated atomically.
    pub struct LocalMetadata<E: EthSpec> {
        enr: Enr,
        metadata: MetaData<E>,
    }

    impl<E: EthSpec> LocalMetadata<E> {
        pub fn new(enr: Enr, spec: &ChainSpec) -> Result<Self, String> {
            let attnets = enr.attestation_bitfield::<E>()?;
            let syncnets = enr.sync_committee_bitfield::<E>()?;

            let metadata = if spec.is_peer_das_scheduled() {
                MetaData::V3(MetaDataV3 {
                    seq_number: enr.seq(),
                    attnets,
                    syncnets,
                    custody_group_count: enr
                        .custody_group_count(spec)?
                        .unwrap_or(spec.custody_requirement),
                })
            } else {
                MetaData::V2(MetaDataV2 {
                    seq_number: enr.seq(),
                    attnets,
                    syncnets,
                })
            };

            Ok(Self { enr, metadata })
        }

        pub fn enr(&self) -> &Enr {
            &self.enr
        }

        pub fn metadata(&self) -> &MetaData<E> {
            &self.metadata
        }
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
        let config = Arc::new(NetworkConfig::default());
        let slot = Slot::new(0);

        let globals = NetworkGlobals::<E>::new_test_globals_with_initial_cgc(
            vec![],
            custody_group_count,
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
        let config = Arc::new(NetworkConfig::default());
        let slot = Slot::new(0);

        let globals = NetworkGlobals::<E>::new_test_globals_with_initial_cgc(
            vec![],
            custody_group_count,
            config,
            Arc::new(spec),
        );
        assert_eq!(
            globals.sampling_columns(slot).len(),
            subnet_sampling_size as usize
        );
    }
}
