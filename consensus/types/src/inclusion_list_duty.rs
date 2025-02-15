use crate::*;
use serde::{Deserialize, Serialize};

#[derive(arbitrary::Arbitrary, Debug, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub struct InclusionListDuty {
    /// The slot during which the validator must produce an inclusion list.
    pub slot: Slot,
    #[serde(with = "serde_utils::quoted_u64")]
    /// The index of the validator.
    pub validator_index: u64,
    /// The hash tree root of the inclusion list committee.
    pub committee_root: Hash256,
    /// The pubkey of the validator.
    pub pubkey: PublicKeyBytes,
}
