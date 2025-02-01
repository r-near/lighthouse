use crate::{BeaconChain, BeaconChainError, BeaconChainTypes};

use slot_clock::SlotClock;
use strum::AsRefStr;
use types::{Domain, EthSpec, SignedInclusionList, SignedRoot, Slot};

#[derive(Debug, AsRefStr)]
pub enum GossipInclusionListError {
    FutureSlot {
        message_slot: Slot,
        latest_permissible_slot: Slot,
    },
    PastSlot {
        message_slot: Slot,
        earliest_permissible_slot: Slot,
    },
    InvalidCommitteeRoot,
    ValidatorNotInCommittee,
    TooManyTransactions,
    InvalidSignature,
    BeaconChainError(BeaconChainError),
    // TODO: equivocation e.g. PriorInclusionListKnown
}

impl From<BeaconChainError> for GossipInclusionListError {
    fn from(value: BeaconChainError) -> Self {
        Self::BeaconChainError(value)
    }
}

pub struct GossipVerifiedInclusionList<T: BeaconChainTypes> {
    pub signed_il: SignedInclusionList<T::EthSpec>,
}

impl<T: BeaconChainTypes> GossipVerifiedInclusionList<T> {
    pub fn verify(
        signed_il: &SignedInclusionList<T::EthSpec>,
        chain: &BeaconChain<T>,
    ) -> Result<Self, GossipInclusionListError> {
        // the slot is equal to the previous slot or the current slot
        let message_slot = signed_il.message.slot;
        let earliest_permissible_slot = chain
            .slot_clock
            .now_with_past_tolerance(chain.spec.maximum_gossip_clock_disparity())
            .ok_or(BeaconChainError::UnableToReadSlot)?;
        if message_slot < earliest_permissible_slot {
            return Err(GossipInclusionListError::PastSlot {
                message_slot,
                earliest_permissible_slot,
            });
        }
        let latest_permissible_slot = chain
            .slot_clock
            .now_with_future_tolerance(chain.spec.maximum_gossip_clock_disparity())
            .ok_or(BeaconChainError::UnableToReadSlot)?;
        if message_slot > latest_permissible_slot {
            return Err(GossipInclusionListError::FutureSlot {
                message_slot,
                latest_permissible_slot,
            });
        }

        // TODO: the slot is equal to the current slot or the previous slot and the current time is
        // not past the attestation deadline

        // TODO: the IL committee root is equal to the hash tree root of the expected committee

        // TODO: the validator index is contained in the committee corresponding to the committee
        // root

        // the transaction length is less than or equal to the specified maximum
        if signed_il.message.transactions.len() > T::EthSpec::max_transactions_per_inclusion_list()
        {
            return Err(GossipInclusionListError::TooManyTransactions);
        }

        // TODO: the message is the first or second valid message received from the validator
        // corresponding to the validator index

        // the signature is valid w.r.t. the validator index
        let epoch = chain.epoch()?;
        let fork = chain.spec.fork_at_epoch(epoch);
        let genesis_validators_root = chain.genesis_validators_root;
        let domain = chain.spec.get_domain(
            epoch,
            Domain::InclusionListCommittee,
            &fork,
            genesis_validators_root,
        );
        let message = signed_il.message.signing_root(domain);
        let validator_index = signed_il.message.validator_index as usize;
        let pubkey = chain.validator_pubkey(validator_index)?;
        let Some(pubkey) = pubkey else {
            return Err(GossipInclusionListError::BeaconChainError(
                BeaconChainError::ValidatorIndexUnknown(validator_index),
            ));
        };
        signed_il.signature.verify(&pubkey, message);

        Ok(Self {
            signed_il: signed_il.clone(),
        })
    }
}
