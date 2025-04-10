use crate::*;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize, Clone, Encode, Decode)]
pub struct CGCUpdates {
    initial_value: u64,
    updates: VariableList<(Slot, u64), ssz_types::typenum::U131072>,
    // TODO(das): Track backfilled CGC
}

impl CGCUpdates {
    pub fn new(initial_value: u64) -> Self {
        Self {
            initial_value,
            updates: VariableList::empty(),
        }
    }

    pub fn at_slot(&self, slot: Slot) -> u64 {
        // TODO: Test and fix logic
        for (update_slot, cgc) in &self.updates {
            if slot > *update_slot {
                return *cgc;
            }
        }

        self.initial_value
    }

    pub fn add_latest_update(&mut self, update: (Slot, u64)) -> Result<(), String> {
        self.updates
            .push(update)
            .map_err(|e| format!("Updates list full: {e:?}"))
    }

    pub fn prune_updates_older_than(&mut self, slot: Slot) {
        todo!("{slot}");
    }

    pub fn iter(&self) -> impl Iterator<Item = (Slot, u64)> + '_ {
        std::iter::once((Slot::new(0), self.initial_value)).chain(self.updates.iter().copied())
    }
}
