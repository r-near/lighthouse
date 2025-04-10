use crate::*;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use std::cmp::Ordering;
use std::ops::Range;

type CGCUpdate = (Slot, u64);

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize, Clone, Encode, Decode)]
pub struct CGCUpdates {
    // Updates ordered in ascending slot order.
    //
    // It always contains at least one item.
    updates: VariableList<CGCUpdate, ssz_types::typenum::U131072>,
}

#[allow(clippy::len_without_is_empty)]
impl CGCUpdates {
    pub fn new(initial_cgc: u64) -> Self {
        // The slot of the initial update doesn't matter. It's only relevant when pushing the next
        // update if it has the same Slot. Otherwise, the result function `cgc(slot)` is independent
        // of the value of initial_update.slot
        Self {
            updates: VariableList::new(vec![(Slot::new(0), initial_cgc)]).expect("1 < 131072"),
        }
    }

    /// Returns the CGC value for the given slot by locating the most recent applicable update.
    /// If the slot is before the first update, returns the first update's value.
    pub fn at_slot(&self, slot: Slot) -> u64 {
        self.updates
            .get(self.update_index_at_slot(slot))
            .expect("updates.len() > 0 and binary_search_by_key returns index in range")
            .1
    }

    /// Returns the update index for the given slot by locating the most recent applicable update.
    /// If the slot is before the first update, returns the first update's value.
    fn update_index_at_slot(&self, slot: Slot) -> usize {
        match &self.updates.binary_search_by_key(&slot, |(s, _)| *s) {
            Ok(i) => {
                // binary_search_by_key found an exact matching slot
                *i
            }
            Err(i) => {
                // binary_search_by_key did NOT found an exact matching slot. The returned index is
                // the position where `slot` could be inserted while maintaining sorted order
                //
                // To have a continuous function to zero, slot values less than the oldest
                // update (index = 0) have the CGC of the oldest update (index = 0). So we use
                // saturating_sub to emulate `if i == 0 { 0 }`
                i.saturating_sub(1)
            }
        }
    }

    /// Returns the ordered list of CGC values in the range of slots `range`. If the range is empty,
    /// i.e. `3..1` returns the CGC value at `range.start`. The return vector will never be empty.
    fn at_slot_range(&self, range: Range<Slot>) -> Vec<u64> {
        let first_update_index = self.update_index_at_slot(range.start);

        let cgcs = self
            .updates
            .get(first_update_index..)
            .expect("updates.len() > 0 and binary_search_by_key returns index in range")
            .iter()
            .take_while(|(s, _)| *s < range.end)
            .map(|(_, cgc)| *cgc)
            .collect::<Vec<_>>();

        if cgcs.is_empty() {
            let last_update = self
                .updates
                .get(first_update_index)
                .expect("updates.len() > 0 and binary_search_by_key returns index in range");
            vec![last_update.1]
        } else {
            cgcs
        }
    }

    /// Returns the minimum CGC value in the range of slots `range`. If the range is empty,
    /// i.e. `slot..slot` returns the CGC value at `slot`.
    pub fn min_at_slot_range(&self, range: Range<Slot>) -> u64 {
        *self
            .at_slot_range(range)
            .iter()
            .min()
            .expect("at_slot_range never returns empty Vec")
    }

    pub fn add_latest_update(&mut self, update: CGCUpdate) -> Result<(), String> {
        if let Some(last_update) = self.updates.last_mut() {
            match last_update.0.cmp(&update.0) {
                // Ok, continue
                Ordering::Less => {}
                // Trying to push an update for the same Slot
                Ordering::Equal => {
                    *last_update = update;
                    return Ok(());
                }
                // Updates are strictly ascending, not allowed
                Ordering::Greater => {
                    return Err(format!(
                        "CGCUpdates must be strictly ascending {} > {}",
                        last_update.0, update.0
                    ))
                }
            }
        }

        self.updates
            .push(update)
            .map_err(|e| format!("Updates list full: {e:?}"))
    }

    pub fn prune_updates_older_than(&mut self, slot: Slot) {
        let to_keep = self
            .updates
            .iter()
            .filter(|(s, _)| *s >= slot)
            .copied()
            .collect::<Vec<_>>();

        self.updates = if to_keep.is_empty() {
            // All updates are < slot, so we should prune all of them but keep the most recent one
            VariableList::new(vec![*self.updates.last().expect("len > 0")]).expect("1 < 131072")
        } else {
            VariableList::new(to_keep).expect("len is reduced")
        };
    }

    pub fn iter(&self) -> impl Iterator<Item = CGCUpdate> + '_ {
        self.updates.iter().copied()
    }

    pub fn len(&self) -> usize {
        self.updates.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new(updates: &[(u64, u64)]) -> CGCUpdates {
        let first_update = *updates.first().expect("should have at least one update");
        let mut u = CGCUpdates::new(first_update.1);
        for update in updates.iter().skip(1) {
            u.add_latest_update(to(*update)).unwrap();
        }
        u
    }

    fn at(updates: &CGCUpdates, slot: u64, expected_cgc: u64) {
        assert_eq!(
            updates.at_slot(Slot::new(slot)),
            expected_cgc,
            "Case ({slot}, {expected_cgc})"
        );
    }

    fn at_range(updates: &CGCUpdates, slots: Range<u64>, expected_cgcs: &[u64]) {
        let cgcs = updates.at_slot_range(Range {
            start: Slot::new(slots.start),
            end: Slot::new(slots.end),
        });
        assert_eq!(&cgcs, expected_cgcs, "Case ({slots:?}, {expected_cgcs:?})");
    }

    fn add(updates: &mut CGCUpdates, slot: u64, cgc: u64) {
        updates.add_latest_update(to((slot, cgc))).unwrap();
    }

    fn to(update: (u64, u64)) -> CGCUpdate {
        (Slot::new(update.0), update.1)
    }

    fn assert_len(updates: &CGCUpdates, len: usize) {
        assert_eq!(updates.iter().count(), len, "Wrong len");
    }

    fn prune(updates: &mut CGCUpdates, slot: u64) {
        updates.prune_updates_older_than(Slot::new(slot));
    }

    const MAX: u64 = u64::MAX;

    #[test]
    fn query_single_zero() {
        // README: These tests do:
        // - Create CGCUpdates from the list of updates passed to `new()`
        // - Assert that `at(slot, cgc)` `updates::at_slot(slot)` returns `cgc`
        let u = new(&[(0, 0)]);
        at(&u, 0, 0);
        at(&u, MAX, 0);
    }

    #[test]
    fn query_single_nonzero() {
        let u = new(&[(1, 10)]);
        at(&u, 0, 10);
        at(&u, 1, 10);
        at(&u, 2, 10);
        at(&u, MAX, 10);
    }

    #[test]
    fn query_two() {
        let u = new(&[(1, 10), (3, 30)]);
        at(&u, 0, 10);
        at(&u, 1, 10);
        at(&u, 2, 10);
        at(&u, 3, 30);
        at(&u, 4, 30);
        at(&u, MAX, 30);
    }

    #[test]
    fn query_range_single_update_zero() {
        let u = new(&[(0, 0)]);
        at_range(&u, 0..0, &[0]);
        at_range(&u, 0..1, &[0]);
        at_range(&u, 0..MAX, &[0]);
        at_range(&u, 1..MAX, &[0]);
        at_range(&u, MAX..MAX, &[0]);
    }

    #[test]
    fn query_range_single_update_nonzero() {
        let u = new(&[(1, 10)]);
        at_range(&u, 0..0, &[10]);
        at_range(&u, 0..1, &[10]);
        at_range(&u, 0..MAX, &[10]);
        at_range(&u, 1..MAX, &[10]);
        at_range(&u, 2..MAX, &[10]);
        at_range(&u, MAX..MAX, &[10]);
    }

    #[test]
    fn query_range_two() {
        let u = new(&[(1, 10), (3, 30)]);
        at_range(&u, 0..0, &[10]);
        at_range(&u, 0..1, &[10]);
        at_range(&u, 0..3, &[10]);
        at_range(&u, 0..4, &[10, 30]);
        at_range(&u, 0..MAX, &[10, 30]);
        at_range(&u, 1..4, &[10, 30]);
        at_range(&u, 2..4, &[10, 30]);
        at_range(&u, 3..4, &[30]);
        at_range(&u, 3..MAX, &[30]);
        at_range(&u, MAX..MAX, &[30]);
    }

    #[test]
    fn query_range_multiple() {
        let u = new(&[(1, 10), (3, 30), (6, 60), (7, 70), (9, 90)]);
        at_range(&u, 0..0, &[10]);
        at_range(&u, 0..1, &[10]);
        at_range(&u, 0..3, &[10]);
        at_range(&u, 0..4, &[10, 30]);
        at_range(&u, 1..4, &[10, 30]);
        at_range(&u, 2..4, &[10, 30]);
        at_range(&u, 2..7, &[10, 30, 60]);
        at_range(&u, 6..8, &[60, 70]);
        at_range(&u, 7..8, &[70]);
        at_range(&u, 7..9, &[70]);
        at_range(&u, 7..MAX, &[70, 90]);
        at_range(&u, MAX..MAX, &[90]);
        at_range(&u, 0..MAX, &[10, 30, 60, 70, 90]);
    }

    #[test]
    fn add_update_replace_last() {
        let mut u = new(&[(1, 10)]);
        at(&u, 1, 10);
        add(&mut u, 1, 20);
        assert_len(&u, 1);
        at(&u, 1, 20);
    }

    #[test]
    fn add_update_append() {
        let mut u = new(&[(1, 10)]);
        at(&u, 2, 10);
        add(&mut u, 2, 20);
        assert_len(&u, 2);
        at(&u, 2, 20);
    }

    #[test]
    fn prune_single_update() {
        let mut u = new(&[(1, 10)]);
        prune(&mut u, 1); // No-op, a single update
        assert_len(&u, 1);
        prune(&mut u, 2); // No-op, a single update
        assert_len(&u, 1);
    }

    #[test]
    fn prune_two_updates_less_than() {
        let mut u = new(&[(1, 10), (3, 30)]);
        prune(&mut u, 1); // No-op, no update older
        assert_len(&u, 2);
        prune(&mut u, 2); // Prunes (1, 10)
        assert_len(&u, 1);
    }

    #[test]
    fn prune_two_updates_exact() {
        let mut u = new(&[(1, 10), (3, 30)]);
        prune(&mut u, 3); // Prunes (1, 10)
        assert_len(&u, 1);
    }

    #[test]
    fn prune_two_updates_max() {
        let mut u = new(&[(1, 10), (3, 30)]);
        prune(&mut u, MAX); // Prunes (1, 10)
        assert_len(&u, 1);
    }
}
