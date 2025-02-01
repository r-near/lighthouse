use std::collections::{HashMap, HashSet};

use super::{EthSpec, InclusionListTransactions, SignedInclusionList, Slot, Transaction};

/// Map from slot to inclusion lists
#[derive(Debug, Default, Clone, PartialEq)]
pub struct InclusionListCache<E: EthSpec> {
    inner_map: HashMap<Slot, Inner<E>>,
}

type ValidatorIndex = u64;

#[derive(Debug, Default, Clone, PartialEq)]
struct Inner<E: EthSpec> {
    pub inclusion_lists: HashSet<SignedInclusionList<E>>,
    pub inclusion_lists_seen: HashSet<ValidatorIndex>,
    pub inclusion_list_equivocators: HashSet<ValidatorIndex>,
    pub inclusion_list_transactions: HashSet<Transaction<E::MaxBytesPerTransaction>>,
}

impl<E: EthSpec> InclusionListCache<E> {
    pub fn initialize(&mut self, slot: Slot) {
        let inner = Inner {
            inclusion_lists: HashSet::new(),
            inclusion_lists_seen: HashSet::new(),
            inclusion_list_equivocators: HashSet::new(),
            inclusion_list_transactions: HashSet::new(),
        };

        self.inner_map.insert(slot, inner);
    }

    pub fn clear_cache(&mut self, slot: Slot) {
        self.inner_map.remove(&slot);
    }

    pub fn on_inclusion_list(&mut self, inclusion_list: SignedInclusionList<E>) {
        let Some(inner) = self.inner_map.get_mut(&inclusion_list.message.slot) else {
            return;
        };

        if inner
            .inclusion_list_equivocators
            .contains(&inclusion_list.message.validator_index)
        {
            return;
        }

        if inner
            .inclusion_lists_seen
            .contains(&inclusion_list.message.validator_index)
            && !inner.inclusion_lists.contains(&inclusion_list)
        {
            inner
                .inclusion_list_equivocators
                .insert(inclusion_list.message.validator_index);
            return;
        }

        // Skip inserting into the cache if we've already seen an identical IL
        if inner
            .inclusion_lists_seen
            .contains(&inclusion_list.message.validator_index)
            && inner.inclusion_lists.contains(&inclusion_list)
        {
            return;
        }

        for transaction in &inclusion_list.message.transactions {
            inner
                .inclusion_list_transactions
                .insert(transaction.clone());
        }
        inner
            .inclusion_lists_seen
            .insert(inclusion_list.message.validator_index);
        inner.inclusion_lists.insert(inclusion_list);
    }

    pub fn get_inclusion_list_transactions(
        &self,
        slot: Slot,
    ) -> Option<InclusionListTransactions<E>> {
        let Some(inner) = &self.inner_map.get(&slot) else {
            return None;
        };

        let il = inner
            .inclusion_list_transactions
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        Some(il.into())
    }
}

impl<E: EthSpec> arbitrary::Arbitrary<'_> for InclusionListCache<E> {
    fn arbitrary(_u: &mut arbitrary::Unstructured<'_>) -> arbitrary::Result<Self> {
        Ok(Self::default())
    }
}
