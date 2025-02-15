use super::{EthSpec, InclusionListTransactions, SignedInclusionList, Slot, Transaction};
use slog::{debug, Logger};
use std::collections::{HashMap, HashSet};

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
    pub fn clear_cache(&mut self, slot: Slot) {
        self.inner_map.remove(&slot);
    }

    pub fn on_inclusion_list(&mut self, inclusion_list: SignedInclusionList<E>, log: &Logger) {
        let slot = inclusion_list.message.slot;
        let inner = self.inner_map.entry(slot).or_default();

        if inner
            .inclusion_list_equivocators
            .contains(&inclusion_list.message.validator_index)
        {
            debug!(
                log,
                "This validator was flagged for an equivocating inclusion list";
                "slot" => slot,
                "validator_index" => inclusion_list.message.validator_index
            );
            return;
        }

        // Skip inserting into the cache if we've already seen an identical IL
        if inner
            .inclusion_lists_seen
            .contains(&inclusion_list.message.validator_index)
            && inner.inclusion_lists.contains(&inclusion_list)
        {
            debug!(
                log,
                "Already seen identical inclusion list from this validator";
            );
            return;
        }

        if inner
            .inclusion_lists_seen
            .contains(&inclusion_list.message.validator_index)
            && !inner.inclusion_lists.contains(&inclusion_list)
        {
            debug!(
                log,
                "Equivocating inclusion list";
                "slot" => slot,
                "validator_index" => inclusion_list.message.validator_index
            );
            inner
                .inclusion_list_equivocators
                .insert(inclusion_list.message.validator_index);
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

        debug!(
            log,
            "Successfully added inclusion list transactions to the cache";
            "slot" => slot,
            "total_count" => inner.inclusion_list_transactions.len()
        );
    }

    pub fn get_inclusion_list_transactions(
        &self,
        slot: Slot,
    ) -> Option<InclusionListTransactions<E>> {
        let Some(inner) = self.inner_map.get(&slot) else {
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
