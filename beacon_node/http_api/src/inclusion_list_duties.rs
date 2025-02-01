use beacon_chain::{BeaconChain, BeaconChainTypes};
use eth2::types::{self as api_types};
use slot_clock::SlotClock;
use types::{Epoch, EthSpec, Hash256, InclusionListDuty};

/// The struct that is returned to the requesting HTTP client.
type ApiDuties = api_types::DutiesResponse<Vec<api_types::InclusionListDutyData>>;

/// Handles a request from the HTTP API for inclusion list duties.
pub fn inclusion_list_duties<T: BeaconChainTypes>(
    request_epoch: Epoch,
    request_indices: &[u64],
    chain: &BeaconChain<T>,
) -> Result<ApiDuties, warp::reject::Rejection> {
    let current_epoch = chain
        .epoch()
        // TODO(focil) unwrap
        .unwrap();
        // .map_err(warp_utils::reject::beacon_chain_error)?;

    // Determine what the current epoch would be if we fast-forward our system clock by
    // `MAXIMUM_GOSSIP_CLOCK_DISPARITY`.
    //
    // Most of the time, `tolerant_current_epoch` will be equal to `current_epoch`. However, during
    // the first `MAXIMUM_GOSSIP_CLOCK_DISPARITY` duration of the epoch `tolerant_current_epoch`
    // will equal `current_epoch + 1`
    let tolerant_current_epoch = chain
        .slot_clock
        .now_with_future_tolerance(chain.spec.maximum_gossip_clock_disparity())
        .ok_or_else(|| warp_utils::reject::custom_server_error("unable to read slot clock".into()))?
        .epoch(T::EthSpec::slots_per_epoch());

    if request_epoch == current_epoch
        || request_epoch == current_epoch + 1
        || request_epoch == tolerant_current_epoch + 1
    {
        let head_block_root = chain.canonical_head.cached_head().head_block_root();
        let (duties, dependent_root) = chain
            .validator_inclusion_list_duties(request_indices, request_epoch, head_block_root)
            // TODO(focil) unwrap
            .unwrap();
            //.map_err(warp_utils::reject::beacon_chain_error)?;
        convert_to_api_response(duties, request_indices, dependent_root, chain)
    } else if request_epoch > current_epoch + 1 {
        Err(warp_utils::reject::custom_bad_request(format!(
            "request epoch {} is more than one epoch past the current epoch {}",
            request_epoch, current_epoch
        )))
    } else {
        // request_epoch < current_epoch
        //
        // TODO: support historical inclusion list duties requests
        Err(warp_utils::reject::custom_bad_request(format!(
            "request epoch {} is earlier than the current epoch {}",
            request_epoch, current_epoch
        )))
    }
}

/// Convert the internal representation of attester duties into the format returned to the HTTP
/// client.
fn convert_to_api_response<T: BeaconChainTypes>(
    duties: Vec<Option<InclusionListDuty>>,
    indices: &[u64],
    dependent_root: Hash256,
    chain: &BeaconChain<T>,
) -> Result<ApiDuties, warp::reject::Rejection> {
    // Protect against an inconsistent slot clock.
    if duties.len() != indices.len() {
        return Err(warp_utils::reject::custom_server_error(format!(
            "duties length {} does not match indices length {}",
            duties.len(),
            indices.len()
        )));
    }

    let usize_indices = indices.iter().map(|i| *i as usize).collect::<Vec<_>>();
    let index_to_pubkey_map = chain
        .validator_pubkey_bytes_many(&usize_indices)
        // TODO(focil) unwrap
        .unwrap();
        // .map_err(warp_utils::reject::beacon_chain_error)?;

    let data = duties
        .into_iter()
        .zip(indices)
        .filter_map(|(duty_opt, &validator_index)| {
            let duty = duty_opt?;
            Some(api_types::InclusionListDutyData {
                validator_index,
                pubkey: *index_to_pubkey_map.get(&(validator_index as usize))?,
                slot: duty.slot,
            })
        })
        .collect::<Vec<_>>();

    // TODO: account for optimistic execution
    Ok(api_types::DutiesResponse {
        dependent_root,
        execution_optimistic: None,
        data,
    })
}
