use std::collections::HashMap;

use substreams::{
    pb::substreams::StoreDeltas,
    prelude::BigInt,
    store::{StoreAddBigInt, StoreGet, StoreGetRaw, StoreNew},
};
use substreams_ethereum::{pb::eth::v2 as eth, Event};

use crate::store_key::StoreKey;
use tycho_substreams::{abi::erc20, prelude::*};

/// Tracks underlying ERC-20 balances held by official FewToken wrappers.
///
/// Ring pairs hold FewTokens, while the executor unwraps the output FewToken into its underlying
/// ERC-20. The pair reserve therefore is not sufficient liquidity information: each wrapper's
/// underlying backing is a separate, dynamic bound on executable output.
#[substreams::handlers::map]
pub fn map_wrapper_backing_deltas(
    block: eth::Block,
    wrapper_store_deltas: StoreDeltas,
    wrapper_store: StoreGetRaw,
) -> Result<BlockBalanceDeltas, substreams::errors::Error> {
    let mut balance_deltas = Vec::new();
    let new_wrappers = newly_tracked_wrappers(wrapper_store_deltas)?;

    for raw_tx in block.transactions() {
        let transaction: Transaction = raw_tx.into();
        for (log, _) in raw_tx.logs_with_calls() {
            let token = log.address.clone();
            let token_key = StoreKey::FewWrapper.get_unique_key(&hex::encode(&token));

            // The snapshot above is already the end-of-block value for a newly tracked wrapper.
            // Applying transfers from the same block would double-count its backing.
            if new_wrappers.contains_key(&token) {
                continue;
            }

            let Some(wrapper) = wrapper_store.get_last(&token_key) else {
                continue;
            };

            let Some(transfer) = erc20::events::Transfer::match_and_decode(log) else {
                continue;
            };

            let mut delta = BigInt::zero();
            if transfer.from.as_slice() == wrapper.as_slice() {
                delta = delta - transfer.value.clone();
            }
            if transfer.to.as_slice() == wrapper.as_slice() {
                delta = delta + transfer.value;
            }
            if delta == BigInt::zero() {
                continue;
            }

            balance_deltas.push(BalanceDelta {
                ord: log.ordinal,
                tx: Some(transaction.clone()),
                token,
                delta: delta.to_signed_bytes_be(),
                component_id: hex::encode(wrapper).into_bytes(),
            });
        }
    }

    if let Some(last_tx) = block
        .transaction_traces
        .last()
        .map(Transaction::from)
    {
        for (underlying, wrapper) in &new_wrappers {
            let balance = erc20::functions::BalanceOf { owner: wrapper.clone() }
                .call(underlying.clone())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Unable to snapshot underlying backing for FewToken wrapper {}",
                        hex::encode(wrapper)
                    )
                })?;
            balance_deltas.push(BalanceDelta {
                ord: u64::MAX,
                tx: Some(last_tx.clone()),
                token: underlying.clone(),
                delta: balance.to_signed_bytes_be(),
                component_id: hex::encode(wrapper).into_bytes(),
            });
        }
    }

    Ok(BlockBalanceDeltas { balance_deltas })
}

fn newly_tracked_wrappers(
    wrapper_store_deltas: StoreDeltas,
) -> Result<HashMap<Vec<u8>, Vec<u8>>, substreams::errors::Error> {
    wrapper_store_deltas
        .deltas
        .into_iter()
        .filter(|delta| delta.old_value.is_empty())
        .map(|delta| {
            let underlying_key = delta
                .key
                .strip_prefix("FewWrapper:")
                .ok_or_else(|| anyhow::anyhow!("Unexpected FewWrapper store key: {}", delta.key))?;
            Ok((hex::decode(underlying_key)?, delta.new_value))
        })
        .collect()
}

#[substreams::handlers::store]
pub fn store_wrapper_backings(deltas: BlockBalanceDeltas, store: StoreAddBigInt) {
    tycho_substreams::balances::store_balance_changes(deltas, store);
}
