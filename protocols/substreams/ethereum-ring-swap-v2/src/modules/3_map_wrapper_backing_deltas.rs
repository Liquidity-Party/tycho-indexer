use std::collections::HashMap;

use substreams::{
    pb::substreams::StoreDeltas,
    prelude::{BigInt, StoreAdd},
    store::{StoreAddBigInt, StoreGet, StoreGetRaw, StoreNew},
};
use substreams_ethereum::{pb::eth::v2 as eth, Event};

use crate::store_key::StoreKey;
use tycho_substreams::{
    abi::{erc20, weth},
    prelude::*,
};

/// Tracks the global underlying ERC-20 balance held by each official FewToken wrapper.
///
/// Pair reserves measure FewTokens, while wrapper backing bounds how many FewTokens can actually be
/// unwrapped. These values are accumulated globally before being projected onto every Ring pool
/// that uses the underlying token.
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

            // A new wrapper is snapshotted at end-of-block below. Applying this block's events as
            // well would count the same backing movement twice.
            if new_wrappers.contains_key(&token) {
                continue;
            }

            let token_key = StoreKey::FewWrapper.get_unique_key(&hex::encode(&token));
            let Some(wrapper) = wrapper_store.get_last(&token_key) else {
                continue;
            };
            let Some(delta) = event_delta(log, &wrapper) else {
                continue;
            };

            balance_deltas.push(BalanceDelta {
                ord: log.ordinal,
                tx: Some(transaction.clone()),
                token,
                delta: delta.to_signed_bytes_be(),
                // This is a global wrapper balance. It becomes component-scoped in the next map.
                component_id: vec![],
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
                component_id: vec![],
            });
        }
    }

    balance_deltas.sort_unstable_by_key(|delta| delta.ord);
    Ok(BlockBalanceDeltas { balance_deltas })
}

/// Returns the signed balance change a log applies to a wrapper's underlying balance.
///
/// Canonical WETH changes balances through `Deposit` and `Withdrawal` without ERC-20 `Transfer`
/// logs, so both event families must be handled.
fn event_delta(log: &eth::Log, wrapper: &[u8]) -> Option<BigInt> {
    if let Some(erc20::events::Transfer { from, to, value }) =
        erc20::events::Transfer::match_and_decode(log)
    {
        let mut delta = BigInt::zero();
        if from.as_slice() == wrapper {
            delta = delta - value.clone();
        }
        if to.as_slice() == wrapper {
            delta = delta + value;
        }
        return (delta != BigInt::zero()).then_some(delta);
    }
    if let Some(weth::events::Deposit { dst, wad }) = weth::events::Deposit::match_and_decode(log) {
        return (dst.as_slice() == wrapper).then_some(wad);
    }
    if let Some(weth::events::Withdrawal { src, wad }) =
        weth::events::Withdrawal::match_and_decode(log)
    {
        return (src.as_slice() == wrapper).then_some(BigInt::zero() - wad);
    }
    None
}

fn newly_tracked_wrappers(
    wrapper_store_deltas: StoreDeltas,
) -> Result<HashMap<Vec<u8>, Vec<u8>>, substreams::errors::Error> {
    let prefix = format!("{}:", StoreKey::FewWrapper.unique_id());
    wrapper_store_deltas
        .deltas
        .into_iter()
        .filter(|delta| delta.old_value.is_empty())
        .map(|delta| {
            let underlying_key = delta
                .key
                .strip_prefix(&prefix)
                .ok_or_else(|| anyhow::anyhow!("Unexpected FewWrapper store key: {}", delta.key))?;
            Ok((hex::decode(underlying_key)?, delta.new_value))
        })
        .collect()
}

#[substreams::handlers::store]
pub fn store_wrapper_backings(deltas: BlockBalanceDeltas, store: StoreAddBigInt) {
    for delta in deltas.balance_deltas {
        store.add(delta.ord, hex::encode(delta.token), BigInt::from_signed_bytes_be(&delta.delta));
    }
}

#[cfg(test)]
mod tests {
    use substreams::hex;

    use super::*;

    const TRANSFER_TOPIC: [u8; 32] =
        hex!("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");
    const DEPOSIT_TOPIC: [u8; 32] =
        hex!("e1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c");
    const WITHDRAWAL_TOPIC: [u8; 32] =
        hex!("7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65");

    fn wrapper() -> Vec<u8> {
        vec![1; 20]
    }

    fn other() -> Vec<u8> {
        vec![2; 20]
    }

    fn topic_word(address: &[u8]) -> Vec<u8> {
        let mut word = vec![0; 32];
        word[12..].copy_from_slice(address);
        word
    }

    fn amount_word(value: u64) -> Vec<u8> {
        let mut word = vec![0; 32];
        word[24..].copy_from_slice(&value.to_be_bytes());
        word
    }

    fn transfer_log(from: &[u8], to: &[u8], value: u64) -> eth::Log {
        eth::Log {
            topics: vec![TRANSFER_TOPIC.to_vec(), topic_word(from), topic_word(to)],
            data: amount_word(value),
            ..Default::default()
        }
    }

    fn deposit_log(dst: &[u8], value: u64) -> eth::Log {
        eth::Log {
            topics: vec![DEPOSIT_TOPIC.to_vec(), topic_word(dst)],
            data: amount_word(value),
            ..Default::default()
        }
    }

    fn withdrawal_log(src: &[u8], value: u64) -> eth::Log {
        eth::Log {
            topics: vec![WITHDRAWAL_TOPIC.to_vec(), topic_word(src)],
            data: amount_word(value),
            ..Default::default()
        }
    }

    #[test]
    fn transfer_events_track_wrapper_balance_in_both_directions() {
        assert_eq!(
            event_delta(&transfer_log(&other(), &wrapper(), 50), &wrapper()),
            Some(BigInt::from(50))
        );
        assert_eq!(
            event_delta(&transfer_log(&wrapper(), &other(), 50), &wrapper()),
            Some(BigInt::from(-50))
        );
    }

    #[test]
    fn self_transfer_does_not_change_backing() {
        assert_eq!(event_delta(&transfer_log(&wrapper(), &wrapper(), 50), &wrapper()), None);
    }

    #[test]
    fn weth_deposit_and_withdrawal_track_wrapper_balance() {
        assert_eq!(event_delta(&deposit_log(&wrapper(), 70), &wrapper()), Some(BigInt::from(70)));
        assert_eq!(
            event_delta(&withdrawal_log(&wrapper(), 70), &wrapper()),
            Some(BigInt::from(-70))
        );
    }

    #[test]
    fn unrelated_events_are_ignored() {
        assert_eq!(event_delta(&transfer_log(&other(), &other(), 50), &wrapper()), None);
        assert_eq!(event_delta(&deposit_log(&other(), 70), &wrapper()), None);
        assert_eq!(event_delta(&withdrawal_log(&other(), 70), &wrapper()), None);
    }
}
