use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use substreams::{
    key,
    pb::substreams::StoreDeltas,
    prelude::BigInt,
    store::{StoreGet, StoreGetProto},
};
use substreams_ethereum::pb::eth::v2::{self as eth};

use substreams_helper::{event_handler::EventHandler, hex::Hexable};

use crate::{abi::pool::events::Sync, store_key::StoreKey, traits::PoolAddresser};
use tycho_substreams::{balances::aggregate_balances_changes, prelude::*};

#[substreams::handlers::map]
pub fn map_pool_events(
    block: eth::Block,
    block_entity_changes: BlockChanges,
    pools_store: StoreGetProto<ProtocolComponent>,
    wrapper_backing_deltas: BlockBalanceDeltas,
    wrapper_backing_store_deltas: StoreDeltas,
) -> Result<BlockChanges, substreams::errors::Error> {
    // Sync event is sufficient for our use-case. Since it's emitted on every reserve-altering
    // function call, we can use it as the only event to update the reserves of a pool.
    let mut tx_changes: HashMap<u64, TransactionChangesBuilder> = HashMap::new();

    merge_created_pools(block_entity_changes, &mut tx_changes);
    handle_sync(&block, &mut tx_changes, &pools_store);
    add_wrapper_backing_changes(
        wrapper_backing_store_deltas,
        wrapper_backing_deltas,
        &mut tx_changes,
    );

    Ok(BlockChanges {
        block: Some((&block).into()),
        changes: tx_changes
            .into_iter()
            .sorted_unstable_by_key(|(index, _)| *index)
            .filter_map(|(_, builder)| builder.build())
            .collect(),
        storage_changes: vec![],
    })
}

fn add_wrapper_backing_changes(
    wrapper_backing_store_deltas: StoreDeltas,
    wrapper_backing_deltas: BlockBalanceDeltas,
    tx_changes: &mut HashMap<u64, TransactionChangesBuilder>,
) {
    let created_wrappers = created_wrapper_ids(&wrapper_backing_store_deltas);
    for (_, (transaction, balances)) in
        aggregate_balances_changes(wrapper_backing_store_deltas, wrapper_backing_deltas)
    {
        let builder = tx_changes
            .entry(transaction.index)
            .or_insert_with(|| TransactionChangesBuilder::new(&transaction));

        for (wrapper_id, token_balances) in balances {
            let wrapper_id = String::from_utf8(wrapper_id)
                .expect("FewToken wrapper balance delta is not valid UTF-8");
            let wrapper = hex::decode(&wrapper_id)
                .expect("FewToken wrapper balance delta has an invalid wrapper address");
            let mut contract_change =
                InterimContractChange::new(&wrapper, created_wrappers.contains(&wrapper_id));
            for balance_change in token_balances.values() {
                contract_change
                    .upsert_token_balance(&balance_change.token, &balance_change.balance);
            }
            builder.add_contract_changes(&contract_change);
        }
    }
}

fn created_wrapper_ids(wrapper_backing_store_deltas: &StoreDeltas) -> HashSet<String> {
    wrapper_backing_store_deltas
        .deltas
        .iter()
        .filter(|delta| delta.old_value.is_empty())
        .map(|delta| key::segment_at(&delta.key, 0).to_string())
        .collect()
}

/// Handle the sync events and update the reserves of the pools.
///
/// This function is called for each block, and it will handle the sync events for each transaction.
/// Ring Swap V2 pairs are UniswapV2-style and emit Sync on every reserve-altering function call,
/// so we can use it as the only event to keep track of the pool state.
///
/// TransactionChangesBuilder consolidates duplicate reserve and balance updates per transaction, so
/// if a pool emits multiple Sync events in the same transaction only the final state is emitted.
fn handle_sync(
    block: &eth::Block,
    tx_changes: &mut HashMap<u64, TransactionChangesBuilder>,
    store: &StoreGetProto<ProtocolComponent>,
) {
    let mut on_sync = |event: Sync, _tx: &eth::TransactionTrace, _log: &eth::Log| {
        let pool_address_hex = _log.address.to_hex();

        let pool = store.must_get_last(StoreKey::Pool.get_unique_key(pool_address_hex.as_str()));
        // Ring pairs keep reserves in FewToken order while components expose the underlying
        // ERC-20s as tokens. Swap reserves when the exposed underlying token order differs from
        // the pair's FewToken order.
        let reserves = exposed_reserves(&pool, event.reserve0, event.reserve1);

        let transaction: Transaction = _tx.into();
        let builder = tx_changes
            .entry(transaction.index)
            .or_insert_with(|| TransactionChangesBuilder::new(&transaction));
        builder.add_entity_change(&EntityChanges {
            component_id: pool_address_hex.clone(),
            attributes: reserves
                .iter()
                .enumerate()
                .map(|(i, reserve)| Attribute {
                    name: format!("reserve{}", i),
                    value: reserve.clone().to_signed_bytes_be(),
                    change: ChangeType::Update.into(),
                })
                .collect(),
        });

        for (index, token) in pool.tokens.iter().enumerate() {
            let balance = &reserves[index];
            builder.add_balance_change(&BalanceChange {
                token: token.clone(),
                balance: balance.clone().to_signed_bytes_be(),
                component_id: pool_address_hex.as_bytes().to_vec(),
            });
        }
    };

    let mut eh = EventHandler::new(block);
    // Filter the sync events by the pool address, to make sure we don't process events for other
    // Protocols that use the same event signature.
    eh.filter_by_address(PoolAddresser { store });
    eh.on::<Sync, _>(&mut on_sync);
    eh.handle_events();
}

/// Convert raw pair reserves (FewToken order) into the reserve order of the exposed component
/// (sorted underlying ERC-20 order).
fn exposed_reserves(pool: &ProtocolComponent, reserve0: BigInt, reserve1: BigInt) -> [BigInt; 2] {
    if static_attribute_byte(pool, "reserves_inverted") == 1 {
        [reserve1, reserve0]
    } else {
        [reserve0, reserve1]
    }
}

/// Read a single-byte static attribute written by map_pools_created. Every pool stored by this
/// package is guaranteed to carry the Ring attributes, so a missing one is a bug, not a data case.
fn static_attribute_byte(pool: &ProtocolComponent, name: &str) -> u8 {
    pool.static_att
        .iter()
        .find(|att| att.name == name)
        .and_then(|att| att.value.last())
        .copied()
        .unwrap_or_else(|| panic!("Ring pool {} is missing the {} static attribute", pool.id, name))
}

fn merge_created_pools(
    block_entity_changes: BlockChanges,
    tx_changes: &mut HashMap<u64, TransactionChangesBuilder>,
) {
    for change in block_entity_changes.changes {
        let transaction = change.tx.as_ref().unwrap();
        let builder = tx_changes
            .entry(transaction.index)
            .or_insert_with(|| TransactionChangesBuilder::new(transaction));
        change
            .component_changes
            .iter()
            .for_each(|component| builder.add_protocol_component(component));
        change
            .entity_changes
            .iter()
            .for_each(|entity_change| builder.add_entity_change(entity_change));
        change
            .balance_changes
            .iter()
            .for_each(|balance_change| builder.add_balance_change(balance_change));
    }
}

#[cfg(test)]
mod tests {
    use substreams::pb::substreams::{StoreDelta, StoreDeltas};

    use super::*;

    fn ring_pool(attributes: &[(&str, u8)]) -> ProtocolComponent {
        ProtocolComponent {
            static_att: attributes
                .iter()
                .map(|(name, value)| Attribute {
                    name: name.to_string(),
                    value: vec![*value],
                    change: ChangeType::Creation.into(),
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn exposed_reserves_keeps_pair_order() {
        let pool = ring_pool(&[("reserves_inverted", 0)]);

        let reserves = exposed_reserves(&pool, BigInt::from(5u64), BigInt::from(2u64));

        assert_eq!(reserves, [BigInt::from(5u64), BigInt::from(2u64)]);
    }

    #[test]
    fn exposed_reserves_swaps_inverted_reserves() {
        let pool = ring_pool(&[("reserves_inverted", 1)]);

        let reserves = exposed_reserves(&pool, BigInt::from(7u64), BigInt::from(9u64));

        assert_eq!(reserves, [BigInt::from(9u64), BigInt::from(7u64)]);
    }

    #[test]
    fn first_backing_delta_creates_the_wrapper_account() {
        let deltas = StoreDeltas {
            deltas: vec![
                StoreDelta { key: "wrapper:token".to_string(), ..Default::default() },
                StoreDelta {
                    key: "existing:token".to_string(),
                    old_value: b"1".to_vec(),
                    new_value: b"2".to_vec(),
                    ..Default::default()
                },
            ],
        };

        let created = created_wrapper_ids(&deltas);

        assert_eq!(created, HashSet::from(["wrapper".to_string()]));
    }
}
