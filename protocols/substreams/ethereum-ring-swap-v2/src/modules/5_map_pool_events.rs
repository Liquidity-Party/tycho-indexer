use std::collections::HashMap;

use itertools::Itertools;
use substreams::{
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
    component_backing_deltas: BlockBalanceDeltas,
    component_backing_store_deltas: StoreDeltas,
) -> Result<BlockChanges, substreams::errors::Error> {
    let mut tx_changes: HashMap<u64, TransactionChangesBuilder> = HashMap::new();

    merge_created_pools(block_entity_changes, &mut tx_changes);
    handle_sync(&block, &mut tx_changes, &pools_store);
    add_component_backing_changes(
        component_backing_store_deltas,
        component_backing_deltas,
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

fn add_component_backing_changes(
    component_backing_store_deltas: StoreDeltas,
    component_backing_deltas: BlockBalanceDeltas,
    tx_changes: &mut HashMap<u64, TransactionChangesBuilder>,
) {
    for (_, (transaction, component_balances)) in
        aggregate_balances_changes(component_backing_store_deltas, component_backing_deltas)
    {
        let builder = tx_changes
            .entry(transaction.index)
            .or_insert_with(|| TransactionChangesBuilder::new(&transaction));

        for token_balances in component_balances.values() {
            for balance_change in token_balances.values() {
                builder.add_balance_change(balance_change);
            }
        }
    }
}

/// Sync updates the pair's FewToken reserves only. Solver-facing component balances are the
/// underlying balances held by the FewToken wrappers and are emitted by the backing pipeline.
fn handle_sync(
    block: &eth::Block,
    tx_changes: &mut HashMap<u64, TransactionChangesBuilder>,
    store: &StoreGetProto<ProtocolComponent>,
) {
    let mut on_sync = |event: Sync, tx: &eth::TransactionTrace, log: &eth::Log| {
        let pool_address_hex = log.address.to_hex();
        let pool = store.must_get_last(StoreKey::Pool.get_unique_key(&pool_address_hex));
        let transaction: Transaction = tx.into();
        let builder = tx_changes
            .entry(transaction.index)
            .or_insert_with(|| TransactionChangesBuilder::new(&transaction));
        builder.add_entity_change(&reserve_change(
            &pool,
            pool_address_hex,
            event.reserve0,
            event.reserve1,
        ));
    };

    let mut event_handler = EventHandler::new(block);
    event_handler.filter_by_address(PoolAddresser { store });
    event_handler.on::<Sync, _>(&mut on_sync);
    event_handler.handle_events();
}

fn reserve_change(
    pool: &ProtocolComponent,
    component_id: String,
    reserve0: BigInt,
    reserve1: BigInt,
) -> EntityChanges {
    let reserves = exposed_reserves(pool, reserve0, reserve1);
    EntityChanges {
        component_id,
        attributes: reserves
            .into_iter()
            .enumerate()
            .map(|(index, reserve)| Attribute {
                name: format!("reserve{index}"),
                value: reserve.to_signed_bytes_be(),
                change: ChangeType::Update.into(),
            })
            .collect(),
    }
}

/// Converts raw pair reserves (FewToken order) into solver-facing underlying token order.
fn exposed_reserves(pool: &ProtocolComponent, reserve0: BigInt, reserve1: BigInt) -> [BigInt; 2] {
    if static_attribute_byte(pool, "reserves_inverted") == 1 {
        [reserve1, reserve0]
    } else {
        [reserve0, reserve1]
    }
}

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
        let Some(transaction) = change.tx else {
            continue;
        };
        let builder = tx_changes
            .entry(transaction.index)
            .or_insert_with(|| TransactionChangesBuilder::new(&transaction));
        for component in &change.component_changes {
            builder.add_protocol_component(component);
        }
        for entity_change in &change.entity_changes {
            builder.add_entity_change(entity_change);
        }
        for balance_change in &change.balance_changes {
            builder.add_balance_change(balance_change);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ring_pool(reserves_inverted: u8) -> ProtocolComponent {
        ProtocolComponent {
            static_att: vec![Attribute {
                name: "reserves_inverted".to_string(),
                value: vec![reserves_inverted],
                change: ChangeType::Creation.into(),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn reserve_change_keeps_pair_order() {
        let change =
            reserve_change(&ring_pool(0), "pool".to_string(), BigInt::from(5), BigInt::from(2));

        assert_eq!(change.attributes[0].value, BigInt::from(5).to_signed_bytes_be());
        assert_eq!(change.attributes[1].value, BigInt::from(2).to_signed_bytes_be());
    }

    #[test]
    fn reserve_change_swaps_inverted_reserves_without_emitting_balances() {
        let change =
            reserve_change(&ring_pool(1), "pool".to_string(), BigInt::from(7), BigInt::from(9));

        assert_eq!(change.attributes[0].value, BigInt::from(9).to_signed_bytes_be());
        assert_eq!(change.attributes[1].value, BigInt::from(7).to_signed_bytes_be());
    }
}
