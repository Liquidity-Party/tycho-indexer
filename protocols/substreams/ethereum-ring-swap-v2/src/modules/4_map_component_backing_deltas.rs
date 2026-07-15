use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use substreams::{
    prelude::BigInt,
    store::{StoreAddBigInt, StoreGet, StoreGetBigInt, StoreGetString, StoreNew},
};

use tycho_substreams::prelude::*;

/// Projects global FewToken backing changes onto every Ring component that uses the underlying
/// token. New pools are seeded from the accumulated global backing store so both component token
/// balances exist even when neither token moved in the pool-creation block.
#[substreams::handlers::map]
pub fn map_component_backing_deltas(
    pools_created: BlockChanges,
    wrapper_backing_deltas: BlockBalanceDeltas,
    wrapper_backing_store: StoreGetBigInt,
    token_pools_store: StoreGetString,
) -> Result<BlockBalanceDeltas, substreams::errors::Error> {
    Ok(project_component_backing_deltas(
        pools_created,
        wrapper_backing_deltas,
        &wrapper_backing_store,
        &token_pools_store,
    ))
}

fn project_component_backing_deltas(
    pools_created: BlockChanges,
    wrapper_backing_deltas: BlockBalanceDeltas,
    wrapper_backing_store: &impl StoreGet<BigInt>,
    token_pools_store: &impl StoreGet<String>,
) -> BlockBalanceDeltas {
    let mut balance_deltas = Vec::new();
    let mut new_component_ids_by_token = HashMap::<Vec<u8>, HashSet<String>>::new();

    for changes in pools_created.changes {
        let Some(transaction) = changes.tx else {
            continue;
        };

        for component in changes.component_changes {
            for token in component.tokens {
                new_component_ids_by_token
                    .entry(token.clone())
                    .or_default()
                    .insert(component.id.clone());

                let balance = wrapper_backing_store
                    .get_last(hex::encode(&token))
                    .unwrap_or_else(BigInt::zero);
                balance_deltas.push(BalanceDelta {
                    ord: transaction.index,
                    tx: Some(transaction.clone()),
                    token,
                    delta: balance.to_signed_bytes_be(),
                    component_id: component.id.as_bytes().to_vec(),
                });
            }
        }
    }

    // Fan global wrapper movements out to all existing pools. Pools created in this block already
    // received the end-of-block backing snapshot above and must not receive the delta twice.
    for wrapper_delta in wrapper_backing_deltas.balance_deltas {
        let token_key = hex::encode(&wrapper_delta.token);
        let Some(component_ids) = token_pools_store.get_last(&token_key) else {
            continue;
        };
        let new_component_ids = new_component_ids_by_token.get(&wrapper_delta.token);

        for component_id in component_ids
            .split(';')
            .filter(|component_id| !component_id.is_empty())
            .unique()
        {
            if new_component_ids.is_some_and(|ids| ids.contains(component_id)) {
                continue;
            }

            balance_deltas.push(BalanceDelta {
                ord: wrapper_delta.ord,
                tx: wrapper_delta.tx.clone(),
                token: wrapper_delta.token.clone(),
                delta: wrapper_delta.delta.clone(),
                component_id: component_id.as_bytes().to_vec(),
            });
        }
    }

    balance_deltas.sort_unstable_by_key(|delta| delta.ord);
    BlockBalanceDeltas { balance_deltas }
}

#[substreams::handlers::store]
pub fn store_component_backings(deltas: BlockBalanceDeltas, store: StoreAddBigInt) {
    tycho_substreams::balances::store_balance_changes(deltas, store);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct MockBigIntStore(HashMap<String, BigInt>);

    impl StoreGet<BigInt> for MockBigIntStore {
        fn new(_size: u32) -> Self {
            Self::default()
        }

        fn get_at<K: AsRef<str>>(&self, _ord: u64, key: K) -> Option<BigInt> {
            self.get_last(key)
        }

        fn get_first<K: AsRef<str>>(&self, key: K) -> Option<BigInt> {
            self.get_last(key)
        }

        fn get_last<K: AsRef<str>>(&self, key: K) -> Option<BigInt> {
            self.0.get(key.as_ref()).cloned()
        }

        fn has_at<K: AsRef<str>>(&self, _ord: u64, key: K) -> bool {
            self.has_last(key)
        }

        fn has_first<K: AsRef<str>>(&self, key: K) -> bool {
            self.has_last(key)
        }

        fn has_last<K: AsRef<str>>(&self, key: K) -> bool {
            self.0.contains_key(key.as_ref())
        }
    }

    #[derive(Default)]
    struct MockStringStore(HashMap<String, String>);

    impl StoreGet<String> for MockStringStore {
        fn new(_size: u32) -> Self {
            Self::default()
        }

        fn get_at<K: AsRef<str>>(&self, _ord: u64, key: K) -> Option<String> {
            self.get_last(key)
        }

        fn get_first<K: AsRef<str>>(&self, key: K) -> Option<String> {
            self.get_last(key)
        }

        fn get_last<K: AsRef<str>>(&self, key: K) -> Option<String> {
            self.0.get(key.as_ref()).cloned()
        }

        fn has_at<K: AsRef<str>>(&self, _ord: u64, key: K) -> bool {
            self.has_last(key)
        }

        fn has_first<K: AsRef<str>>(&self, key: K) -> bool {
            self.has_last(key)
        }

        fn has_last<K: AsRef<str>>(&self, key: K) -> bool {
            self.0.contains_key(key.as_ref())
        }
    }

    fn transaction(index: u64) -> Transaction {
        Transaction { index, hash: vec![index as u8], ..Default::default() }
    }

    fn pool_created(id: &str, tokens: Vec<Vec<u8>>) -> BlockChanges {
        BlockChanges {
            changes: vec![TransactionChanges {
                tx: Some(transaction(7)),
                component_changes: vec![ProtocolComponent {
                    id: id.to_string(),
                    tokens,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn global_delta(token: Vec<u8>, value: i64) -> BalanceDelta {
        BalanceDelta {
            ord: 10,
            tx: Some(transaction(10)),
            token,
            delta: BigInt::from(value).to_signed_bytes_be(),
            component_id: vec![],
        }
    }

    #[test]
    fn new_pool_is_seeded_with_both_wrapper_backings() {
        let token0 = vec![1; 20];
        let token1 = vec![2; 20];
        let backing_store = MockBigIntStore(HashMap::from([
            (hex::encode(&token0), BigInt::from(100)),
            (hex::encode(&token1), BigInt::from(200)),
        ]));

        let deltas = project_component_backing_deltas(
            pool_created("pool-a", vec![token0.clone(), token1.clone()]),
            BlockBalanceDeltas::default(),
            &backing_store,
            &MockStringStore::default(),
        );

        assert_eq!(deltas.balance_deltas.len(), 2);
        assert!(deltas
            .balance_deltas
            .iter()
            .any(|delta| {
                delta.token == token0 &&
                    BigInt::from_signed_bytes_be(&delta.delta) == BigInt::from(100)
            }));
        assert!(deltas
            .balance_deltas
            .iter()
            .any(|delta| {
                delta.token == token1 &&
                    BigInt::from_signed_bytes_be(&delta.delta) == BigInt::from(200)
            }));
        assert!(deltas
            .balance_deltas
            .iter()
            .all(|delta| delta.component_id == b"pool-a"));
    }

    #[test]
    fn backing_change_fans_out_once_to_every_existing_pool() {
        let token = vec![1; 20];
        let token_pools = MockStringStore(HashMap::from([(
            hex::encode(&token),
            "pool-a;pool-b;pool-a".to_string(),
        )]));

        let deltas = project_component_backing_deltas(
            BlockChanges::default(),
            BlockBalanceDeltas { balance_deltas: vec![global_delta(token, 5)] },
            &MockBigIntStore::default(),
            &token_pools,
        );
        let component_ids = deltas
            .balance_deltas
            .iter()
            .map(|delta| String::from_utf8(delta.component_id.clone()).unwrap())
            .collect::<HashSet<_>>();

        assert_eq!(deltas.balance_deltas.len(), 2);
        assert_eq!(component_ids, HashSet::from(["pool-a".to_string(), "pool-b".to_string()]));
    }

    #[test]
    fn new_pool_snapshot_is_not_double_counted_by_same_block_delta() {
        let token0 = vec![1; 20];
        let token1 = vec![2; 20];
        let backing_store = MockBigIntStore(HashMap::from([
            (hex::encode(&token0), BigInt::from(100)),
            (hex::encode(&token1), BigInt::from(200)),
        ]));
        let token_pools =
            MockStringStore(HashMap::from([(hex::encode(&token0), "pool-a;pool-old".to_string())]));

        let deltas = project_component_backing_deltas(
            pool_created("pool-a", vec![token0.clone(), token1]),
            BlockBalanceDeltas { balance_deltas: vec![global_delta(token0.clone(), 5)] },
            &backing_store,
            &token_pools,
        );

        assert_eq!(
            deltas
                .balance_deltas
                .iter()
                .filter(|delta| delta.component_id == b"pool-a" && delta.token == token0)
                .count(),
            1
        );
        assert!(deltas
            .balance_deltas
            .iter()
            .any(|delta| delta.component_id == b"pool-old"));
    }
}
