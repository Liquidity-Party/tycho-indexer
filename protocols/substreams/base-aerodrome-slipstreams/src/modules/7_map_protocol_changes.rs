use crate::{
    events::get_log_changed_attributes,
    modules::utils::{DynamicFeeEvent, Params},
    pb::tycho::evm::aerodrome::Pool,
};

use itertools::Itertools;
use std::{collections::HashMap, vec};
use substreams::{
    pb::substreams::StoreDeltas,
    store::{StoreGet, StoreGetBigInt, StoreGetProto},
};
use substreams_ethereum::pb::eth::v2::{self as eth};
use substreams_helper::hex::Hexable;
use tycho_substreams::{balances::aggregate_balances_changes, prelude::*};

#[substreams::handlers::map]
pub fn map_protocol_changes(
    params: String,
    block: eth::Block,
    protocol_components: BlockChanges,
    pools_store: StoreGetProto<Pool>,
    dynamic_fee_config_store: StoreGetBigInt,
    balance_store: StoreDeltas,
    balance_deltas: BlockBalanceDeltas,
) -> Result<BlockChanges, substreams::errors::Error> {
    let params = Params::parse_from_query(&params)?;
    let dynamic_fee_modules = params
        .dynamic_fee_modules
        .iter()
        .map(|f| hex::decode(f).expect("Invalid dynamic_fee_module hex"))
        .collect::<Vec<Vec<u8>>>();
    let mut transaction_changes: HashMap<_, TransactionChangesBuilder> = HashMap::new();

    for change in protocol_components.changes.into_iter() {
        let tx = change.tx.as_ref().unwrap();
        let builder = transaction_changes
            .entry(tx.index)
            .or_insert_with(|| TransactionChangesBuilder::new(tx));
        change
            .component_changes
            .iter()
            .for_each(|c| {
                builder.add_protocol_component(c);
            });
        change
            .entity_changes
            .iter()
            .for_each(|c| {
                builder.add_entity_change(c);
            });
    }

    aggregate_balances_changes(balance_store, balance_deltas)
        .into_iter()
        .for_each(|(_, (tx, balances))| {
            let builder = transaction_changes
                .entry(tx.index)
                .or_insert_with(|| TransactionChangesBuilder::new(&tx));
            balances
                .values()
                .for_each(|token_bc_map| {
                    token_bc_map
                        .values()
                        .for_each(|bc| builder.add_balance_change(bc))
                });
        });

    for trx in block.transactions() {
        let tx = Transaction {
            to: trx.to.clone(),
            from: trx.from.clone(),
            hash: trx.hash.clone(),
            index: trx.index.into(),
        };
        let builder = transaction_changes
            .entry(tx.index)
            .or_insert_with(|| TransactionChangesBuilder::new(&tx));

        for (log, call_view) in trx.logs_with_calls() {
            if let Some(pool) = pools_store.get_last(format!("{}:{}", "Pool", log.address.to_hex()))
            {
                let changed_attributes = get_log_changed_attributes(
                    log,
                    &call_view.call.storage_changes,
                    pool.address
                        .clone()
                        .as_slice()
                        .try_into()
                        .expect("Pool address is not 20 bytes long"),
                );
                if !changed_attributes.is_empty() {
                    builder.add_entity_change(&EntityChanges {
                        component_id: pool.address.clone().to_hex(),
                        attributes: changed_attributes,
                    });
                }
            }
            if dynamic_fee_modules.contains(&log.address) {
                let mut handle_event = |pool: &[u8]| {
                    let pool_key = format!("Pool:{}", pool.to_hex());
                    if pools_store
                        .get_last(&pool_key)
                        .is_some()
                    {
                        // Every configured-module event publishes a complete versioned snapshot.
                        // Fields absent from the module store are zero-valued contract defaults.
                        let attributes = [
                            "dfc_baseFee",
                            "dfc_scalingFactor",
                            "dfc_feeCap",
                            "dfc_initialFeeEnabled",
                            "dfc_initialFee",
                        ]
                        .into_iter()
                        .map(|attribute| Attribute {
                            name: attribute.into(),
                            value: dynamic_fee_config_store
                                .get_at(log.ordinal, format!("{}:{attribute}", pool.to_hex()))
                                .unwrap_or_default()
                                .to_signed_bytes_be(),
                            change: ChangeType::Update.into(),
                        })
                        .chain(std::iter::once(Attribute {
                            name: "dynamic_fee_module".into(),
                            value: log.address.clone(),
                            change: ChangeType::Update.into(),
                        }))
                        .collect();
                        builder.add_entity_change(&EntityChanges {
                            component_id: pool.to_hex(),
                            attributes,
                        });
                    }
                };
                if let Some(event) = DynamicFeeEvent::match_and_decode(log) {
                    handle_event(event.pool());
                }
            }
        }
    }

    Ok(BlockChanges {
        block: Some((&block).into()),
        changes: transaction_changes
            .drain()
            .sorted_unstable_by_key(|(index, _)| *index)
            .filter_map(|(_, builder)| builder.build())
            .collect::<Vec<_>>(),
        storage_changes: vec![],
    })
}
