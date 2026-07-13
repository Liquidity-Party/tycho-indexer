use crate::modules::utils::{DynamicFeeEvent, Params};
use substreams::{
    scalar::BigInt,
    store::{StoreNew, StoreSet, StoreSetBigInt},
};
use substreams_ethereum::pb::eth::v2 as eth;
use substreams_helper::hex::Hexable;

fn config_key(pool: &[u8], attribute: &str) -> String {
    format!("{}:{attribute}", pool.to_hex())
}

fn set_config_value(
    store: &StoreSetBigInt,
    ordinal: u64,
    pool: &[u8],
    attribute: &str,
    value: &BigInt,
) {
    // Keep the write at the event ordinal so maps processing another event in the same block see
    // exactly the configuration that was active at that point in the transaction stream.
    store.set(ordinal, config_key(pool, attribute), value);
}

#[substreams::handlers::store]
pub fn store_dynamic_fee_config(params: String, block: eth::Block, store: StoreSetBigInt) {
    let params = Params::parse_from_query(&params).expect("Invalid module parameters");
    let dynamic_fee_modules = params
        .dynamic_fee_modules
        .iter()
        .map(|module| hex::decode(module).expect("Invalid dynamic_fee_module hex"))
        .collect::<Vec<_>>();

    for transaction in block.transactions() {
        for (log, _) in transaction.logs_with_calls() {
            if !dynamic_fee_modules.contains(&log.address) {
                continue;
            }

            let Some(event) = DynamicFeeEvent::match_and_decode(log) else {
                continue;
            };
            match event {
                DynamicFeeEvent::CustomFeeSet(event) => {
                    set_config_value(&store, log.ordinal, &event.pool, "dfc_baseFee", &event.fee);
                }
                DynamicFeeEvent::ScalingFactorSet(event) => set_config_value(
                    &store,
                    log.ordinal,
                    &event.pool,
                    "dfc_scalingFactor",
                    &event.scaling_factor,
                ),
                DynamicFeeEvent::FeeCapSet(event) => {
                    set_config_value(&store, log.ordinal, &event.pool, "dfc_feeCap", &event.fee_cap)
                }
                DynamicFeeEvent::InitialFeeSet(event) => {
                    set_config_value(
                        &store,
                        log.ordinal,
                        &event.pool,
                        "dfc_initialFeeEnabled",
                        &BigInt::from(1),
                    );
                    set_config_value(
                        &store,
                        log.ordinal,
                        &event.pool,
                        "dfc_initialFee",
                        &event.initial_fee,
                    );
                }
                DynamicFeeEvent::InitialFeeDisabled(event) => {
                    set_config_value(
                        &store,
                        log.ordinal,
                        &event.pool,
                        "dfc_initialFeeEnabled",
                        &BigInt::from(0),
                    );
                    set_config_value(
                        &store,
                        log.ordinal,
                        &event.pool,
                        "dfc_initialFee",
                        &BigInt::from(0),
                    );
                }
                DynamicFeeEvent::DynamicFeeReset(event) => {
                    for attribute in [
                        "dfc_scalingFactor",
                        "dfc_feeCap",
                        "dfc_initialFeeEnabled",
                        "dfc_initialFee",
                    ] {
                        set_config_value(
                            &store,
                            log.ordinal,
                            &event.pool,
                            attribute,
                            &BigInt::from(0),
                        );
                    }
                }
            }
        }
    }
}
