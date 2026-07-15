use crate::modules::utils::{
    dynamic_fee_config_initialized_key, dynamic_fee_config_key, DynamicFeeEvent, Params,
};
use substreams::{
    scalar::BigInt,
    store::{StoreNew, StoreSet, StoreSetBigInt},
};
use substreams_ethereum::pb::eth::v2 as eth;

// Earliest deployment among the configured fee modules:
// - 0x090b2a6bb475c00e2256e2095a60887cd710803b at block 44_221_569
// - 0xf4ecd78ebeb6d36cf7f80b5b6b41453515fe2785 at block 44_221_840
const FIRST_DYNAMIC_FEE_MODULE_DEPLOYMENT_BLOCK: u64 = 44_221_569;

fn should_process_dynamic_fee_config(block_number: u64) -> bool {
    block_number >= FIRST_DYNAMIC_FEE_MODULE_DEPLOYMENT_BLOCK
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
    store.set(ordinal, dynamic_fee_config_key(pool, attribute), value);
}

#[substreams::handlers::store]
pub fn store_dynamic_fee_config(params: String, block: eth::Block, store: StoreSetBigInt) {
    if !should_process_dynamic_fee_config(block.number) {
        return;
    }

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
            let pool = event.pool();
            store.set(log.ordinal, dynamic_fee_config_initialized_key(pool), &BigInt::from(1));
            for (attribute, value) in event.config_updates() {
                set_config_value(&store, log.ordinal, pool, attribute, &value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::should_process_dynamic_fee_config;

    #[test]
    fn starts_processing_at_the_first_configured_fee_module_deployment() {
        assert!(!should_process_dynamic_fee_config(44_221_568));
        assert!(should_process_dynamic_fee_config(44_221_569));
    }
}
