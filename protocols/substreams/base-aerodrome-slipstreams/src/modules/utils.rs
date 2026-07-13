use crate::abi::dynamic_swap_fee_module::events::{
    CustomFeeSet, DynamicFeeReset, FeeCapSet, InitialFeeDisabled, InitialFeeSet, ScalingFactorSet,
};
use anyhow::{anyhow, Result};
use serde::Deserialize;
use substreams_ethereum::{pb::eth::v2 as eth, Event};

pub enum DynamicFeeEvent {
    CustomFeeSet(CustomFeeSet),
    ScalingFactorSet(ScalingFactorSet),
    FeeCapSet(FeeCapSet),
    InitialFeeSet(InitialFeeSet),
    InitialFeeDisabled(InitialFeeDisabled),
    DynamicFeeReset(DynamicFeeReset),
}

impl DynamicFeeEvent {
    pub fn match_and_decode(log: &eth::Log) -> Option<Self> {
        if let Some(event) = CustomFeeSet::match_and_decode(log) {
            Some(Self::CustomFeeSet(event))
        } else if let Some(event) = ScalingFactorSet::match_and_decode(log) {
            Some(Self::ScalingFactorSet(event))
        } else if let Some(event) = FeeCapSet::match_and_decode(log) {
            Some(Self::FeeCapSet(event))
        } else if let Some(event) = InitialFeeSet::match_and_decode(log) {
            Some(Self::InitialFeeSet(event))
        } else if let Some(event) = InitialFeeDisabled::match_and_decode(log) {
            Some(Self::InitialFeeDisabled(event))
        } else {
            DynamicFeeReset::match_and_decode(log).map(Self::DynamicFeeReset)
        }
    }

    pub fn pool(&self) -> &[u8] {
        match self {
            Self::CustomFeeSet(event) => &event.pool,
            Self::ScalingFactorSet(event) => &event.pool,
            Self::FeeCapSet(event) => &event.pool,
            Self::InitialFeeSet(event) => &event.pool,
            Self::InitialFeeDisabled(event) => &event.pool,
            Self::DynamicFeeReset(event) => &event.pool,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Params {
    pub factories: Vec<String>,
    pub dynamic_fee_modules: Vec<String>,
}

impl Params {
    pub fn parse_from_query(input: &str) -> Result<Self> {
        serde_qs::from_str(input).map_err(|e| anyhow!("Failed to parse query params: {}", e))
    }
}
