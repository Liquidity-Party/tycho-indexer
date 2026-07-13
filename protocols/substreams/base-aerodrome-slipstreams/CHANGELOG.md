# Changelog

## v0.1.3

### Changed

- Replace the configured dynamic swap fee modules with the current Base and Aerodrome
  deployments:
  - `0x090b2A6bb475c00e2256e2095A60887cD710803b`
  - `0xF4Ecd78EBEB6d36CF7f80B5B6B41453515fe2785`
- Keep fee module selection explicit in the SPKG parameters instead of following Factory module
  changes dynamically. A future module rotation requires updating both the SPKG configuration and
  the Tycho Simulation allowlist.
- Add support for the upgraded dynamic fee configuration fields
  `dfc_initialFeeEnabled` and `dfc_initialFee`, including the corresponding set, disable, and
  reset events.
- Accumulate dynamic fee events in a Substreams store and emit a complete five-field dynamic fee
  configuration on every event from a configured module. This prevents attributes left by a
  previous module from being combined with a partial update from the current module.
- Add the `dynamic_fee_module` pool attribute as a version marker. Tycho Simulation only applies
  dynamic fee attributes when this marker matches one of the two configured modules above;
  missing or unsupported markers fall back to the module defaults.
- Update Tycho Simulation for the upgraded module's initial-fee behavior and use `30_000` as the
  default fee cap.

### Migration and backfill

Updating the SPKG does not retroactively send its historical map outputs to an existing Tycho
Indexer cursor. If the new package starts streaming at cutover block `Y`, the Substreams store can
reconstruct the configured modules' history before `Y`, but the Tycho database still lacks the
corresponding entity changes from before the cutover. A database backfill is therefore required.

The backfill must restore each affected pool's complete configuration as of block `Y`; it must not
only replay events between the Factory module switch at block `X` and the SPKG cutover. A module
can be configured before it becomes active. For example,
`0xF4Ecd78EBEB6d36CF7f80B5B6B41453515fe2785` first emitted a configuration update at block
`44_227_070`, before the Factory selected it at block `44_228_401`.

For each configured module, the backfill should:

1. Scan from the module deployment block, or its first relevant configuration block `D`, through
   cutover block `Y` to discover every pool touched by a dynamic fee event.
2. Reconstruct or query each discovered pool's complete configuration at the end of block `Y`.
3. Write `dynamic_fee_module`, `dfc_baseFee`, `dfc_scalingFactor`, `dfc_feeCap`,
   `dfc_initialFeeEnabled`, and `dfc_initialFee` together.
4. Leave pools that have never been configured in the new module unchanged. Their missing or old
   module marker makes Tycho Simulation ignore stale attributes and use the default fee behavior.

Use a non-overlapping cutover boundary: the backfill owns state through block `Y`, and the new SPKG
stream owns updates after block `Y`. If indexing continues while the backfill runs, database writes
must be conditional so the block-`Y` snapshot cannot overwrite newer streamed updates.
