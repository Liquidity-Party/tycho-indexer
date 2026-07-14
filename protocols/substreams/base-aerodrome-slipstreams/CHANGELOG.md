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

`protocols/substreams/base-aerodrome-slipstreams/scripts/backfill_slipstreams_dynamic_fees.py`
implements this scan and reconstruction.

Run it from the repository root with an archive-capable Base RPC:

```bash
BASE_RPC_URL=https://your-base-rpc.example \
python3 protocols/substreams/base-aerodrome-slipstreams/scripts/backfill_slipstreams_dynamic_fees.py \
  --to-block <CUTOVER_BLOCK_Y> \
  > /tmp/slipstreams_dynamic_fee_scan.jsonl
```

The script uses the two fee modules configured by this package unless one or more `--module`
arguments are supplied. It discovers each module's deployment block with historical
`eth_getCode`, scans through the inclusive `--to-block`, and prints JSON Lines containing every
decoded configuration event followed by the reconstructed final snapshot for each touched pool.
If the RPC is not archive-capable, or the deployment block is already known, pass a safe inclusive
start with `--from-block <DEPLOYMENT_OR_EARLIER_BLOCK>`.

To reuse that JSONL output and skip the expensive `eth_getLogs` scan:

```bash
BASE_RPC_URL=https://your-base-rpc.example \
python3 protocols/substreams/base-aerodrome-slipstreams/scripts/backfill_slipstreams_dynamic_fees.py \
  --to-block <CUTOVER_BLOCK_Y> \
  --input-file /tmp/slipstreams_dynamic_fee_scan.jsonl \
  --sql-output /tmp/slipstreams_dynamic_fee_backfill.sql
```

`--input-file` reads the saved `type=event` rows and replays them into the final pool snapshots;
the saved `type=pool_state` rows are ignored. Log scanning is completely skipped. An RPC is still
required with `--sql-output`, but only to fetch block and transaction metadata for each pool's last
configuration transaction.

Alternatively, generate the database migration during the initial scan:

```bash
BASE_RPC_URL=https://your-base-rpc.example \
python3 protocols/substreams/base-aerodrome-slipstreams/scripts/backfill_slipstreams_dynamic_fees.py \
  --to-block <CUTOVER_BLOCK_Y> \
  --sql-output /tmp/slipstreams_dynamic_fee_backfill.sql
```

The generated SQL is transactional and idempotent. It stages the reconstructed state, inserts a
missing event block or transaction before resolving `modify_tx`, and resolves each
`protocol_component_id` from the `base` chain, the `aerodrome_slipstreams` protocol system, and the
pool address stored in `protocol_component.external_id`. It versions `dynamic_fee_module` and all
five `dfc_*` attributes together by conditionally upserting their final snapshot into
`protocol_state_default`. This is a current-state repair, not a historical replay: it does not
create or modify archived `protocol_state` versions. If a newer streamed version already exists,
that row is skipped. Contract-creation transactions have no recipient and are stored with an empty
`to` byte array, matching the indexer's normal transaction insertion path. The SQL aborts if any
staged row cannot be resolved, so its staged and resolved row counts should be reviewed before
applying it.

Use a non-overlapping cutover boundary: the backfill owns state through block `Y`, and the new SPKG
stream owns updates after block `Y`. If indexing continues while the backfill runs, database writes
must be conditional so the block-`Y` snapshot cannot overwrite newer streamed updates.
