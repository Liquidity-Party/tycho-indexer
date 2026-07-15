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
- Accumulate events from the statically configured fee modules in a Substreams store keyed by pool
  and attribute. The first event observed for a pool emits all five configuration fields together
  with `dynamic_fee_module`, explicitly replacing any stale database attributes left by a retired
  module. Later events emit only the fields changed by that event. Pools configured before the
  SPKG cutover receive their complete initial snapshot from the backfill described below. Store
  keys deliberately omit the module address because module rotation is handled as a static SPKG
  configuration upgrade plus backfill, not as a runtime transition between modules for the same
  pool.
- Add the `dynamic_fee_module` pool attribute as a version marker for downstream consumers. The
  corresponding Tycho Simulation support is released separately so this SPKG can be deployed and
  backfilled first.

### Migration and backfill

Updating the SPKG does not retroactively send its historical map outputs to an existing Tycho
Indexer cursor. Define cutover block `Y` as the last finalized block committed by the old SPKG.
The new package resumes at `Y + 1`. Its Substreams store can reconstruct the configured modules'
history through `Y`, but the Tycho database still lacks the corresponding entity changes from
before the cutover. A database backfill is therefore required.

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
  --expected-cutover-hash <CUTOVER_BLOCK_HASH> \
  > /tmp/slipstreams_dynamic_fee_scan.jsonl
```

The script uses the two fee modules configured by this package unless one or more `--module`
arguments are supplied. It discovers each module's deployment block with historical
`eth_getCode`, scans through the inclusive `--to-block`, and prints JSON Lines containing every
decoded configuration event followed by the reconstructed final snapshot for each touched pool.
The first JSONL row records the chain, module set, per-module scan start, inclusive cutover block,
and cutover block hash. This metadata proves the saved scan covers the requested migration range.
If the RPC is not archive-capable, or the deployment block is already known, pass a safe inclusive
start with `--from-block <DEPLOYMENT_OR_EARLIER_BLOCK>`.

To reuse that JSONL output and skip the expensive `eth_getLogs` scan:

```bash
BASE_RPC_URL=https://your-base-rpc.example \
python3 protocols/substreams/base-aerodrome-slipstreams/scripts/backfill_slipstreams_dynamic_fees.py \
  --to-block <CUTOVER_BLOCK_Y> \
  --expected-cutover-hash <CUTOVER_BLOCK_HASH> \
  --input-file /tmp/slipstreams_dynamic_fee_scan.jsonl \
  --sql-output /tmp/slipstreams_dynamic_fee_backfill.sql
```

`--input-file` validates that the saved chain, module set, and inclusive cutover block exactly
match the current arguments, then reads the saved `type=event` rows and replays them into the final
pool snapshots; the saved `type=pool_state` rows are ignored. Legacy JSONL files without
`type=scan_metadata` are rejected and must be regenerated. Log scanning is completely skipped. An
RPC is still required with `--sql-output` to verify that the cutover is finalized and its recorded
block hash is still canonical, validate every implicitly discovered scan start against the module
deployment block, and fetch block and transaction metadata for each pool's last configuration
transaction. If the original scan used an explicit `--from-block`, repeat the same argument when
reusing the file; the saved and requested starts must match exactly.

Alternatively, generate the database migration during the initial scan:

```bash
BASE_RPC_URL=https://your-base-rpc.example \
python3 protocols/substreams/base-aerodrome-slipstreams/scripts/backfill_slipstreams_dynamic_fees.py \
  --to-block <CUTOVER_BLOCK_Y> \
  --expected-cutover-hash <CUTOVER_BLOCK_HASH> \
  --sql-output /tmp/slipstreams_dynamic_fee_backfill.sql
```

The generated SQL is transactional and idempotent. It stages the reconstructed state, inserts a
missing event block or transaction before resolving `modify_tx`, and resolves each
`protocol_component_id` from the `base` chain, the `aerodrome_slipstreams` protocol system, and the
pool address stored in `protocol_component.external_id`. It versions `dynamic_fee_module` and all
five `dfc_*` attributes together by conditionally upserting their final snapshot into
`protocol_state_default`. This is a current-state repair, not a historical replay: it does not
create or modify archived `protocol_state` versions. Existing rows whose `modify_tx` is at or
before `Y` are replaced even when a retired-module update is later than the current module's last
event. Rows written after `Y` are preserved, including updates committed concurrently while the
SQL runs. Contract-creation transactions have no recipient and are stored with an empty `to` byte
array, matching the indexer's normal transaction insertion path. The SQL aborts if any staged row
cannot be resolved, so its staged and resolved row counts should be reviewed before applying it.

Use a non-overlapping cutover boundary: stop the old extractor, read its last committed finalized
block `Y` from `extraction_state`, and confirm that value is stable. The backfill owns state through
block `Y`, and the new SPKG stream starts at `Y + 1`. Apply the backfill while the extractor is
stopped for the simplest operational rollout; the generated SQL also protects rows whose
`modify_tx` is after `Y`. Start the new SPKG only after the backfill commits, verify its resolved
start block is `Y + 1`, and deploy the corresponding Tycho Simulation release last. The script
also rejects `Y` when it is above the Base RPC's `finalized` block or when the canonical RPC hash
does not equal `--expected-cutover-hash` copied from `extraction_state`.

After stopping the old extractor, query the cutover twice and continue only when the number and
hash no longer change:

```sql
SELECT es.name,
       b.number AS cutover_block,
       '0x' || encode(b.hash, 'hex') AS cutover_block_hash,
       es.modified_ts
FROM extraction_state es
JOIN chain c ON c.id = es.chain_id
JOIN "block" b ON b.id = es.block_id
WHERE c.name = 'base'
  AND es.name = 'aerodrome_slipstreams'
  AND b.main = TRUE;
```
