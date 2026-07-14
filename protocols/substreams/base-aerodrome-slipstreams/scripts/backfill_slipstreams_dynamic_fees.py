#!/usr/bin/env python3
"""Backfill Aerodrome Slipstreams dynamic-fee state from Base logs.

The script prints every relevant event and the reconstructed final state for each
pool. Pass ``--sql-output`` to also produce an idempotent PostgreSQL migration.

Example:

    BASE_RPC_URL=https://... python3 \
        protocols/substreams/base-aerodrome-slipstreams/scripts/backfill_slipstreams_dynamic_fees.py \
        --to-block 44250000 \
        --sql-output /tmp/slipstreams_dynamic_fee_backfill.sql

Reuse a previous JSONL scan without fetching logs again:

    BASE_RPC_URL=https://... python3 \
        protocols/substreams/base-aerodrome-slipstreams/scripts/backfill_slipstreams_dynamic_fees.py \
        --to-block 44250000 \
        --input-file /tmp/slipstreams_dynamic_fee_scan.jsonl \
        --sql-output /tmp/slipstreams_dynamic_fee_backfill.sql

By default, each configured module is scanned from its deployment block. This
requires an archive-capable RPC. ``--from-block`` can be used when the deployment
block is already known or the RPC cannot serve historical ``eth_getCode`` calls.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from web3 import Web3


DEFAULT_FEE_MODULES = (
    "0x090b2A6bb475c00e2256e2095A60887cD710803b",
    "0xF4Ecd78EBEB6d36CF7f80B5B6B41453515fe2785",
)
DEFAULT_CHAIN = "base"
DEFAULT_PROTOCOL_SYSTEM = "aerodrome_slipstreams"
ABI_PATH = (
    Path(__file__).resolve().parents[1] / "abi/DynamicSwapFeeModule.json"
)
MAX_TIMESTAMP_SQL = "TIMESTAMPTZ '262142-12-31 23:59:59.999999+00'"

DYNAMIC_FEE_ATTRIBUTES = (
    "dfc_baseFee",
    "dfc_scalingFactor",
    "dfc_feeCap",
    "dfc_initialFeeEnabled",
    "dfc_initialFee",
)
RELEVANT_EVENTS = {
    "CustomFeeSet",
    "ScalingFactorSet",
    "FeeCapSet",
    "InitialFeeSet",
    "InitialFeeDisabled",
    "DynamicFeeReset",
}


@dataclass(frozen=True)
class DynamicFeeEvent:
    module: str
    pool: str
    name: str
    value: int | None
    block_number: int
    block_hash: str
    transaction_hash: str
    transaction_index: int
    log_index: int

    @property
    def ordinal(self) -> tuple[int, int, int]:
        return (self.block_number, self.transaction_index, self.log_index)


@dataclass
class PoolState:
    module: str
    pool: str
    attributes: dict[str, int] = field(
        default_factory=lambda: {attribute: 0 for attribute in DYNAMIC_FEE_ATTRIBUTES}
    )
    last_event: DynamicFeeEvent | None = None


@dataclass(frozen=True)
class BlockMetadata:
    hash: str
    parent_hash: str
    number: int
    timestamp: int


@dataclass(frozen=True)
class TransactionMetadata:
    hash: str
    from_address: str
    to_address: str | bytes
    index: int
    block: BlockMetadata


def normalize_hex(value: Any, byte_length: int, label: str) -> str:
    if isinstance(value, str):
        raw = value[2:] if value.startswith("0x") else value
    else:
        raw = bytes(value).hex()
    if len(raw) != byte_length * 2:
        raise ValueError(f"Invalid {label}: expected {byte_length} bytes, got {len(raw) // 2}")
    try:
        bytes.fromhex(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid {label}: not hexadecimal") from exc
    return "0x" + raw.lower()


def normalize_address(value: Any, label: str = "address") -> str:
    return normalize_hex(value, 20, label)


def normalize_hash(value: Any, label: str = "hash") -> str:
    return normalize_hex(value, 32, label)


def transaction_recipient_for_storage(value: Any | None) -> str | bytes:
    """Match Tycho storage: contract-creation transactions use an empty ``to`` bytea."""
    return b"" if value is None else normalize_address(value, "transaction recipient")


def as_int(value: Any) -> int:
    if isinstance(value, str):
        return int(value, 16) if value.startswith("0x") else int(value)
    return int(value)


def encode_positive_bigint(value: int) -> bytes:
    """Match num_bigint::BigInt::to_signed_bytes_be for non-negative values."""
    if value < 0:
        raise ValueError("Dynamic fee values cannot be negative")
    if value == 0:
        return b"\x00"
    encoded = value.to_bytes((value.bit_length() + 7) // 8, "big")
    return b"\x00" + encoded if encoded[0] & 0x80 else encoded


def apply_event(state: PoolState, event: DynamicFeeEvent) -> None:
    if event.name == "CustomFeeSet":
        state.attributes["dfc_baseFee"] = require_value(event)
    elif event.name == "ScalingFactorSet":
        state.attributes["dfc_scalingFactor"] = require_value(event)
    elif event.name == "FeeCapSet":
        state.attributes["dfc_feeCap"] = require_value(event)
    elif event.name == "InitialFeeSet":
        state.attributes["dfc_initialFeeEnabled"] = 1
        state.attributes["dfc_initialFee"] = require_value(event)
    elif event.name == "InitialFeeDisabled":
        state.attributes["dfc_initialFeeEnabled"] = 0
        state.attributes["dfc_initialFee"] = 0
    elif event.name == "DynamicFeeReset":
        # This intentionally mirrors store_dynamic_fee_config: custom base fee survives a reset.
        for attribute in (
            "dfc_scalingFactor",
            "dfc_feeCap",
            "dfc_initialFeeEnabled",
            "dfc_initialFee",
        ):
            state.attributes[attribute] = 0
    else:
        raise ValueError(f"Unsupported dynamic fee event: {event.name}")
    state.last_event = event


def require_value(event: DynamicFeeEvent) -> int:
    if event.value is None:
        raise ValueError(f"{event.name} is missing its value")
    return event.value


def replay_events(events: Iterable[DynamicFeeEvent]) -> list[PoolState]:
    states: dict[str, PoolState] = {}
    for event in sorted(events, key=lambda item: item.ordinal):
        pool = normalize_address(event.pool, "pool address")
        module = normalize_address(event.module, "fee module address")
        state = states.get(pool)
        if state is None:
            state = PoolState(module=module, pool=pool)
            states[pool] = state
        elif state.module != module:
            raise ValueError(
                f"Pool {pool} was updated by multiple fee modules: {state.module} and {module}"
            )
        apply_event(state, event)
    return sorted(states.values(), key=lambda item: item.pool)


def load_event_abis() -> dict[str, Mapping[str, Any]]:
    with ABI_PATH.open() as abi_file:
        abi = json.load(abi_file)
    result = {
        item["name"]: item
        for item in abi
        if item.get("type") == "event" and item.get("name") in RELEVANT_EVENTS
    }
    missing = RELEVANT_EVENTS.difference(result)
    if missing:
        raise RuntimeError(f"Dynamic fee ABI is missing events: {sorted(missing)}")
    return result


def event_signature(event_abi: Mapping[str, Any]) -> str:
    parameter_types = ",".join(item["type"] for item in event_abi["inputs"])
    return f"{event_abi['name']}({parameter_types})"


def topic_to_bytes(topic: Any) -> bytes:
    if isinstance(topic, str):
        return bytes.fromhex(topic.removeprefix("0x"))
    return bytes(topic)


def build_topic_map(event_abis: Mapping[str, Mapping[str, Any]]) -> dict[str, str]:
    return {
        normalize_hash(Web3.keccak(text=event_signature(event_abi)), "event topic"): name
        for name, event_abi in event_abis.items()
    }


def decode_log(log: Mapping[str, Any], topic_map: Mapping[str, str]) -> DynamicFeeEvent:
    topics = log["topics"]
    topic0 = "0x" + topic_to_bytes(topics[0]).hex()
    try:
        name = topic_map[topic0.lower()]
    except KeyError as exc:
        raise ValueError(f"Unsupported event topic: {topic0}") from exc

    expected_topics = 2 if name in {"InitialFeeDisabled", "DynamicFeeReset"} else 3
    if len(topics) != expected_topics:
        raise ValueError(f"{name} has {len(topics)} topics; expected {expected_topics}")

    pool_topic = topic_to_bytes(topics[1])
    value = int.from_bytes(topic_to_bytes(topics[2]), "big") if expected_topics == 3 else None
    return DynamicFeeEvent(
        module=normalize_address(log["address"], "fee module address"),
        pool=normalize_address(pool_topic[-20:], "pool address"),
        name=name,
        value=value,
        block_number=as_int(log["blockNumber"]),
        block_hash=normalize_hash(log["blockHash"], "block hash"),
        transaction_hash=normalize_hash(log["transactionHash"], "transaction hash"),
        transaction_index=as_int(log["transactionIndex"]),
        log_index=as_int(log["logIndex"]),
    )


def discover_creation_block(web3: Web3, module: str, to_block: int) -> int:
    checksum_module = Web3.to_checksum_address(module)
    if not web3.eth.get_code(checksum_module, block_identifier=to_block):
        raise ValueError(f"Fee module {module} has no code at block {to_block}")

    low, high = 0, to_block
    while low < high:
        middle = (low + high) // 2
        if web3.eth.get_code(checksum_module, block_identifier=middle):
            high = middle
        else:
            low = middle + 1
    if low == 0:
        raise RuntimeError(
            "RPC returned contract code at genesis; historical eth_getCode is not reliable"
        )
    return low


def _fetch_logs_with_splitting(
    web3: Web3,
    module: str,
    topic0s: Sequence[str],
    from_block: int,
    to_block: int,
) -> list[Mapping[str, Any]]:
    try:
        return list(
            web3.eth.get_logs(
                {
                    "address": Web3.to_checksum_address(module),
                    "fromBlock": from_block,
                    "toBlock": to_block,
                    "topics": [list(topic0s)],
                }
            )
        )
    except Exception:
        if from_block == to_block:
            raise
        middle = (from_block + to_block) // 2
        return _fetch_logs_with_splitting(web3, module, topic0s, from_block, middle) + (
            _fetch_logs_with_splitting(web3, module, topic0s, middle + 1, to_block)
        )


def fetch_module_events(
    web3: Web3,
    module: str,
    from_block: int,
    to_block: int,
    chunk_size: int,
    topic_map: Mapping[str, str],
) -> list[DynamicFeeEvent]:
    events: list[DynamicFeeEvent] = []
    for chunk_start in range(from_block, to_block + 1, chunk_size):
        chunk_end = min(to_block, chunk_start + chunk_size - 1)
        logs = _fetch_logs_with_splitting(
            web3, module, tuple(topic_map), chunk_start, chunk_end
        )
        events.extend(decode_log(log, topic_map) for log in logs)
    return events


def fetch_transaction_metadata(
    web3: Web3, states: Sequence[PoolState]
) -> dict[str, TransactionMetadata]:
    result: dict[str, TransactionMetadata] = {}
    blocks: dict[str, BlockMetadata] = {}
    for state in states:
        if state.last_event is None:
            raise ValueError(f"Pool {state.pool} has no last event")
        tx_hash = state.last_event.transaction_hash
        if tx_hash in result:
            continue
        tx = web3.eth.get_transaction(tx_hash)
        block_hash = normalize_hash(tx["blockHash"], "block hash")
        block = blocks.get(block_hash)
        if block is None:
            rpc_block = web3.eth.get_block(block_hash)
            block = BlockMetadata(
                hash=block_hash,
                parent_hash=normalize_hash(rpc_block["parentHash"], "parent block hash"),
                number=as_int(rpc_block["number"]),
                timestamp=as_int(rpc_block["timestamp"]),
            )
            blocks[block_hash] = block
        result[tx_hash] = TransactionMetadata(
            hash=tx_hash,
            from_address=normalize_address(tx["from"], "transaction sender"),
            to_address=transaction_recipient_for_storage(tx["to"]),
            index=as_int(tx["transactionIndex"]),
            block=block,
        )
    return result


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def bytea_sql(value: str | bytes) -> str:
    raw = value.removeprefix("0x") if isinstance(value, str) else value.hex()
    return f"decode('{raw}', 'hex')"


def values_sql(rows: Sequence[Sequence[str]]) -> str:
    return ",\n".join("    (" + ", ".join(row) + ")" for row in rows)


def render_sql(
    states: Sequence[PoolState],
    transactions: Mapping[str, TransactionMetadata],
    *,
    chain: str,
    protocol_system: str,
    to_block: int,
) -> str:
    if not states:
        return f"-- No dynamic fee events found through {chain} block {to_block}.\n"

    used_transactions: dict[str, TransactionMetadata] = {}
    state_rows: list[tuple[str, ...]] = []
    for state in states:
        if state.last_event is None:
            raise ValueError(f"Pool {state.pool} has no last event")
        tx_hash = state.last_event.transaction_hash
        try:
            used_transactions[tx_hash] = transactions[tx_hash]
        except KeyError as exc:
            raise ValueError(f"Missing metadata for transaction {tx_hash}") from exc

        attributes = {"dynamic_fee_module": bytes.fromhex(state.module.removeprefix("0x"))}
        attributes.update(
            (attribute, encode_positive_bigint(state.attributes[attribute]))
            for attribute in DYNAMIC_FEE_ATTRIBUTES
        )
        for attribute, value in attributes.items():
            state_rows.append(
                (
                    sql_literal(state.pool),
                    sql_literal(attribute),
                    bytea_sql(value),
                    bytea_sql(tx_hash),
                )
            )

    blocks = {tx.block.hash: tx.block for tx in used_transactions.values()}
    block_rows = [
        (
            bytea_sql(block.hash),
            bytea_sql(block.parent_hash),
            str(block.number),
            str(block.timestamp),
        )
        for block in sorted(blocks.values(), key=lambda item: item.number)
    ]
    transaction_rows = [
        (
            bytea_sql(tx.hash),
            bytea_sql(tx.from_address),
            bytea_sql(tx.to_address),
            str(tx.index),
            bytea_sql(tx.block.hash),
        )
        for tx in sorted(used_transactions.values(), key=lambda item: (item.block.number, item.index))
    ]

    chain_literal = sql_literal(chain)
    system_literal = sql_literal(protocol_system)
    return f"""-- Aerodrome Slipstreams dynamic fee backfill.
-- Backfill owns state through Base block {to_block}; streamed updates after this block win.
-- Review the staged row counts before COMMIT when running this in production.

BEGIN;

CREATE TEMP TABLE _slipstreams_backfill_blocks (
    block_hash bytea PRIMARY KEY,
    parent_hash bytea NOT NULL,
    block_number bigint NOT NULL,
    block_timestamp bigint NOT NULL
) ON COMMIT DROP;

INSERT INTO _slipstreams_backfill_blocks VALUES
{values_sql(block_rows)};

CREATE TEMP TABLE _slipstreams_backfill_transactions (
    tx_hash bytea PRIMARY KEY,
    from_address bytea NOT NULL,
    to_address bytea NOT NULL,
    tx_index bigint NOT NULL,
    block_hash bytea NOT NULL
) ON COMMIT DROP;

INSERT INTO _slipstreams_backfill_transactions VALUES
{values_sql(transaction_rows)};

CREATE TEMP TABLE _slipstreams_backfill_state (
    pool_external_id text NOT NULL,
    attribute_name text NOT NULL,
    attribute_value bytea NOT NULL,
    tx_hash bytea NOT NULL,
    PRIMARY KEY (pool_external_id, attribute_name)
) ON COMMIT DROP;

INSERT INTO _slipstreams_backfill_state VALUES
{values_sql(state_rows)};

DO $$
BEGIN
    IF (SELECT count(*) FROM chain WHERE name = {chain_literal}) <> 1 THEN
        RAISE EXCEPTION 'Expected exactly one chain named %', {chain_literal};
    END IF;
    IF (SELECT count(*) FROM protocol_system WHERE name = {system_literal}) <> 1 THEN
        RAISE EXCEPTION 'Expected exactly one protocol system named %', {system_literal};
    END IF;
END $$;

-- A transaction requires a block FK. Insert a canonical block only when the indexer has not
-- already persisted the event block.
INSERT INTO "block" ("hash", parent_hash, main, "number", ts, chain_id)
SELECT staged.block_hash,
       staged.parent_hash,
       TRUE,
       staged.block_number,
       to_timestamp(staged.block_timestamp),
       c.id
FROM _slipstreams_backfill_blocks staged
JOIN chain c ON c.name = {chain_literal}
ON CONFLICT DO NOTHING;

INSERT INTO "transaction" ("hash", "from", "to", "index", block_id)
SELECT staged.tx_hash,
       staged.from_address,
       staged.to_address,
       staged.tx_index,
       b.id
FROM _slipstreams_backfill_transactions staged
JOIN chain c ON c.name = {chain_literal}
JOIN "block" b ON b.chain_id = c.id AND b."hash" = staged.block_hash
ON CONFLICT DO NOTHING;

CREATE TEMP TABLE _slipstreams_backfill_resolved ON COMMIT DROP AS
SELECT pc.id AS protocol_component_id,
       staged.attribute_name,
       staged.attribute_value,
       tx.id AS modify_tx,
       b.ts AS valid_from,
       b."number" AS block_number,
       tx."index" AS tx_index
FROM _slipstreams_backfill_state staged
JOIN chain c ON c.name = {chain_literal}
JOIN protocol_system ps ON ps.name = {system_literal}
JOIN protocol_component pc
  ON pc.chain_id = c.id
 AND pc.protocol_system_id = ps.id
 AND lower(pc.external_id) = lower(staged.pool_external_id)
JOIN "transaction" tx ON tx."hash" = staged.tx_hash
JOIN "block" b ON b.id = tx.block_id;

DO $$
DECLARE
    staged_count bigint;
    resolved_count bigint;
BEGIN
    SELECT count(*) INTO staged_count FROM _slipstreams_backfill_state;
    SELECT count(*) INTO resolved_count FROM _slipstreams_backfill_resolved;
    IF staged_count <> resolved_count THEN
        RAISE EXCEPTION
            'Resolved % of % state rows; check blocks, transactions, pools and protocol system',
            resolved_count, staged_count;
    END IF;
END $$;

-- This is a current-state repair, not a historical replay. Upsert only when the current row is
-- absent or is no newer than the reconstructed final snapshot.
INSERT INTO protocol_state_default (
    protocol_component_id,
    attribute_name,
    attribute_value,
    previous_value,
    modify_tx,
    valid_from,
    valid_to
)
SELECT resolved.protocol_component_id,
       resolved.attribute_name,
       resolved.attribute_value,
       CASE
           WHEN ROW(current_block."number", current_tx."index")
                  < ROW(resolved.block_number, resolved.tx_index)
               THEN current_state.attribute_value
           WHEN ROW(current_block."number", current_tx."index")
                  = ROW(resolved.block_number, resolved.tx_index)
               THEN current_state.previous_value
           ELSE NULL
       END,
       resolved.modify_tx,
       resolved.valid_from,
       {MAX_TIMESTAMP_SQL}
FROM _slipstreams_backfill_resolved resolved
LEFT JOIN protocol_state_default current_state
  ON current_state.protocol_component_id = resolved.protocol_component_id
 AND current_state.attribute_name = resolved.attribute_name
LEFT JOIN "transaction" current_tx ON current_tx.id = current_state.modify_tx
LEFT JOIN "block" current_block ON current_block.id = current_tx.block_id
WHERE current_state.valid_from IS NULL
   OR ROW(current_block."number", current_tx."index")
      <= ROW(resolved.block_number, resolved.tx_index)
ON CONFLICT ON CONSTRAINT protocol_state_default_unique_pk DO UPDATE
SET attribute_value = EXCLUDED.attribute_value,
    previous_value = EXCLUDED.previous_value,
    modify_tx = EXCLUDED.modify_tx,
    valid_from = EXCLUDED.valid_from,
    modified_ts = CURRENT_TIMESTAMP
WHERE protocol_state_default.valid_from <= EXCLUDED.valid_from;

SELECT count(*) AS backfilled_attribute_rows FROM _slipstreams_backfill_resolved;

COMMIT;
"""


def print_results(events: Sequence[DynamicFeeEvent], states: Sequence[PoolState]) -> None:
    for event in events:
        print(json.dumps({"type": "event", **asdict(event)}, sort_keys=True))
    for state in states:
        if state.last_event is None:
            continue
        print(
            json.dumps(
                {
                    "type": "pool_state",
                    "module": state.module,
                    "pool": state.pool,
                    "attributes": state.attributes,
                    "last_update_block": state.last_event.block_number,
                    "last_update_tx": state.last_event.transaction_hash,
                },
                sort_keys=True,
            )
        )


def load_events_from_output(path: Path) -> list[DynamicFeeEvent]:
    """Load event rows from JSONL previously written by ``print_results``."""
    events: list[DynamicFeeEvent] = []
    with path.open() as output_file:
        for line_number, line in enumerate(output_file, start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON in {path} at line {line_number}: {exc.msg}") from exc
            if not isinstance(record, dict):
                raise ValueError(f"Invalid JSONL record in {path} at line {line_number}")
            if record.get("type") != "event":
                continue
            try:
                name = str(record["name"])
                if name not in RELEVANT_EVENTS:
                    raise ValueError(f"unsupported event {name}")
                raw_value = record.get("value")
                events.append(
                    DynamicFeeEvent(
                        module=normalize_address(record["module"], "fee module address"),
                        pool=normalize_address(record["pool"], "pool address"),
                        name=name,
                        value=None if raw_value is None else as_int(raw_value),
                        block_number=as_int(record["block_number"]),
                        block_hash=normalize_hash(record["block_hash"], "block hash"),
                        transaction_hash=normalize_hash(
                            record["transaction_hash"], "transaction hash"
                        ),
                        transaction_index=as_int(record["transaction_index"]),
                        log_index=as_int(record["log_index"]),
                    )
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(
                    f"Invalid event record in {path} at line {line_number}: {exc}"
                ) from exc
    if not events:
        raise ValueError(f"{path} does not contain any event rows")
    events.sort(key=lambda item: item.ordinal)
    return events


def connect_web3(args: argparse.Namespace) -> Web3:
    if not args.rpc_url:
        raise ValueError("Provide --rpc-url, BASE_RPC_URL or RPC_URL")
    web3 = Web3(Web3.HTTPProvider(args.rpc_url, request_kwargs={"timeout": args.request_timeout}))
    if not web3.is_connected():
        raise ConnectionError("Could not connect to the Base RPC")
    return web3


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rpc-url",
        default=os.getenv("BASE_RPC_URL") or os.getenv("RPC_URL"),
        help="Base JSON-RPC URL (defaults to BASE_RPC_URL, then RPC_URL)",
    )
    parser.add_argument("--to-block", required=True, type=int, help="Inclusive cutover block Y")
    parser.add_argument(
        "--from-block",
        type=int,
        help="Inclusive scan start override; otherwise discover each module deployment block",
    )
    parser.add_argument(
        "--module",
        action="append",
        dest="modules",
        help="Fee module address; repeat to override the two current modules",
    )
    parser.add_argument("--chunk-size", type=int, default=5_000)
    parser.add_argument("--request-timeout", type=int, default=60)
    parser.add_argument(
        "--input-file",
        type=Path,
        help="Read prior JSONL output and skip the eth_getLogs scan",
    )
    parser.add_argument("--sql-output", type=Path, help="Write an executable PostgreSQL script")
    parser.add_argument("--chain", default=DEFAULT_CHAIN)
    parser.add_argument("--protocol-system", default=DEFAULT_PROTOCOL_SYSTEM)
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> int:
    if args.to_block < 0:
        raise ValueError("--to-block must be non-negative")
    if args.from_block is not None and args.from_block > args.to_block:
        raise ValueError("--from-block cannot be greater than --to-block")
    if args.chunk_size <= 0:
        raise ValueError("--chunk-size must be positive")
    if args.input_file and (args.from_block is not None or args.modules):
        raise ValueError("--input-file cannot be combined with --from-block or --module")

    web3: Web3 | None = None
    if args.input_file:
        events = load_events_from_output(args.input_file)
        if events[-1].block_number > args.to_block:
            raise ValueError(
                f"{args.input_file} contains an event after --to-block {args.to_block}"
            )
        print(f"Loaded {len(events)} event rows from {args.input_file}; scan skipped", file=sys.stderr)
    else:
        modules = tuple(
            normalize_address(module, "fee module address")
            for module in (args.modules or DEFAULT_FEE_MODULES)
        )
        web3 = connect_web3(args)
        topic_map = build_topic_map(load_event_abis())
        events = []
        for module in modules:
            if args.from_block is None:
                try:
                    from_block = discover_creation_block(web3, module, args.to_block)
                except Exception as exc:
                    raise RuntimeError(
                        f"Could not discover deployment block for {module}; use --from-block with "
                        "a known safe start if the RPC is not archive-capable"
                    ) from exc
            else:
                from_block = args.from_block
            print(
                f"Scanning {module} from block {from_block} through {args.to_block}",
                file=sys.stderr,
            )
            events.extend(
                fetch_module_events(
                    web3,
                    module,
                    from_block,
                    args.to_block,
                    args.chunk_size,
                    topic_map,
                )
            )
        events.sort(key=lambda item: item.ordinal)

    states = replay_events(events)
    if not args.input_file:
        print_results(events, states)
    print(
        f"Found {len(events)} relevant events affecting {len(states)} pools",
        file=sys.stderr,
    )

    if args.sql_output:
        if web3 is None:
            web3 = connect_web3(args)
        transactions = fetch_transaction_metadata(web3, states)
        sql = render_sql(
            states,
            transactions,
            chain=args.chain,
            protocol_system=args.protocol_system,
            to_block=args.to_block,
        )
        args.sql_output.write_text(sql)
        print(f"Wrote SQL backfill to {args.sql_output}", file=sys.stderr)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return run(parse_args(argv))
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
