import importlib.util
import json
import sys
import tempfile
import unittest
from dataclasses import asdict
from pathlib import Path


SCRIPT_PATH = Path(__file__).parents[1] / "backfill_slipstreams_dynamic_fees.py"
SPEC = importlib.util.spec_from_file_location("slipstreams_dynamic_fee_backfill", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class DynamicFeeBackfillTest(unittest.TestCase):
    def event(self, name, value=None, module="0x" + "11" * 20, pool="0x" + "aa" * 20):
        return MODULE.DynamicFeeEvent(
            module=module,
            pool=pool,
            name=name,
            value=value,
            block_number=100,
            block_hash="0x" + "01" * 32,
            transaction_hash="0x" + "02" * 32,
            transaction_index=3,
            log_index=4,
        )

    def test_replay_matches_substreams_reset_and_initial_fee_semantics(self):
        events = [
            self.event("CustomFeeSet", 500),
            self.event("ScalingFactorSet", 8_000_000),
            self.event("FeeCapSet", 30_000),
            self.event("InitialFeeSet", 700),
            self.event("DynamicFeeReset"),
        ]

        [state] = MODULE.replay_events(events)

        self.assertEqual(state.attributes["dfc_baseFee"], 500)
        self.assertEqual(state.attributes["dfc_scalingFactor"], 0)
        self.assertEqual(state.attributes["dfc_feeCap"], 0)
        self.assertEqual(state.attributes["dfc_initialFeeEnabled"], 0)
        self.assertEqual(state.attributes["dfc_initialFee"], 0)
        self.assertEqual(state.last_event, events[-1])

    def test_initial_fee_disabled_clears_both_initial_fields(self):
        events = [self.event("InitialFeeSet", 700), self.event("InitialFeeDisabled")]

        [state] = MODULE.replay_events(events)

        self.assertEqual(state.attributes["dfc_initialFeeEnabled"], 0)
        self.assertEqual(state.attributes["dfc_initialFee"], 0)

    def test_replay_rejects_a_pool_touched_by_multiple_modules(self):
        events = [
            self.event("CustomFeeSet", 500, module="0x" + "11" * 20),
            self.event("FeeCapSet", 30_000, module="0x" + "22" * 20),
        ]

        with self.assertRaisesRegex(ValueError, "multiple fee modules"):
            MODULE.replay_events(events)

    def test_bigint_encoding_matches_num_bigint_signed_bytes(self):
        self.assertEqual(MODULE.encode_positive_bigint(0), b"\x00")
        self.assertEqual(MODULE.encode_positive_bigint(127), b"\x7f")
        self.assertEqual(MODULE.encode_positive_bigint(128), b"\x00\x80")
        self.assertEqual(MODULE.encode_positive_bigint(30_000), b"\x75\x30")

    def test_contract_creation_transaction_recipient_is_stored_as_empty_bytes(self):
        self.assertEqual(MODULE.transaction_recipient_for_storage(None), b"")
        self.assertEqual(
            MODULE.transaction_recipient_for_storage("0x" + "11" * 20),
            "0x" + "11" * 20,
        )

    def test_topic_map_uses_rpc_prefixed_topic_format(self):
        topic_map = MODULE.build_topic_map(MODULE.load_event_abis())

        self.assertTrue(all(topic.startswith("0x") and len(topic) == 66 for topic in topic_map))

    def test_load_events_from_previous_jsonl_output(self):
        events = [self.event("FeeCapSet", 30_000), self.event("ScalingFactorSet", 5_000_000)]
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "scan.jsonl"
            lines = [json.dumps({"type": "event", **asdict(event)}) for event in events]
            lines.append(
                json.dumps(
                    {
                        "type": "pool_state",
                        "pool": events[0].pool,
                        "module": events[0].module,
                        "attributes": {},
                        "last_update_block": events[-1].block_number,
                        "last_update_tx": events[-1].transaction_hash,
                    }
                )
            )
            output.write_text("\n".join(lines) + "\n")

            loaded = MODULE.load_events_from_output(output)

        self.assertEqual(loaded, events)

    def test_load_events_rejects_output_without_event_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "scan.jsonl"
            output.write_text('{"type":"pool_state"}\n')

            with self.assertRaisesRegex(ValueError, "does not contain any event rows"):
                MODULE.load_events_from_output(output)

    def test_sql_resolves_relations_and_does_not_overwrite_newer_state(self):
        event = self.event("FeeCapSet", 30_000)
        [state] = MODULE.replay_events([event])
        tx = MODULE.TransactionMetadata(
            hash=event.transaction_hash,
            from_address="0x" + "bb" * 20,
            to_address=event.module,
            index=event.transaction_index,
            block=MODULE.BlockMetadata(
                hash=event.block_hash,
                parent_hash="0x" + "03" * 32,
                number=event.block_number,
                timestamp=1_700_000_000,
            ),
        )

        sql = MODULE.render_sql(
            [state],
            {event.transaction_hash: tx},
            chain="base",
            protocol_system="aerodrome_slipstreams",
            to_block=100,
        )

        self.assertIn('INSERT INTO "block"', sql)
        self.assertIn('INSERT INTO "transaction"', sql)
        self.assertIn("JOIN protocol_system ps", sql)
        self.assertIn("JOIN protocol_component pc", sql)
        self.assertIn("protocol_state_default", sql)
        self.assertIn(
            'ROW(current_block."number", current_tx."index")\n'
            "      <= ROW(resolved.block_number, resolved.tx_index)",
            sql,
        )
        self.assertIn("WHERE protocol_state_default.valid_from <= EXCLUDED.valid_from", sql)
        self.assertIn("dynamic_fee_module", sql)
        self.assertIn("dfc_initialFeeEnabled", sql)
        self.assertIn("-- Backfill owns state through Base block 100", sql)

    def test_sql_only_upserts_the_final_current_state(self):
        event = self.event("FeeCapSet", 30_000)
        [state] = MODULE.replay_events([event])
        tx = MODULE.TransactionMetadata(
            hash=event.transaction_hash,
            from_address="0x" + "bb" * 20,
            to_address=event.module,
            index=event.transaction_index,
            block=MODULE.BlockMetadata(
                hash=event.block_hash,
                parent_hash="0x" + "03" * 32,
                number=event.block_number,
                timestamp=1_700_000_000,
            ),
        )

        sql = MODULE.render_sql(
            [state],
            {event.transaction_hash: tx},
            chain="base",
            protocol_system="aerodrome_slipstreams",
            to_block=100,
        )

        self.assertNotIn("INSERT INTO protocol_state (", sql)
        self.assertNotIn("UPDATE protocol_state historical", sql)
        self.assertNotIn("JOIN LATERAL", sql)
        self.assertIn("INSERT INTO protocol_state_default", sql)

    def test_sql_encodes_empty_contract_creation_recipient(self):
        event = self.event("FeeCapSet", 30_000)
        [state] = MODULE.replay_events([event])
        tx = MODULE.TransactionMetadata(
            hash=event.transaction_hash,
            from_address="0x" + "bb" * 20,
            to_address=b"",
            index=event.transaction_index,
            block=MODULE.BlockMetadata(
                hash=event.block_hash,
                parent_hash="0x" + "03" * 32,
                number=event.block_number,
                timestamp=1_700_000_000,
            ),
        )

        sql = MODULE.render_sql(
            [state],
            {event.transaction_hash: tx},
            chain="base",
            protocol_system="aerodrome_slipstreams",
            to_block=100,
        )

        self.assertIn("decode('', 'hex')", sql)


if __name__ == "__main__":
    unittest.main()
