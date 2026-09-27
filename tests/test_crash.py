import json
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from reminder_engine import Engine
from tests.crash_scenario import (FALLBACK, FAIL_CALLS, FAIL_IDS, T_END,
                                  assignee_at, conv_ids, scenario_calls)
from tests.reference import Reference

CHILD = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     "crash_child.py")


def run_clean():
    """In-process crash-free run with the same sink failure behaviour."""
    records = []

    def sink(rid, to, conv):
        records.append((rid, to, conv))
        if rid in FAIL_IDS or len(records) in FAIL_CALLS:
            raise RuntimeError("configured sink failure")

    db = os.path.join(tempfile.mkdtemp(), "clean.db")
    eng = Engine(db, sink, FALLBACK)
    for call in scenario_calls():
        if call[0] == "tick":
            eng.tick(call[1])
        elif call[0] == "message":
            eng.message(call[1], call[2], call[3], call[4])
        elif call[0] == "set_status":
            eng.set_status(call[1], call[2], call[3], call[4])
        else:
            eng.assign(call[1], call[2], call[3], call[4])
    result = {
        "breaches": {c: eng.breaches(c) for c in conv_ids()},
        "reminders": dict(eng._conn.execute(
            "SELECT reminder_id, state FROM reminders").fetchall()),
    }
    eng.close()
    return result


def run_reference():
    ref = Reference()
    for call in scenario_calls():
        if call[0] == "message":
            ref.message(call[2], call[3], call[4])
        elif call[0] == "set_status":
            ref.set_status(call[2], call[3], call[4])
        elif call[0] == "assign":
            ref.assign(call[2], call[3], call[4])
    ref.finalize(T_END)
    return ref


class CrashRecoveryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.clean = run_clean()
        cls.ref = run_reference()

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.db = os.path.join(self.tmp, "crash.db")
        self.log = os.path.join(self.tmp, "sink.log")

    def run_child(self, extra_env):
        env = dict(os.environ)
        env["SINK_FAIL_CALLS"] = ",".join(str(n) for n in FAIL_CALLS)
        env["SINK_FAIL_IDS"] = ",".join(sorted(FAIL_IDS))
        env.update(extra_env)
        return subprocess.run([sys.executable, CHILD, self.db, self.log],
                              env=env, capture_output=True, text=True)

    def read_log(self):
        with open(self.log) as fh:
            return [json.loads(line) for line in fh if line.strip()]

    def check_invariants(self):
        # 1. Breach records identical to the crash-free run and the reference.
        eng = Engine(self.db, lambda *a: None, FALLBACK)
        for conv in conv_ids():
            self.assertEqual(eng.breaches(conv), self.clean["breaches"][conv],
                             conv)
            self.assertEqual(eng.breaches(conv), self.ref.breaches(conv), conv)
        eng.close()

        conn = sqlite3.connect(self.db)
        reminders = conn.execute(
            "SELECT reminder_id, conv_id, seg_seq, state, attempts "
            "FROM reminders").fetchall()
        conn.close()
        states = {r[0]: r[3] for r in reminders}
        attempts = {r[0]: r[4] for r in reminders}

        # Exactly one reminder per breached segment, none left pending.
        total_breaches = sum(len(v) for v in self.clean["breaches"].values())
        self.assertEqual(len(reminders), total_breaches)
        self.assertNotIn("pending", set(states.values()))
        self.assertEqual(states, self.clean["reminders"])
        for rid, n in attempts.items():
            self.assertLessEqual(n, 4, (rid, n))

        # 2. Every reminder's sink was called at most 4 times.
        entries = self.read_log()
        counts = {}
        for entry in entries:
            counts[entry["id"]] = counts.get(entry["id"], 0) + 1
        for rid, n in counts.items():
            self.assertLessEqual(n, 4, (rid, n))

        # 3. Reminders that must arrive did arrive; exhausted only by config.
        delivered = {rid for rid, st in states.items() if st == "delivered"}
        clean_delivered = {rid for rid, st in self.clean["reminders"].items()
                           if st == "delivered"}
        self.assertEqual(delivered, clean_delivered)
        self.assertEqual(states.get("E#1"), "exhausted")
        self.assertEqual(counts.get("E#1"), 4)

        # Sink calls: right recipient, never later than the segment's end.
        for entry in entries:
            conv, seq = entry["id"].rsplit("#", 1)
            self.assertEqual(entry["to"], assignee_at(conv, entry["at"]),
                             entry)
            end = self.ref.segment_end(conv, int(seq))
            if end is not None:
                self.assertLessEqual(entry["at"], end, entry)

    def test_crash_inside_sink(self):
        proc = self.run_child({"CRASH_MODE": "sink", "CRASH_AT_CALL": "3"})
        self.assertEqual(proc.returncode, -signal.SIGKILL, proc.stderr)
        proc = self.run_child({})
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.check_invariants()

    def test_crash_after_sink_before_persist(self):
        proc = self.run_child({"CRASH_MODE": "after_sink",
                               "CRASH_AT_CALL": "3"})
        self.assertEqual(proc.returncode, -signal.SIGKILL, proc.stderr)
        proc = self.run_child({})
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.check_invariants()

    def test_crash_mid_event_transaction(self):
        proc = self.run_child({"CRASH_MODE": "event", "CRASH_AT_EVENT": "10"})
        self.assertEqual(proc.returncode, -signal.SIGKILL, proc.stderr)
        proc = self.run_child({})
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.check_invariants()


if __name__ == "__main__":
    unittest.main()
