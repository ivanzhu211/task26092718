"""Child-process script for crash-recovery tests.

Runs the deterministic scenario against a real SQLite db. The sink appends
every invocation as a JSON line to a log file (so calls are recorded across
processes) and can be configured via env vars:

- SINK_FAIL_CALLS: comma-separated 1-based global call numbers that raise.
- SINK_FAIL_IDS:   comma-separated reminder ids whose sink always raises.
- CRASH_MODE:      "" (clean replay) | "sink" | "after_sink" | "event".
- CRASH_AT_CALL:   for "sink"/"after_sink", the 1-based sink-call number at
                   which the process SIGKILLs itself.
- CRASH_AT_EVENT:  for "event", the 1-based applied-event count at which the
                   process SIGKILLs itself inside the event transaction.
"""

import json
import os
import signal
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from reminder_engine import Engine
from tests.crash_scenario import (FALLBACK, scenario_calls)


def _env_set(name):
    return {x for x in os.environ.get(name, "").split(",") if x}


def make_sink(log_path, now_box):
    fail_calls = {int(x) for x in _env_set("SINK_FAIL_CALLS")}
    fail_ids = _env_set("SINK_FAIL_IDS")
    crash_at_call = int(os.environ.get("CRASH_AT_CALL", "0") or 0)
    crash_mode = os.environ.get("CRASH_MODE", "")

    def sink(reminder_id, recipient, conv_id):
        with open(log_path, "a") as fh:
            fh.write(json.dumps({"id": reminder_id, "to": recipient,
                                 "conv": conv_id, "at": now_box[0]}) + "\n")
        with open(log_path) as fh:
            n = sum(1 for _ in fh)
        if reminder_id in fail_ids or n in fail_calls:
            raise RuntimeError("configured sink failure")
        if crash_mode == "sink" and n == crash_at_call:
            os.kill(os.getpid(), signal.SIGKILL)

    return sink


class AfterSinkCrashEngine(Engine):
    """SIGKILLs after the sink returned but before delivery is persisted."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sink_calls = 0

    def _call_sink(self, reminder_id, recipient, conv_id):
        super()._call_sink(reminder_id, recipient, conv_id)
        self._sink_calls += 1
        if self._sink_calls == int(os.environ["CRASH_AT_CALL"]):
            os.kill(os.getpid(), signal.SIGKILL)


class MidEventCrashEngine(Engine):
    """SIGKILLs inside the event transaction, after writes, before commit."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._events_applied = 0

    def _event_applied(self, conn):
        self._events_applied += 1
        if self._events_applied == int(os.environ["CRASH_AT_EVENT"]):
            os.kill(os.getpid(), signal.SIGKILL)


def main():
    db_path, log_path = sys.argv[1], sys.argv[2]
    mode = os.environ.get("CRASH_MODE", "")
    engine_cls = {"after_sink": AfterSinkCrashEngine,
                  "event": MidEventCrashEngine}.get(mode, Engine)
    now_box = [None]
    eng = engine_cls(db_path, make_sink(log_path, now_box), FALLBACK)
    for call in scenario_calls():
        kind = call[0]
        if kind == "tick":
            now_box[0] = call[1]
            eng.tick(call[1])
        elif kind == "message":
            eng.message(call[1], call[2], call[3], call[4])
        elif kind == "set_status":
            eng.set_status(call[1], call[2], call[3], call[4])
        else:
            eng.assign(call[1], call[2], call[3], call[4])
    eng.close()


if __name__ == "__main__":
    main()
