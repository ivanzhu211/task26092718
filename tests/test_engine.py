import os
import random
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from reminder_engine import Engine
from tests.reference import Reference

STATUSES = ["Open", "In Progress", "Pending", "Resolved", "Closed"]


def gen_events(seed, n=250):
    rng = random.Random(seed)
    convs = ["c%d" % i for i in range(5)]
    users = ["u%d" % i for i in range(3)]
    events = []
    t = 0
    for i in range(n):
        t += rng.choice([0, 0, 1, 2, 5, 30, 60, 300, 900])
        conv = rng.choice(convs)
        kind = rng.random()
        eid = "s%d-e%d" % (seed, i)
        if kind < 0.5:
            author = "customer" if rng.random() < 0.5 else "agent"
            events.append(("message", eid, conv, author, t))
        elif kind < 0.75:
            events.append(("set_status", eid, conv, rng.choice(STATUSES), t))
        else:
            events.append(("assign", eid, conv, rng.choice(users), t))
    return events


def crafted_events():
    # Deterministic edge cases: Pending round-trips, reassign, close/reopen,
    # and agent replies landing exactly on the breach deadline.
    ev = []

    def add(kind, *args):
        ev.append((kind, "crafted-%d" % len(ev)) + args)

    # exact-deadline reply cancels the breach
    add("message", "x1", "customer", 1000)
    add("message", "x1", "agent", 4600)
    # Pending round-trip shifts the deadline; exact-deadline reply again
    add("message", "x2", "customer", 1000)
    add("set_status", "x2", "Pending", 2000)      # acc = 1000
    add("set_status", "x2", "In Progress", 5000)  # deadline = 5000 + 2600
    add("message", "x2", "agent", 7600)
    # plain breach, no reply
    add("message", "x3", "customer", 1000)
    # breach, then close, reopen, new customer message -> second segment
    add("message", "x4", "customer", 100)         # breach at 3700
    add("set_status", "x4", "Resolved", 5000)
    add("set_status", "x4", "Closed", 5100)
    add("set_status", "x4", "Open", 5200)
    add("message", "x4", "agent", 5300)           # no open segment: no-op
    add("message", "x4", "customer", 5400)        # breach at 9000
    add("assign", "x4", "u0", 5500)
    # customer message while Pending: segment opens but stays paused
    add("set_status", "x5", "Pending", 100)
    add("message", "x5", "customer", 200)
    add("set_status", "x5", "Open", 1000)         # deadline = 4600
    ev.sort(key=lambda e: e[-1])
    return ev


def run_engine(db_path, events, tick_times):
    sink_calls = []
    eng = Engine(db_path,
                 lambda rid, to, conv: sink_calls.append((rid, to, conv)),
                 "fallback")
    ticks = iter(sorted(tick_times))
    pending_tick = next(ticks, None)
    for ev in events:
        at = ev[-1]
        while pending_tick is not None and pending_tick <= at:
            eng.tick(pending_tick)
            pending_tick = next(ticks, None)
        kind = ev[0]
        if kind == "message":
            eng.message(ev[1], ev[2], ev[3], ev[4])
        elif kind == "set_status":
            eng.set_status(ev[1], ev[2], ev[3], ev[4])
        else:
            eng.assign(ev[1], ev[2], ev[3], ev[4])
    while pending_tick is not None:
        eng.tick(pending_tick)
        pending_tick = next(ticks, None)
    eng.close()
    return sink_calls


def run_reference(events, final_now):
    ref = Reference()
    for ev in events:
        if ev[0] == "message":
            ref.message(ev[2], ev[3], ev[4])
        elif ev[0] == "set_status":
            ref.set_status(ev[2], ev[3], ev[4])
        else:
            ref.assign(ev[2], ev[3], ev[4])
    ref.finalize(final_now)
    return ref


def all_convs(events):
    return sorted({ev[2] for ev in events})


class TickFrequencyTest(unittest.TestCase):
    def check_scenario(self, events):
        t_end = max(ev[-1] for ev in events) + 1
        convs = all_convs(events)
        schedules = {
            "every-1s": list(range(0, t_end + 1)),
            "every-17min": list(range(0, t_end + 1, 1020)) + [t_end],
            "final-only": [t_end],
        }
        expected = run_reference(events, t_end)
        results = {}
        tmp = tempfile.mkdtemp()
        for name, ticks in schedules.items():
            db = os.path.join(tmp, name + ".db")
            sink_calls = run_engine(db, events, ticks)
            eng = Engine(db, lambda *a: None, "fallback")
            got = {c: eng.breaches(c) for c in convs}
            eng.close()
            results[name] = got
            # per-reminder sink calls never exceed MAX_ATTEMPTS (4)
            counts = {}
            for rid, _to, _conv in sink_calls:
                counts[rid] = counts.get(rid, 0) + 1
            for rid, n in counts.items():
                self.assertLessEqual(n, 4, (name, rid, n))
        for name, got in results.items():
            for c in convs:
                self.assertEqual(got[c], expected.breaches(c),
                                 (name, c, got[c], expected.breaches(c)))

    def test_crafted_edges(self):
        self.check_scenario(crafted_events())

    def test_random_sequences(self):
        for seed in range(6):
            with self.subTest(seed=seed):
                self.check_scenario(gen_events(seed) + crafted_events())


if __name__ == "__main__":
    unittest.main()
