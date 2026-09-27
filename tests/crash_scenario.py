"""Deterministic call sequence shared by the crash child and the parent test.

Covers: breach + delivery + retry, a reminder cancelled because its segment
ends before any tick can deliver it, Pending round-trips, reassignment,
close/reopen, an agent reply exactly on the deadline, a reminder that always
fails (exhausts retries), and a fallback recipient.
"""

FALLBACK = "fallback"
FAIL_CALLS = {1}        # global sink-call numbers (1-based) that raise
FAIL_IDS = {"E#1"}      # reminder ids whose sink always raises
TICK_STEP = 10
T_END = 10000

_EVENTS = [
    # (kind, event_id, conv, arg, at)
    ("assign", "ev00", "A", "u1", 0),
    ("message", "ev01", "E", "customer", 20),      # breach 3620, always fails
    ("message", "ev02", "D", "customer", 50),      # breach 3650 -> fallback
    ("message", "ev03", "A", "customer", 10 + 0),  # placeholder, fixed below
    ("message", "ev04", "B", "customer", 100),     # breach 3700, then cancelled
    ("message", "ev05", "C", "customer", 500),     # Pending round-trip below
    ("set_status", "ev06", "C", "Pending", 1000),  # acc = 500
    ("set_status", "ev07", "C", "Open", 2000),     # deadline = 2000 + 3100
    ("message", "ev08", "B", "agent", 3705),       # ends seg just after breach
    ("message", "ev09", "D", "agent", 4000),       # ends seg after delivery
    ("set_status", "ev10", "D", "Closed", 4100),
    ("message", "ev11", "E", "agent", 5000),       # ends exhausted segment
    ("message", "ev12", "C", "agent", 5100),       # reply exactly on deadline
    ("set_status", "ev13", "C", "Resolved", 5200),
    ("set_status", "ev14", "C", "Closed", 5300),
    ("set_status", "ev15", "C", "Open", 5400),     # reopen
    ("message", "ev16", "C", "agent", 5500),       # no open segment: no-op
    ("message", "ev17", "C", "customer", 5600),    # seg2, breach at 9200
    ("assign", "ev18", "C", "u2", 6000),
    ("message", "ev19", "A", "agent", 8000),       # ends A's segment
    ("set_status", "ev20", "C", "Resolved", 9500), # ends C seg2 after delivery
]
# A's customer message at t=10 (breach at 3610, first sink call fails once).
_EVENTS[3] = ("message", "ev03", "A", "customer", 10)


def scenario_calls():
    """Ordered call list: ('tick', now) or ('message'|'set_status'|'assign', ...).

    At equal timestamps ticks come before events, exercising the rule that
    events with at == now may still arrive after tick(now).
    """
    calls = [("tick", t) for t in range(0, T_END + 1, TICK_STEP)]
    calls += [tuple(ev) for ev in _EVENTS]
    calls.sort(key=lambda c: (c[-1], 0 if c[0] == "tick" else 1))
    return calls


def conv_ids():
    return sorted({ev[2] for ev in _EVENTS})


def assignee_at(conv, at):
    """Assignee strictly before time `at` (ticks run before same-time events)."""
    owner = None
    for ev in sorted(_EVENTS, key=lambda e: e[-1]):
        if ev[0] == "assign" and ev[2] == conv and ev[4] < at:
            owner = ev[3]
    return owner if owner is not None else FALLBACK
