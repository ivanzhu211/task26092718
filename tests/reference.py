"""Independent in-memory reference implementation.

Deliberately naive: full scans, no persistence, no performance concerns.
Shares no code with the engine; used to cross-check breach computation.
"""

ACTIVE = frozenset({"Open", "In Progress"})
ENDING = frozenset({"Resolved", "Closed"})
LIMIT = 3600


class Reference:
    def __init__(self):
        self._convs = {}
        self._breaches = {}
        self._seg_ends = {}

    def _conv(self, conv):
        return self._convs.setdefault(conv, {
            "status": "Open",
            "seg_open": False,
            "seg_seq": 0,
            "breached": False,
            "acc": 0,
            "last": None,
            "deadline": None,
        })

    def _judge(self, conv, st, now):
        if st["seg_open"] and st["deadline"] is not None \
                and st["deadline"] < now:
            self._breaches.setdefault(conv, []).append(st["deadline"])
            st["deadline"] = None
            st["breached"] = True

    def _end(self, conv, st, at):
        if st["seg_open"]:
            st["seg_open"] = False
            st["deadline"] = None
            st["acc"] = 0
            self._seg_ends[(conv, st["seg_seq"])] = at

    def message(self, conv, author, at):
        st = self._conv(conv)
        self._judge(conv, st, at)
        if author == "customer":
            if not st["seg_open"]:
                st["seg_open"] = True
                st["seg_seq"] += 1
                st["breached"] = False
                st["acc"] = 0
                st["last"] = at
                st["deadline"] = at + LIMIT if st["status"] in ACTIVE else None
        elif author == "agent":
            self._end(conv, st, at)
        else:
            raise ValueError("unknown author: %r" % (author,))

    def set_status(self, conv, status, at):
        st = self._conv(conv)
        self._judge(conv, st, at)
        if st["seg_open"]:
            if st["status"] in ACTIVE:
                st["acc"] += at - st["last"]
            if status in ENDING:
                self._end(conv, st, at)
            elif status in ACTIVE and not st["breached"]:
                st["last"] = at
                st["deadline"] = at + (LIMIT - st["acc"])
            else:
                st["last"] = at
                st["deadline"] = None
        st["status"] = status

    def assign(self, conv, user, at):
        self._conv(conv)

    def finalize(self, now):
        for conv, st in self._convs.items():
            self._judge(conv, st, now)

    def breaches(self, conv):
        return sorted(self._breaches.get(conv, []))

    def segment_end(self, conv, seg_seq):
        return self._seg_ends.get((conv, seg_seq))
