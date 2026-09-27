"""First-response timeout reminder engine.

All state lives in a single SQLite file. No in-memory timers, no threads,
no wall-clock reads: every timestamp comes from the caller.

Semantics:
- A waiting segment opens on the first customer message after the previous
  agent reply (or after a reopen), and ends on an agent reply or on entering
  Resolved/Closed. It accumulates time only while the conversation status is
  Open or In Progress; Pending (or any other non-active status) pauses it.
- A breach occurs at the exact moment the accumulated active time reaches
  SEGMENT_LIMIT. All events at a given timestamp are applied before a timeout
  at that timestamp is judged, so a timeout at T is only judged once time is
  known to be strictly past T (an event or tick with a larger timestamp).
- Each segment produces at most one reminder, delivered to the assignee at
  attempt time (or the fallback). Failures retry after RETRY_DELAY seconds,
  with at most MAX_ATTEMPTS sink calls in total. A reminder whose segment
  ends before successful delivery is cancelled.
"""

import sqlite3

ACTIVE_STATUSES = frozenset({"Open", "In Progress"})
ENDING_STATUSES = frozenset({"Resolved", "Closed"})
SEGMENT_LIMIT = 3600
RETRY_DELAY = 30
MAX_ATTEMPTS = 4

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS convs (
    conv_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    assignee TEXT,
    seg_open INTEGER NOT NULL DEFAULT 0,
    seg_seq INTEGER NOT NULL DEFAULT 0,
    seg_breached INTEGER NOT NULL DEFAULT 0,
    accumulated REAL NOT NULL DEFAULT 0,
    last_ts REAL,
    deadline REAL
);
CREATE INDEX IF NOT EXISTS idx_convs_deadline ON convs (deadline);
CREATE TABLE IF NOT EXISTS breaches (
    conv_id TEXT NOT NULL,
    seg_seq INTEGER NOT NULL,
    breach_at REAL NOT NULL,
    PRIMARY KEY (conv_id, seg_seq)
);
CREATE TABLE IF NOT EXISTS reminders (
    reminder_id TEXT PRIMARY KEY,
    conv_id TEXT NOT NULL,
    seg_seq INTEGER NOT NULL,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    due_at REAL NOT NULL,
    breach_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reminders_due ON reminders (state, due_at);
"""


class Engine:
    def __init__(self, db_path, sink, fallback):
        self._sink = sink
        self._fallback = fallback
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------------ API

    def message(self, event_id, conv, author, at):
        if author not in ("customer", "agent"):
            raise ValueError("unknown author: %r" % (author,))
        with self._conn as conn:
            if not self._record_event(conn, event_id):
                return
            row = self._get_conv(conn, conv)
            self._judge_conv(conn, row, at)
            if author == "customer":
                if not row["seg_open"]:
                    self._open_segment(conn, row, at)
            else:
                self._end_segment(conn, row)
            self._event_applied(conn)

    def set_status(self, event_id, conv, status, at):
        with self._conn as conn:
            if not self._record_event(conn, event_id):
                return
            row = self._get_conv(conn, conv)
            self._judge_conv(conn, row, at)
            if row["seg_open"]:
                accumulated = row["accumulated"]
                if row["status"] in ACTIVE_STATUSES:
                    accumulated += at - row["last_ts"]
                if status in ENDING_STATUSES:
                    self._end_segment(conn, row)
                elif status in ACTIVE_STATUSES and not row["seg_breached"]:
                    row["accumulated"] = accumulated
                    row["last_ts"] = at
                    row["deadline"] = at + (SEGMENT_LIMIT - accumulated)
                    conn.execute(
                        "UPDATE convs SET accumulated=?, last_ts=?, deadline=? "
                        "WHERE conv_id=?",
                        (accumulated, at, row["deadline"], conv))
                else:
                    row["accumulated"] = accumulated
                    row["last_ts"] = at
                    row["deadline"] = None
                    conn.execute(
                        "UPDATE convs SET accumulated=?, last_ts=?, deadline=NULL "
                        "WHERE conv_id=?",
                        (accumulated, at, conv))
            row["status"] = status
            conn.execute("UPDATE convs SET status=? WHERE conv_id=?",
                         (status, conv))
            self._event_applied(conn)

    def assign(self, event_id, conv, user, at):
        with self._conn as conn:
            if not self._record_event(conn, event_id):
                return
            row = self._get_conv(conn, conv)
            self._judge_conv(conn, row, at)
            conn.execute("UPDATE convs SET assignee=? WHERE conv_id=?",
                         (user, conv))
            self._event_applied(conn)

    def tick(self, now):
        conn = self._conn
        with conn:
            rows = conn.execute(
                "SELECT * FROM convs WHERE deadline IS NOT NULL AND deadline < ?",
                (now,)).fetchall()
            for row in rows:
                self._judge_conv(conn, dict(row), now)
        while True:
            with conn:
                rem = conn.execute(
                    "SELECT * FROM reminders "
                    "WHERE state='pending' AND due_at <= ? "
                    "ORDER BY due_at, reminder_id LIMIT 1",
                    (now,)).fetchone()
                if rem is None:
                    return
                rem = dict(rem)
                if rem["attempts"] >= MAX_ATTEMPTS:
                    conn.execute(
                        "UPDATE reminders SET state='exhausted' "
                        "WHERE reminder_id=?",
                        (rem["reminder_id"],))
                    continue
                # Count the attempt durably *before* calling the sink, so a
                # crash mid-call can never push a reminder past MAX_ATTEMPTS.
                conn.execute(
                    "UPDATE reminders SET attempts=attempts+1 "
                    "WHERE reminder_id=?",
                    (rem["reminder_id"],))
                arow = conn.execute(
                    "SELECT assignee FROM convs WHERE conv_id=?",
                    (rem["conv_id"],)).fetchone()
                recipient = arow["assignee"] if arow and arow["assignee"] \
                    else self._fallback
            try:
                self._call_sink(rem["reminder_id"], recipient, rem["conv_id"])
            except Exception:
                with conn:
                    if rem["attempts"] + 1 >= MAX_ATTEMPTS:
                        conn.execute(
                            "UPDATE reminders SET state='exhausted' "
                            "WHERE reminder_id=?",
                            (rem["reminder_id"],))
                    else:
                        conn.execute(
                            "UPDATE reminders SET due_at=? WHERE reminder_id=?",
                            (now + RETRY_DELAY, rem["reminder_id"]))
            else:
                with conn:
                    conn.execute(
                        "UPDATE reminders SET state='delivered' "
                        "WHERE reminder_id=?",
                        (rem["reminder_id"],))

    def breaches(self, conv):
        rows = self._conn.execute(
            "SELECT breach_at FROM breaches WHERE conv_id=? ORDER BY breach_at",
            (conv,)).fetchall()
        return [row["breach_at"] for row in rows]

    # -------------------------------------------------------------- internals

    def _call_sink(self, reminder_id, recipient, conv_id):
        self._sink(reminder_id, recipient, conv_id)

    def _event_applied(self, conn):
        """Hook invoked inside the event transaction, just before commit."""

    def _record_event(self, conn, event_id):
        cur = conn.execute(
            "INSERT OR IGNORE INTO events (event_id) VALUES (?)", (event_id,))
        return cur.rowcount > 0

    def _get_conv(self, conn, conv):
        row = conn.execute(
            "SELECT * FROM convs WHERE conv_id=?", (conv,)).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO convs (conv_id, status) VALUES (?, 'Open')",
                (conv,))
            row = conn.execute(
                "SELECT * FROM convs WHERE conv_id=?", (conv,)).fetchone()
        return dict(row)

    def _judge_conv(self, conn, row, now):
        """Record a breach if the open segment's deadline is strictly past."""
        if not row["seg_open"]:
            return
        deadline = row["deadline"]
        if deadline is None or not deadline < now:
            return
        conn.execute(
            "INSERT OR IGNORE INTO breaches (conv_id, seg_seq, breach_at) "
            "VALUES (?,?,?)",
            (row["conv_id"], row["seg_seq"], deadline))
        reminder_id = "%s#%d" % (row["conv_id"], row["seg_seq"])
        conn.execute(
            "INSERT OR IGNORE INTO reminders "
            "(reminder_id, conv_id, seg_seq, state, attempts, due_at, breach_at) "
            "VALUES (?,?,?,'pending',0,?,?)",
            (reminder_id, row["conv_id"], row["seg_seq"], deadline, deadline))
        conn.execute(
            "UPDATE convs SET deadline=NULL, seg_breached=1 WHERE conv_id=?",
            (row["conv_id"],))
        row["deadline"] = None
        row["seg_breached"] = 1

    def _open_segment(self, conn, row, at):
        row["seg_open"] = 1
        row["seg_seq"] += 1
        row["seg_breached"] = 0
        row["accumulated"] = 0
        row["last_ts"] = at
        row["deadline"] = at + SEGMENT_LIMIT \
            if row["status"] in ACTIVE_STATUSES else None
        conn.execute(
            "UPDATE convs SET seg_open=1, seg_seq=?, seg_breached=0, "
            "accumulated=0, last_ts=?, deadline=? WHERE conv_id=?",
            (row["seg_seq"], at, row["deadline"], row["conv_id"]))

    def _end_segment(self, conn, row):
        if not row["seg_open"]:
            return
        row["seg_open"] = 0
        row["deadline"] = None
        row["accumulated"] = 0
        conn.execute(
            "UPDATE convs SET seg_open=0, deadline=NULL, accumulated=0 "
            "WHERE conv_id=?",
            (row["conv_id"],))
        conn.execute(
            "UPDATE reminders SET state='cancelled' "
            "WHERE conv_id=? AND seg_seq=? AND state='pending'",
            (row["conv_id"], row["seg_seq"]))
