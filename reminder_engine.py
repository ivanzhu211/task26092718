"""客服会话首响超时提醒引擎。

全部状态持久化在一个 SQLite 文件中；崩溃重启后调用方从头重放全部调用即可恢复。
时间只取调用方传入的时间戳，不读系统时钟。

核心不变量：
- 变更操作按 event_id 去重，重复 id 为空操作（重放安全）。
- 等待段：上次客服回复后的第一条客户消息开启；客服回复或进入 Resolved/Closed
  时结束。只在 Open / In Progress 下计时，Pending 暂停。累计恰满 60 分钟的
  那一刻为超时时刻。
- 同一时刻先应用全部事件再判定超时：只有当时钟推进到严格大于某段的到期时刻
  （deadline < t）时，才判定该段是否超时。因此 at == deadline 的结束事件会
  取消本次超时，且判定结果与 tick 频率无关。
- 每个等待段至多一条提醒（reminder_id 确定性生成），投递失败 30 秒后重试，
  至多 4 次 sink 调用；尝试计数在调用 sink 之前落盘，崩溃也计入。
"""

import sqlite3

OPEN_STATUSES = ("Open", "In Progress")
PAUSE_STATUS = "Pending"
END_STATUSES = ("Resolved", "Closed")
ALL_STATUSES = OPEN_STATUSES + (PAUSE_STATUS,) + END_STATUSES

FIRST_RESPONSE_SLA = 3600.0   # 60 分钟
RETRY_INTERVAL = 30.0         # 失败后 30 秒重试
MAX_ATTEMPTS = 4              # 1 次首发 + 3 次重试

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS conversations (
    conv_id   TEXT PRIMARY KEY,
    status    TEXT NOT NULL,
    assignee  TEXT,
    seg_count INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS segments (
    conv_id      TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    state        TEXT NOT NULL,          -- 'open' | 'closed'
    accumulated  REAL NOT NULL,          -- 已累计的计时秒数（不含当前计时区间）
    active_since REAL,                   -- 非 NULL 表示正在计时
    deadline     REAL,                   -- 非 NULL 表示到期时刻（暂停/结束时为 NULL）
    breach_at    REAL,                   -- 判定出的超时时刻
    end_at       REAL,
    PRIMARY KEY (conv_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_segments_due
    ON segments(deadline)
    WHERE state = 'open' AND deadline IS NOT NULL AND breach_at IS NULL;
CREATE TABLE IF NOT EXISTS reminders (
    reminder_id      TEXT PRIMARY KEY,   -- '<conv_id>:<seq>'，每段恰好一条
    conv_id          TEXT NOT NULL,
    seg_seq          INTEGER NOT NULL,
    state            TEXT NOT NULL,      -- 'pending' | 'delivered' | 'cancelled' | 'exhausted'
    started_attempts INTEGER NOT NULL,   -- 已开始的 sink 调用次数（含被崩溃打断的）
    next_attempt_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reminders_due
    ON reminders(next_attempt_at)
    WHERE state = 'pending';
"""


class Engine:
    """首响超时提醒引擎。

    sink(reminder_id, recipient, conv_id) 由调用方注入，抛异常即投递失败。
    fallback 为无主负责人时的兜底接收人。
    """

    def __init__(self, db_path, sink, fallback):
        self._sink = sink
        self._fallback = fallback
        self._db = sqlite3.connect(db_path)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._init_schema()

    def _init_schema(self):
        with self._db:
            self._db.executescript(_SCHEMA)

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------
    def _is_fresh(self, cur, event_id):
        """按 event_id 去重；已见过返回 False。"""
        cur.execute("INSERT OR IGNORE INTO events(event_id) VALUES (?)",
                    (event_id,))
        return cur.rowcount == 1

    def _ensure_conv(self, cur, conv):
        cur.execute(
            "INSERT OR IGNORE INTO conversations(conv_id, status) VALUES (?, 'Open')",
            (conv,))

    def _judge(self, cur, t):
        """判定所有到期时刻严格早于 t 的未判定等待段。

        只访问到期项（索引扫描），与总会话数无关。幂等，可重复调用。
        """
        cur.execute(
            "SELECT conv_id, seq, deadline FROM segments "
            "WHERE state='open' AND deadline IS NOT NULL "
            "  AND breach_at IS NULL AND deadline < ?",
            (t,))
        due = cur.fetchall()
        for conv_id, seq, deadline in due:
            cur.execute(
                "UPDATE segments SET breach_at=? "
                "WHERE conv_id=? AND seq=?",
                (deadline, conv_id, seq))
            cur.execute(
                "INSERT OR IGNORE INTO reminders"
                "(reminder_id, conv_id, seg_seq, state, started_attempts,"
                " next_attempt_at) VALUES (?,?,?, 'pending', 0, ?)",
                (f"{conv_id}:{seq}", conv_id, seq, deadline))

    def _open_segment(self, cur, conv, status, at):
        """客户消息触发：若当前无开启中的等待段且会话未终结，则开新段。"""
        if status in END_STATUSES:
            return
        cur.execute(
            "SELECT 1 FROM segments WHERE conv_id=? AND state='open' LIMIT 1",
            (conv,))
        if cur.fetchone():
            return
        cur.execute(
            "UPDATE conversations SET seg_count = seg_count + 1 WHERE conv_id=?",
            (conv,))
        cur.execute("SELECT seg_count FROM conversations WHERE conv_id=?", (conv,))
        seq = cur.fetchone()[0]
        if status in OPEN_STATUSES:
            active_since, deadline = at, at + FIRST_RESPONSE_SLA
        else:  # Pending：段开启但计时暂停
            active_since, deadline = None, None
        cur.execute(
            "INSERT INTO segments"
            "(conv_id, seq, state, accumulated, active_since, deadline)"
            " VALUES (?,?, 'open', 0.0, ?, ?)",
            (conv, seq, active_since, deadline))

    def _close_segment(self, cur, conv, at):
        """结束开启中的等待段，并作废其未投递提醒。"""
        cur.execute(
            "SELECT seq FROM segments WHERE conv_id=? AND state='open' LIMIT 1",
            (conv,))
        row = cur.fetchone()
        if not row:
            return
        seq = row[0]
        cur.execute(
            "UPDATE segments SET state='closed', end_at=?,"
            " active_since=NULL, deadline=NULL"
            " WHERE conv_id=? AND seq=?",
            (at, conv, seq))
        cur.execute(
            "UPDATE reminders SET state='cancelled'"
            " WHERE reminder_id=? AND state='pending'",
            (f"{conv}:{seq}",))

    # ------------------------------------------------------------------
    # 变更操作
    # ------------------------------------------------------------------
    def message(self, event_id, conv, author, at):
        """记录一条消息；author 为 'customer' 或 'agent'。"""
        if author not in ("customer", "agent"):
            raise ValueError(f"unknown author: {author!r}")
        with self._db:
            cur = self._db.cursor()
            if not self._is_fresh(cur, event_id):
                return
            self._judge(cur, at)
            self._ensure_conv(cur, conv)
            if author == "customer":
                cur.execute("SELECT status FROM conversations WHERE conv_id=?",
                            (conv,))
                self._open_segment(cur, conv, cur.fetchone()[0], at)
            else:
                self._close_segment(cur, conv, at)

    def set_status(self, event_id, conv, status, at):
        """设置会话状态：Open / In Progress / Pending / Resolved / Closed。"""
        if status not in ALL_STATUSES:
            raise ValueError(f"unknown status: {status!r}")
        with self._db:
            cur = self._db.cursor()
            if not self._is_fresh(cur, event_id):
                return
            self._judge(cur, at)
            self._ensure_conv(cur, conv)
            cur.execute("SELECT status FROM conversations WHERE conv_id=?",
                        (conv,))
            old = cur.fetchone()[0]
            cur.execute("UPDATE conversations SET status=? WHERE conv_id=?",
                        (status, conv))
            if status in END_STATUSES:
                self._close_segment(cur, conv, at)
                return
            cur.execute(
                "SELECT seq, accumulated, active_since FROM segments"
                " WHERE conv_id=? AND state='open' LIMIT 1",
                (conv,))
            seg = cur.fetchone()
            if not seg:
                return
            seq, accumulated, active_since = seg
            if status == PAUSE_STATUS and old != PAUSE_STATUS:
                if active_since is not None:
                    accumulated += at - active_since
                cur.execute(
                    "UPDATE segments SET accumulated=?, active_since=NULL,"
                    " deadline=NULL WHERE conv_id=? AND seq=?",
                    (accumulated, conv, seq))
            elif status in OPEN_STATUSES and old == PAUSE_STATUS:
                cur.execute(
                    "UPDATE segments SET active_since=?, deadline=?"
                    " WHERE conv_id=? AND seq=?",
                    (at, at + (FIRST_RESPONSE_SLA - accumulated), conv, seq))

    def assign(self, event_id, conv, user, at):
        """设置会话主负责人。"""
        with self._db:
            cur = self._db.cursor()
            if not self._is_fresh(cur, event_id):
                return
            self._judge(cur, at)
            self._ensure_conv(cur, conv)
            cur.execute("UPDATE conversations SET assignee=? WHERE conv_id=?",
                        (user, conv))

    # ------------------------------------------------------------------
    # 时钟推进与投递
    # ------------------------------------------------------------------
    def tick(self, now):
        """推进时间，执行到期投递。"""
        with self._db:
            self._judge(self._db.cursor(), now)
        while True:
            due = self._claim_next_due(now)
            if due is None:
                return
            reminder_id, recipient, conv_id = due
            try:
                self._sink(reminder_id, recipient, conv_id)
            except Exception:
                self._mark_failure(reminder_id)
            else:
                self._mark_success(reminder_id)

    def _claim_next_due(self, now):
        """取出最早到期的一条待投递提醒并原子地登记一次尝试。

        尝试计数与下次尝试时刻在调用 sink 之前落盘：崩溃打断的调用也计入
        总数，保证每条提醒的 sink 调用不超过 MAX_ATTEMPTS 次。
        """
        with self._db:
            cur = self._db.cursor()
            cur.execute(
                "SELECT r.reminder_id, r.conv_id, c.assignee"
                " FROM reminders r JOIN conversations c"
                "   ON c.conv_id = r.conv_id"
                " WHERE r.state='pending' AND r.next_attempt_at <= ?"
                " ORDER BY r.next_attempt_at, r.reminder_id LIMIT 1",
                (now,))
            row = cur.fetchone()
            if row is None:
                return None
            reminder_id, conv_id, assignee = row
            cur.execute(
                "UPDATE reminders SET started_attempts = started_attempts + 1,"
                " next_attempt_at = ?"
                " WHERE reminder_id=? AND state='pending'",
                (now + RETRY_INTERVAL, reminder_id))
            if cur.rowcount != 1:
                return None
            recipient = assignee if assignee is not None else self._fallback
            return reminder_id, recipient, conv_id

    def _mark_success(self, reminder_id):
        with self._db:
            self._db.execute(
                "UPDATE reminders SET state='delivered' WHERE reminder_id=?",
                (reminder_id,))

    def _mark_failure(self, reminder_id):
        with self._db:
            self._db.execute(
                "UPDATE reminders SET state = CASE"
                " WHEN started_attempts >= ? THEN 'exhausted' ELSE 'pending' END"
                " WHERE reminder_id=?",
                (MAX_ATTEMPTS, reminder_id))

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def breaches(self, conv):
        """返回该会话所有超时时刻（升序）。"""
        cur = self._db.execute(
            "SELECT breach_at FROM segments"
            " WHERE conv_id=? AND breach_at IS NOT NULL ORDER BY breach_at",
            (conv,))
        return [row[0] for row in cur.fetchall()]

    def close(self):
        self._db.close()
