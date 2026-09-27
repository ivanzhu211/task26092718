"""崩溃测试子进程脚本。

用法:
    python crash_worker.py <db> <sinklog> <out_json> <scenario> <failn>

scenario: clean | in_sink_before_log | in_sink_after_log | after_sink_return | mid_event | resume
failn:    本进程内每条提醒的前 failn 次 sink 调用抛异常

run 类场景跑脚本并在指定点 SIGKILL 自己；resume 在同一数据库上从头重放全部
调用并跑完。结束后把 breaches 与 reminders 表转储为 JSON 供父进程断言。
"""

import json
import os
import signal
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from reminder_engine import Engine

NOW = [0]


def script_ops():
    """确定性脚本：覆盖暂停/恢复、改派、准点回复取消、结案重开、投递前作废。"""
    ops = []

    def msg(eid, conv, author, at):
        ops.append(("message", eid, conv, author, at))

    def st(eid, conv, status, at):
        ops.append(("set_status", eid, conv, status, at))

    def asg(eid, conv, user, at):
        ops.append(("assign", eid, conv, user, at))

    asg("e01", "c1", "alice", 0)
    msg("e02", "c1", "customer", 0)          # c1:1 开启，deadline 3600
    msg("e03", "c2", "customer", 50)         # c2:1 开启，deadline 3650
    st("e04", "c1", "Pending", 100)          # 暂停，累计 100
    st("e05", "c1", "Open", 200)             # 恢复，deadline 3700
    msg("e06", "c3", "customer", 300)
    msg("e07", "c3", "agent", 400)           # c3 段及时结束，无超时
    msg("e08", "c4", "customer", 500)        # c4:1，deadline 4100
    msg("e09", "c2", "customer", 1400)       # c2:1 仍开启（同一等待段）
    asg("e10", "c1", "bob", 3600)            # 改派：c1:1 投递时负责人为 bob
    st("e11", "c2", "Resolved", 3651)        # 超时(3650)判定后立刻作废，不投递
    msg("e12", "c4", "agent", 4100)          # 恰在到期时刻回复：c4:1 无超时
    msg("e13", "c4", "customer", 4200)       # c4:2，deadline 7800
    st("e14", "c4", "Resolved", 4300)        # c4:2 结束，无超时
    st("e15", "c4", "Open", 4400)            # 重开
    msg("e16", "c4", "customer", 4500)       # c4:3，deadline 8100
    msg("e17", "c5", "customer", 10000)      # c5:1，deadline 13600
    ticks = sorted(set(list(range(250, 20001, 250)) + [3700, 5000, 8100]))
    # 按时间归并，同一时刻事件先于 tick（时间戳单调不减）
    merged = [(op[-1], 0, op) for op in ops]
    merged += [(t, 1, ("tick", t)) for t in ticks]
    merged.sort(key=lambda x: (x[0], x[1]))
    return [op for _, _, op in merged]


class Sink:
    """跨进程记录每次调用；按 failn 配置失败；可按场景在调用中自杀。"""

    def __init__(self, log_path, scenario, failn):
        self.log_path = log_path
        self.scenario = scenario
        self.failn = failn
        self.calls_per_rid = {}
        self.total = 0

    def __call__(self, rid, recipient, conv):
        self.total += 1
        if self.scenario == "in_sink_before_log" and self.total == 1:
            os.kill(os.getpid(), signal.SIGKILL)
        n = self.calls_per_rid.get(rid, 0) + 1
        self.calls_per_rid[rid] = n
        ok = n > self.failn
        with open(self.log_path, "a") as f:
            f.write(json.dumps({
                "rid": rid, "recipient": recipient, "conv": conv,
                "t": NOW[0], "result": "ok" if ok else "fail",
            }) + "\n")
            f.flush()
            os.fsync(f.fileno())
        if self.scenario == "in_sink_after_log" and self.total == 1:
            os.kill(os.getpid(), signal.SIGKILL)
        if not ok:
            raise RuntimeError("configured sink failure")


class KillAfterFirstSuccess(Engine):
    """sink 返回后、结果落盘前自杀。"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._done = False

    def _mark_success(self, reminder_id):
        if not self._done:
            self._done = True
            os.kill(os.getpid(), signal.SIGKILL)
        super()._mark_success(reminder_id)


class KillMidEvent(Engine):
    """第若干次事件写入的中途（事务未提交）自杀。"""

    KILL_AFTER = 12

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._n = 0

    def _is_fresh(self, cur, event_id):
        fresh = super()._is_fresh(cur, event_id)
        self._n += 1
        if self._n >= self.KILL_AFTER:
            os.kill(os.getpid(), signal.SIGKILL)
        return fresh


def main():
    db_path, sink_log, out_json, scenario, failn = (
        sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5]))
    sink = Sink(sink_log, scenario, failn)
    cls = {"after_sink_return": KillAfterFirstSuccess,
           "mid_event": KillMidEvent}.get(scenario, Engine)
    eng = cls(db_path, sink, "fallback")

    for op in script_ops():
        NOW[0] = op[-1]
        if op[0] == "tick":
            eng.tick(op[1])
        elif op[0] == "message":
            _, eid, conv, author, at = op
            eng.message(eid, conv, author, at)
        elif op[0] == "set_status":
            _, eid, conv, status, at = op
            eng.set_status(eid, conv, status, at)
        else:
            _, eid, conv, user, at = op
            eng.assign(eid, conv, user, at)

    convs = ["c1", "c2", "c3", "c4", "c5"]
    reminders = eng._db.execute(
        "SELECT reminder_id, conv_id, seg_seq, state, started_attempts,"
        " next_attempt_at FROM reminders ORDER BY reminder_id").fetchall()
    segments = eng._db.execute(
        "SELECT conv_id, seq, state, breach_at, end_at FROM segments"
        " ORDER BY conv_id, seq").fetchall()
    with open(out_json, "w") as f:
        json.dump({
            "breaches": {c: eng.breaches(c) for c in convs},
            "reminders": reminders,
            "segments": segments,
        }, f)
    eng.close()


if __name__ == "__main__":
    main()
