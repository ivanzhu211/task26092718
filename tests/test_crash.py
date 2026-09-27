"""性质测试 2：SIGKILL 崩溃后重启重放，三条不变量仍成立。

- 超时记录与未崩溃运行一致；
- 每条提醒的 sink 调用不超过 4 次（被崩溃打断的也算），同一等待段只有一个 reminder_id；
- sink 调用不晚于对应等待段的结束；未作废的提醒最终送达或耗尽重试。
"""

import json
import os
import signal
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
WORKER = os.path.join(HERE, "crash_worker.py")

CRASH_SCENARIOS = [
    "in_sink_before_log",   # sink 调用中（写日志前）崩溃
    "in_sink_after_log",    # sink 调用中（写日志后）崩溃
    "after_sink_return",    # sink 返回后、结果落盘前崩溃
    "mid_event",            # 事件写入中途崩溃
]


def run_worker(tmp_path, name, scenario, failn):
    db = str(tmp_path / f"{name}.db")
    log = tmp_path / f"{name}.sinklog"
    out = tmp_path / f"{name}.json"
    log.touch()
    proc = subprocess.run(
        [sys.executable, WORKER, db, str(log), str(out), scenario, str(failn)],
        capture_output=True, text=True)
    return proc, db, log, out


def load_sink_log(path):
    return [json.loads(line) for line in open(path) if line.strip()]


def assert_invariants(db_out, sink_entries, clean_breaches):
    # 超时记录与未崩溃运行一致
    assert db_out["breaches"] == clean_breaches

    reminders = db_out["reminders"]
    segments = db_out["segments"]
    breached = [(c, s) for c, s, _st, b, _e in segments if b is not None]

    # 同一等待段不产生第二个 reminder_id；每个超时段恰好一条提醒
    assert len(reminders) == len({(r[1], r[2]) for r in reminders})
    assert sorted((r[1], r[2]) for r in reminders) == sorted(breached)

    by_rid = {}
    for e in sink_entries:
        by_rid.setdefault(e["rid"], []).append(e)

    ends = {f"{c}:{s}": end for c, s, _st, _b, end in segments}
    for rid, _conv, _seq, state, started, _next in reminders:
        entries = by_rid.get(rid, [])
        # 每条提醒的 sink 调用不超过 4 次（日志计不到的由落盘的尝试计数兜底）
        assert len(entries) <= 4, (rid, len(entries))
        assert started <= 4, (rid, started)
        # 未作废的提醒最终必须送达或耗尽重试
        assert state in ("delivered", "cancelled", "exhausted"), (rid, state)
        if state == "delivered":
            assert any(e["result"] == "ok" for e in entries), rid
        if state == "exhausted":
            assert started == 4, rid
        # sink 调用不晚于对应等待段的结束
        for e in entries:
            end = ends[rid]
            assert end is None or e["t"] <= end, (rid, e["t"], end)


def test_clean_run_baseline(tmp_path):
    proc, _db, log, out = run_worker(tmp_path, "clean", "clean", failn=2)
    assert proc.returncode == 0, proc.stderr
    db_out = json.loads(out.read_text())
    entries = load_sink_log(log)
    # 基线自身也满足不变量（与自身比较）
    assert_invariants(db_out, entries, db_out["breaches"])
    # 每条提醒恰好 3 次尝试（前 2 次配置为失败）
    for rid, _c, _s, state, started, _n in db_out["reminders"]:
        if state == "delivered":
            assert started == 3


def test_crash_and_replay(tmp_path):
    proc, _db, _log, out = run_worker(tmp_path, "base", "clean", failn=2)
    assert proc.returncode == 0, proc.stderr
    clean_breaches = json.loads(out.read_text())["breaches"]

    for i, scenario in enumerate(CRASH_SCENARIOS):
        # 第一遍：跑到指定点被 SIGKILL
        proc, db, log, out = run_worker(tmp_path, f"crash{i}", scenario, failn=0)
        assert proc.returncode == -signal.SIGKILL, (scenario, proc.returncode)

        # 第二遍：重启后从头重放全部调用，必须跑完
        proc, db, log, out = run_worker(tmp_path, f"crash{i}", "resume", failn=0)
        assert proc.returncode == 0, (scenario, proc.stderr)
        db_out = json.loads(out.read_text())
        entries = load_sink_log(log)
        assert_invariants(db_out, entries, clean_breaches)

        # 第三遍：再次重放应为完全空操作，不产生新的 sink 调用
        before = log.read_text()
        proc, db, log, out2 = run_worker(tmp_path, f"crash{i}", "resume", failn=0)
        assert proc.returncode == 0, (scenario, proc.stderr)
        assert log.read_text() == before
        assert json.loads(out2.read_text())["breaches"] == clean_breaches
