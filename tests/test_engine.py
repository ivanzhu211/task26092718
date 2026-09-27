"""性质测试 1：随机事件序列在三种 tick 频率下 breaches 一致，且与参考实现一致。"""

import os
import random
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

import pytest

from reminder_engine import Engine
from reference import reference_breaches

STATUSES = ["Open", "In Progress", "Pending", "Resolved", "Closed"]


def gen_events(seed, n_convs=6, n_events=300):
    """生成随机事件序列，覆盖 Pending 往返、改派、结案重开、恰在到期时刻的回复。"""
    rng = random.Random(seed)
    convs = [f"conv{i}" for i in range(n_convs)]
    status = {c: "Open" for c in convs}
    seg_open = {c: False for c in convs}
    forced = []  # (time, conv) 计划在到期时刻准点发出的客服回复
    events = []
    t = 0
    eid = 0

    def emit(kind, conv, arg, at):
        nonlocal eid
        events.append((kind, f"e{seed}-{eid}", conv, arg, at))
        eid += 1

    while eid < n_events:
        due = sorted(f for f in forced if f[0] <= t)
        if due and rng.random() < 0.8:
            _, conv = due[0]
            forced.remove(due[0])
            emit("message", conv, "agent", t)
            seg_open[conv] = False
            continue
        t += rng.choice([0, 0, 1, 5, 30, 120, 300, 900])
        conv = rng.choice(convs)
        kind = rng.choices(
            ["customer", "agent", "status", "assign"],
            weights=[30, 20, 35, 15])[0]
        if kind == "customer":
            emit("message", conv, "customer", t)
            if status[conv] not in ("Resolved", "Closed") and not seg_open[conv]:
                seg_open[conv] = True
                if status[conv] in ("Open", "In Progress") and rng.random() < 0.4:
                    forced.append((t + 3600, conv))  # 恰在到期时刻回复
        elif kind == "agent":
            emit("message", conv, "agent", t)
            seg_open[conv] = False
        elif kind == "status":
            new = rng.choice(STATUSES)
            emit("set_status", conv, new, t)
            old = status[conv]
            status[conv] = new
            if new in ("Resolved", "Closed"):
                seg_open[conv] = False
            elif new == "Pending" or old == "Pending":
                forced = [f for f in forced if f[1] != conv]  # 暂停后不再准点
        else:
            emit("assign", conv, f"user{rng.randrange(4)}", t)
    return events, t


def run_engine(db_path, events, tick_times):
    """按 (时间, tick 优先) 合并事件与 tick 后顺序调用引擎。"""
    sink_calls = []
    now_box = [0]

    def sink(rid, recipient, conv):
        sink_calls.append((rid, recipient, conv, now_box[0]))

    eng = Engine(db_path, sink, "fallback")
    ops = [(ev[-1], 1, ev) for ev in events]
    ops += [(tt, 0, ("tick", tt)) for tt in tick_times]
    ops.sort(key=lambda x: (x[0], x[1]))
    for at, _, payload in ops:
        now_box[0] = at
        if payload[0] == "tick":
            eng.tick(at)
        else:
            kind, eid, conv, arg, _ = payload
            if kind == "message":
                eng.message(eid, conv, arg, at)
            elif kind == "set_status":
                eng.set_status(eid, conv, arg, at)
            else:
                eng.assign(eid, conv, arg, at)
    return eng, sink_calls


def ticks_every(step, t_end):
    return list(range(1, t_end + 2, step))


@pytest.mark.parametrize("seed", [1, 2, 3, 7, 42])
def test_breaches_tick_frequency_independent(tmp_path, seed):
    events, t_end = gen_events(seed)
    schedules = [
        ticks_every(1, t_end),          # 每秒 tick
        ticks_every(17 * 60, t_end),    # 每 17 分钟 tick
        [t_end + 1],                    # 只在末尾 tick 一次
    ]
    all_breaches = []
    for i, ticks in enumerate(schedules):
        eng, sink_calls = run_engine(str(tmp_path / f"run{i}.db"), events, ticks)
        convs = {ev[2] for ev in events}
        got = {c: eng.breaches(c) for c in convs}
        all_breaches.append(got)

        # 与参考实现一致（参考实现吃同一份含 tick 的调用序列）
        ops = []
        for ev in events:
            ops.append(ev)
        ref_ops = sorted(
            [("tick", tt) for tt in ticks] + ops,
            key=lambda op: (op[-1], 0 if op[0] == "tick" else 1))
        assert got == {c: reference_breaches(ref_ops).get(c, []) for c in convs}

        # 每条提醒的 sink 调用不超过 4 次；同一等待段只有一个 reminder_id
        rids = [c[0] for c in sink_calls]
        assert len(rids) == len(set(rids))  # 无失败 sink：每条提醒至多投递一次

        # sink 调用不晚于对应等待段的结束
        db = sqlite3.connect(str(tmp_path / f"run{i}.db"))
        ends = {f"{c}:{s}": e for c, s, e in db.execute(
            "SELECT conv_id, seq, end_at FROM segments")}
        for rid, _r, _c, at in sink_calls:
            end = ends[rid]
            assert end is None or at <= end, (rid, at, end)
        # 未作废的提醒最终都已送达
        pending = db.execute(
            "SELECT COUNT(*) FROM reminders WHERE state='pending'").fetchone()[0]
        assert pending == 0
        db.close()
        eng.close()

    assert all_breaches[0] == all_breaches[1] == all_breaches[2]
    # 序列确实产生了超时，测试不是空转
    assert sum(len(v) for v in all_breaches[0].values()) > 0


def test_reply_exactly_at_deadline_cancels(tmp_path):
    calls = []
    eng = Engine(str(tmp_path / "t.db"),
                 lambda *a: calls.append(a), "fb")
    eng.message("e1", "c", "customer", 1000)
    eng.message("e2", "c", "agent", 1000 + 3600)  # 恰在到期时刻
    eng.tick(1000 + 3600)
    eng.tick(1000 + 7200)
    assert eng.breaches("c") == []
    assert calls == []


def test_retry_exhaustion_and_cancel(tmp_path):
    # 始终失败：4 次尝试后耗尽，不再调用
    calls = []
    eng = Engine(str(tmp_path / "t.db"),
                 lambda *a: (_ for _ in ()).throw(RuntimeError()) if not calls.append(a) else None,
                 "fb")
    eng.message("e1", "c", "customer", 0)
    for now in (3601, 3631, 3661, 3691, 3721, 10000):
        eng.tick(now)
    assert len(calls) == 4

    # 重试间隔内段结束：提醒作废，不再尝试
    calls2 = []
    state = {"fail": True}

    def flaky(*a):
        calls2.append(a)
        if state["fail"]:
            raise RuntimeError("boom")

    eng2 = Engine(str(tmp_path / "t2.db"), flaky, "fb")
    eng2.message("e1", "c", "customer", 0)
    eng2.tick(3601)          # 第 1 次失败，下次 3631
    eng2.message("e2", "c", "agent", 3650)  # 段结束，提醒作废
    state["fail"] = False
    eng2.tick(3631)
    eng2.tick(10000)
    assert len(calls2) == 1
    assert eng2.breaches("c") == [3600.0]


def test_tick_uses_due_indexes(tmp_path):
    """tick 的判定与投递查询只扫描到期项（走部分索引，不全表遍历）。"""
    eng = Engine(str(tmp_path / "t.db"), lambda *a: None, "fb")
    plan = eng._db.execute(
        "EXPLAIN QUERY PLAN SELECT conv_id, seq, deadline FROM segments "
        "WHERE state='open' AND deadline IS NOT NULL "
        "AND breach_at IS NULL AND deadline < 1").fetchall()
    assert any("idx_segments_due" in str(row) for row in plan), plan
    plan = eng._db.execute(
        "EXPLAIN QUERY PLAN SELECT r.reminder_id, r.conv_id, c.assignee "
        "FROM reminders r JOIN conversations c ON c.conv_id = r.conv_id "
        "WHERE r.state='pending' AND r.next_attempt_at <= 1 "
        "ORDER BY r.next_attempt_at, r.reminder_id LIMIT 1").fetchall()
    assert any("idx_reminders_due" in str(row) for row in plan), plan


def test_scale_10k_conversations(tmp_path):
    """一万个活跃会话：到期项之外的状态不影响 tick 结果。"""
    calls = []
    eng = Engine(str(tmp_path / "t.db"),
                 lambda rid, r, c: calls.append((rid, r, c)), "fb")
    n = 10000
    for i in range(n):
        eng.message(f"m{i}", f"conv{i}", "customer", 0)
    # 只有一个会话的客服没回复之外的会话全部及时回复
    for i in range(1, n):
        eng.message(f"a{i}", f"conv{i}", "agent", 100)
    eng.tick(3601)
    eng.tick(20000)
    assert eng.breaches("conv0") == [3600.0]
    assert calls == [("conv0:1", "fb", "conv0")]
    for i in range(1, n):
        assert eng.breaches(f"conv{i}") == []
