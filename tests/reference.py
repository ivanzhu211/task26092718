"""独立参考实现：纯内存、不计性能，直接按语义模拟。

输入为调用序列（事件与 tick 交错），输出每个会话的超时时刻列表。
与引擎共用同一条判定规则：时钟推进到 t 时，先应用 at <= t 的全部事件，
再判定 deadline 严格小于 t 的等待段。
"""

SLA = 3600.0
OPEN = ("Open", "In Progress")
END = ("Resolved", "Closed")


def reference_breaches(ops):
    convs = {}
    breaches = {}

    def ensure(conv):
        if conv not in convs:
            convs[conv] = {"status": "Open", "seg": None}

    def judge(t):
        for conv, st in convs.items():
            seg = st["seg"]
            if (seg is not None and seg["deadline"] is not None
                    and not seg["breached"] and seg["deadline"] < t):
                seg["breached"] = True
                breaches.setdefault(conv, []).append(seg["deadline"])

    for op in ops:
        kind = op[0]
        if kind == "tick":
            judge(op[1])
            continue
        _, _eid, conv = op[0], op[1], op[2]
        at = op[-1]
        judge(at)
        ensure(conv)
        st = convs[conv]
        if kind == "message":
            author = op[3]
            if author == "customer":
                if st["status"] not in END and st["seg"] is None:
                    active = st["status"] in OPEN
                    st["seg"] = {
                        "acc": 0.0,
                        "active_since": at if active else None,
                        "deadline": (at + SLA) if active else None,
                        "breached": False,
                    }
            else:
                st["seg"] = None
        elif kind == "set_status":
            status = op[3]
            old = st["status"]
            st["status"] = status
            seg = st["seg"]
            if status in END:
                st["seg"] = None
            elif seg is not None:
                if status == "Pending" and old != "Pending":
                    if seg["active_since"] is not None:
                        seg["acc"] += at - seg["active_since"]
                    seg["active_since"] = None
                    seg["deadline"] = None
                elif status in OPEN and old == "Pending":
                    seg["active_since"] = at
                    seg["deadline"] = at + (SLA - seg["acc"])
        elif kind == "assign":
            pass
        else:
            raise ValueError(kind)
    return breaches
