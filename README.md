# task26092718

客服会话首响超时提醒引擎。全部状态存于一个 SQLite 文件，不读系统时钟、
不用内存定时器与后台线程，崩溃重启后由调用方从头重放全部调用即可恢复。

## 用法

```python
from reminder_engine import Engine

def sink(reminder_id, recipient, conv_id):
    ...  # 投递；抛异常即失败，按 reminder_id 去重

eng = Engine("state.db", sink, fallback="oncall")
eng.message("evt-1", "conv-1", "customer", t0)   # author: customer / agent
eng.set_status("evt-2", "conv-1", "Pending", t1)  # Open / In Progress / Pending / Resolved / Closed
eng.assign("evt-3", "conv-1", "alice", t2)
eng.tick(t3)                                     # 推进时间，执行到期投递
eng.breaches("conv-1")                           # 该会话所有超时时刻
```

规则：上次客服回复后的第一条客户消息开启等待段；客服回复或进入
Resolved/Closed 结束。只在 Open / In Progress 下计时，Pending 暂停。
累计恰满 60 分钟为超时时刻，每段恰好一条提醒；发给尝试时刻的主负责人
（无则发给 fallback），失败 30 秒后重试，至多 4 次调用；段在投递成功前
结束则提醒作废。同一时刻先应用全部事件再判定超时（deadline 严格小于
当前时钟才判定），因此 `breaches` 与 tick 频率无关。

## 测试

```
python3 -m pytest tests/
```

- `tests/test_engine.py`：随机事件序列按每秒 / 每 17 分钟 / 仅末尾一次三种
  tick 频率各跑一遍，`breaches` 彼此相同且与独立参考实现
  （`tests/reference.py`）一致；另含准点回复、重试耗尽、作废、索引与
  一万会话的用例。
- `tests/test_crash.py` + `tests/crash_worker.py`：子进程中在 sink 调用中、
  sink 返回后未落盘、事件写入中途 SIGKILL，重启重放后断言三条不变量。
