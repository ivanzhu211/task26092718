# task26092718

客服会话首响超时提醒引擎。全部状态存于单个 SQLite 文件，无内存定时器、
无后台线程、不读系统时钟；崩溃重启后仅凭数据库文件恢复，调用方从头重放即可。

## 用法

```python
from reminder_engine import Engine

def sink(reminder_id, recipient, conv_id):
    send_notification(recipient, conv_id)  # 抛异常即投递失败

eng = Engine("state.db", sink, fallback="on-call")
eng.message("evt-1", "conv-1", "customer", at=1000)
eng.assign("evt-2", "conv-1", "agent-7", at=1005)
eng.set_status("evt-3", "conv-1", "In Progress", at=1010)
eng.tick(5000)                  # 推进时间，执行到期投递/重试
eng.breaches("conv-1")          # 该会话所有超时时刻
```

## 语义要点

- 等待段：上次客服回复后的首条客户消息开启，客服回复或进入 Resolved/Closed
  结束；只在 Open / In Progress 下计时，其余状态暂停。累计恰满 3600 秒即超时，
  每段至多一条提醒（`reminder_id = "<conv>#<段序号>"`）。
- 同一时刻先应用全部事件再判定超时：时刻 T 的超时在时间严格越过 T 时才判定。
- 投递发给尝试时刻的主负责人，无则发给 `fallback`；失败 30 秒后重试，
  每条提醒至多 4 次 sink 调用；段在投递成功前结束则提醒作废。
- 变更操作按 `event_id` 去重，重复 id 为空操作；`tick` 只触达到期项
  （`deadline`、`(state, due_at)` 索引），不遍历全部会话。

## 测试

```
python3 -m unittest discover -s tests -v
```

- `tests/test_engine.py`：随机事件序列（含 Pending 往返、改派、结案重开、
  恰在到期时刻的回复）按每秒 / 每 17 分钟 / 仅末尾三种 tick 频率各跑一遍，
  `breaches` 彼此相同并与独立参考实现 `tests/reference.py` 一致。
- `tests/test_crash.py`：子进程分别在 sink 调用中、sink 返回后未落盘、
  事件事务中途 SIGKILL，重启重放后断言超时记录一致、每条提醒 sink 调用
  不超过 4 次、每段至多一个 `reminder_id`、未作废提醒均送达或耗尽重试。
