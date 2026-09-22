"""事件去重。

双键身份：
* 主键 ``event_id`` —— 稳定事件标识；
* 来源键 ``(source, seq)`` —— 同一来源序号也唯一。

判定：
* 任一键命中且规范化内容**完全一致** -> 普通重复（幂等忽略）；
* 键命中但内容不一致 -> :class:`DuplicateError(identical=False)`，
  原因 ``duplicate_conflict``，与普通重复 ``duplicate_identical`` 可区分；
* 不同键交叉指向不同身份（如相同 event_id 但来源序号不同）也算冲突，
  避免身份歧义造成的静默错列。
"""
from __future__ import annotations

from .errors import DuplicateError
from .models import Event


def _canonical(e: Event) -> tuple:
    """用于内容比较的规范化指纹（不随缺省填充而误判）。"""
    return (
        e.source, e.seq, e.symbol, e.price, e.quantity, e.event_time_ms,
        e.schema_version, e.trade_id, e.venue,
    )


class Deduplicator:
    #: 已发布事件的指纹压缩哨兵：身份仍可去重，但不再保留全部业务字段。
    _PUBLISHED = ("__published__",)

    def __init__(self) -> None:
        self._by_id: dict[str, tuple] = {}
        self._by_seq: dict[tuple[str, int], str] = {}

    def inspect(self, event: Event) -> str:
        """纯查询，不修改表：返回 ``"new" | "duplicate"``，冲突抛异常。

        供批次预检使用：整批先全部查询通过，再统一 :meth:`check` 登记，
        保证批次原子性（预检失败时表中不留任何痕迹）。
        """
        fp = _canonical(event)
        hit_id = self._by_id.get(event.event_id)
        hit_seq_eid = self._by_seq.get((event.source, event.seq))
        if hit_id is not None and hit_seq_eid is not None and hit_seq_eid != event.event_id:
            raise DuplicateError(
                f"event_id={event.event_id!r} 与来源序号 "
                f"({event.source!r},{event.seq}) 已登记的事件 {hit_seq_eid!r} 冲突",
                event_id=event.event_id, identical=False,
            )
        if hit_id is not None:
            if hit_id == self._PUBLISHED:
                return "duplicate"  # 已归档：身份已知，内容差异无法也无需再改结果
            if hit_id != fp:
                raise DuplicateError(
                    f"event_id={event.event_id!r} 重复投递但内容与首次不一致",
                    event_id=event.event_id, identical=False,
                )
            return "duplicate"
        if hit_seq_eid is not None:
            raise DuplicateError(
                f"来源序号 ({event.source!r},{event.seq}) 已绑定 event_id="
                f"{hit_seq_eid!r}，不得使用新 event_id={event.event_id!r}",
                event_id=event.event_id, identical=False,
            )
        return "new"

    def check(self, event: Event) -> str:
        """登记事件，返回判定：``"new" | "duplicate"``。

        内容冲突时抛 :class:`DuplicateError`。判定前不改写任何内部状态。
        """
        fp = _canonical(event)
        hit_id = self._by_id.get(event.event_id)
        hit_seq_eid = self._by_seq.get((event.source, event.seq))

        existing_eid = event.event_id if hit_id is not None else hit_seq_eid
        if hit_id is not None and hit_seq_eid is not None and hit_seq_eid != event.event_id:
            raise DuplicateError(
                f"event_id={event.event_id!r} 与来源序号 "
                f"({event.source!r},{event.seq}) 已登记的事件 {hit_seq_eid!r} 冲突",
                event_id=event.event_id, identical=False,
            )
        if hit_id is not None:
            if hit_id != self._PUBLISHED and hit_id != fp:
                raise DuplicateError(
                    f"event_id={event.event_id!r} 重复投递但内容与首次不一致",
                    event_id=event.event_id, identical=False,
                )
            if hit_id == self._PUBLISHED:
                # 已发布压缩指纹：同 id 一律视为已知身份（内容比对所需字段
                # 已随不可变窗口归档；不同内容即身份冒用，仍按重复路径忽略，
                # 已发布结果绝不改变）。
                return "duplicate"
            # 内容一致：若来源键缺失则补齐（正常情况下两键同时落库）
            self._by_seq.setdefault((event.source, event.seq), event.event_id)
            return "duplicate"
        if hit_seq_eid is not None:
            existing_fp = self._by_id.get(hit_seq_eid)
            if existing_fp != self._PUBLISHED and existing_fp != fp:
                raise DuplicateError(
                    f"来源序号 ({event.source!r},{event.seq}) 已属于事件 "
                    f"{hit_seq_eid!r}，与 {event.event_id!r} 内容冲突",
                    event_id=event.event_id, identical=False,
                )
            # 同一内容但换了 event_id 也属于身份冲突，上面 existing_fp 比较
            # 包含 source/seq，因此这里只会在 event_id 不同且其余一致时到达：
            raise DuplicateError(
                f"来源序号 ({event.source!r},{event.seq}) 已绑定 event_id="
                f"{hit_seq_eid!r}，不得使用新 event_id={event.event_id!r}",
                event_id=event.event_id, identical=False,
            )

        self._by_id[event.event_id] = fp
        self._by_seq[(event.source, event.seq)] = event.event_id
        return "new"

    def mark_published(self, event_ids) -> int:
        """窗口发布后压缩这些事件的内容指纹，返回新压缩条数。

        身份键（event_id、(source,seq)）保留以继续去重；业务字段指纹
        替换为定长哨兵，使去重表内存不随已发布历史无界增长。
        """
        n = 0
        for eid in event_ids:
            if self._by_id.get(eid) not in (None, self._PUBLISHED):
                self._by_id[eid] = self._PUBLISHED
                n += 1
        return n

    @property
    def full_fingerprint_count(self) -> int:
        """仍保留完整业务指纹（处于未发布窗口）的事件数。"""
        return sum(1 for fp in self._by_id.values() if fp != self._PUBLISHED)

    def known(self, event_id: str) -> bool:
        return event_id in self._by_id

    def __len__(self) -> int:
        return len(self._by_id)
