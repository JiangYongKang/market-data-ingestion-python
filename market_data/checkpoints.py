"""来源位点（checkpoint）。

规则
----
* 位点按 ``source`` 各自维护，只增不减；
* ``advance(source, seq)`` 仅在 ``seq > 当前值`` 时生效（同值重复提交幂等，
  不报错、不写入）；
* 请求更小的值必须抛 :class:`CheckpointRollbackError`，
  **绝不**从中间继续、绝不产生重复；
* 落盘采用临时文件 + ``os.replace`` 原子替换，失败不留半截文件。

位点语义
--------
位点表示"该来源序号 <= seq 的事件均已被接入处理"。重放方据此
从 ``seq+1`` 开始续传；位点推进由服务在事件成功落盘/聚合后执行，
保证位点不超前于实际状态（失败时整体不生效）。
"""
from __future__ import annotations

import json
import os
import tempfile

from .errors import CheckpointRollbackError


class CheckpointStore:
    def __init__(self, data_dir: str) -> None:
        self._path: str | None = None
        self._points: dict[str, int] = {}
        if data_dir != ":memory:":
            os.makedirs(data_dir, exist_ok=True)
            self._path = os.path.join(data_dir, "checkpoints.json")
            if os.path.exists(self._path):
                with open(self._path, encoding="utf-8") as fh:
                    loaded = json.load(fh)
                self._points = {str(k): int(v) for k, v in loaded.items()}

    def get(self, source: str) -> int:
        return self._points.get(source, -1)

    def advance(self, source: str, seq: int) -> bool:
        cur = self.get(source)
        if seq < cur:
            raise CheckpointRollbackError(
                f"拒绝位点回退：source={source!r} 请求 {seq} < 已确认 {cur}",
                source=source, requested=seq, current=cur,
            )
        if seq == cur:
            return False  # 同值重复提交，幂等无操作
        self._points[source] = seq
        self._flush()
        return True

    def all(self) -> dict[str, int]:
        return dict(sorted(self._points.items()))

    def _flush(self) -> None:
        if self._path is None:
            return
        directory = os.path.dirname(self._path) or "."
        fd, tmp = tempfile.mkstemp(prefix=".ckpt-", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(self._points, fh, sort_keys=True, indent=2)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self._path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
