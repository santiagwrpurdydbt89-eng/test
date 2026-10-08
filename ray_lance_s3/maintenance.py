"""基于 lance-ray 的分布式数据集运维：加列、合并小文件、建索引、版本管理。"""

from __future__ import annotations

from typing import Any, Callable

import pyarrow as pa


def add_columns(
    uri: str,
    transform: Callable[[pa.RecordBatch], pa.RecordBatch],
    storage_options: dict[str, str] | None = None,
    *,
    read_columns: list[str] | None = None,
    batch_size: int = 1024,
    concurrency: int | None = None,
) -> None:
    """按 fragment 并行计算新列并写回（不重写已有列的数据文件）。

    ``transform`` 接收只包含 ``read_columns`` 的 RecordBatch，返回仅包含新列的
    RecordBatch，行数必须一致。
    """
    import lance_ray as lr

    lr.add_columns(
        uri,
        transform=transform,
        read_columns=read_columns,
        storage_options=storage_options,
        batch_size=batch_size,
        concurrency=concurrency,
    )


def compact_files(
    uri: str,
    storage_options: dict[str, str] | None = None,
    *,
    target_rows_per_fragment: int = 1024 * 1024,
    num_workers: int = 4,
):
    """用 Ray 并行合并小 fragment（频繁 append 之后建议执行）。"""
    import lance_ray as lr
    from lance.optimize import CompactionOptions

    return lr.compact_files(
        uri,
        compaction_options=CompactionOptions(target_rows_per_fragment=target_rows_per_fragment),
        num_workers=num_workers,
        storage_options=storage_options,
    )


def create_scalar_index(
    uri: str,
    column: str,
    storage_options: dict[str, str] | None = None,
    *,
    index_type: str = "BTREE",
    num_workers: int = 4,
    **kwargs: Any,
):
    """分布式构建标量索引（BTREE / BITMAP / INVERTED(FTS) 等）。"""
    import lance_ray as lr

    return lr.create_scalar_index(
        uri,
        column=column,
        index_type=index_type,
        num_workers=num_workers,
        storage_options=storage_options,
        **kwargs,
    )


def list_versions(uri: str, storage_options: dict[str, str] | None = None) -> list[dict]:
    """列出数据集全部版本（每次写入/覆盖/加列都会生成新版本）。"""
    import lance

    return lance.dataset(uri, storage_options=storage_options).versions()
