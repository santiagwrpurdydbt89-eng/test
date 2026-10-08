"""Lance Blob V2 大字段（图片、音频、视频等）在 Ray + S3 上的读写。

Blob V2 按每个值的大小自动选择存储位置（pylance 13 实测默认阈值）：

=========== ======================== ==========================================
kind        大小                      存储位置
=========== ======================== ==========================================
0 inline    <= 64 KiB                与普通列一起放在 ``.lance`` 数据文件中
1 packed    64 KiB ~ 4 MiB           多个值拼接进同一个 ``.blob`` 文件（默认上限 1 GiB）
2 dedicated > 4 MiB                  每个值单独一个 ``.blob`` 文件
3 external  外部 URI                  只记录 ``s3://`` / ``file://`` 地址
=========== ======================== ==========================================

要求 ``data_storage_version >= "2.2"``。

在 Ray 中使用时要注意两点（Ray 2.59 + lance-ray 0.5 + pylance 13 实测）：

- 写入：Ray 里的 ``bytes`` 列不能直接按 blob schema 写入（报
  ``Unsupported cast from binary to struct``），需要先用 :func:`to_blob_columns`
  在 ``map_batches`` 里转换成 blob 类型。
- 读取：``lance_ray.read_lance`` 会把 blob 列的完整内容读进 Ray object store；
  ``ray.data.read_lance`` 则只返回描述信息。按需读取内容请用 :func:`read_with_blobs`。
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Callable

import pyarrow as pa
import ray
import ray.data

BLOB_KINDS = {0: "inline", 1: "packed", 2: "dedicated", 3: "external"}
MIN_STORAGE_VERSION = "2.2"


def blob_field(
    name: str,
    *,
    inline_size_threshold: int | None = None,
    dedicated_size_threshold: int | None = None,
    pack_file_size_threshold: int | None = None,
) -> pa.Field:
    """构造 blob 列的 Arrow field，可按列调整三个阈值（字节）。

    - ``inline_size_threshold``：不超过该大小的值内联存放（0 表示从不内联）；
    - ``dedicated_size_threshold``：超过该大小的值单独存一个文件；
    - ``pack_file_size_threshold``：单个 packed ``.blob`` 文件的大小上限。
    """
    from lance.blob import blob_field as _blob_field

    return _blob_field(
        name,
        inline_size_threshold=inline_size_threshold,
        dedicated_size_threshold=dedicated_size_threshold,
        pack_file_size_threshold=pack_file_size_threshold,
    )


def to_blob_columns(
    columns: list[str] | dict[str, pa.Field],
) -> Callable[[pa.Table], pa.Table]:
    """返回一个 ``map_batches(..., batch_format="pyarrow")`` 用的转换函数。

    把 ``columns`` 中的列转换成 blob 类型。列中每个值可以是 ``bytes``
    （内容）、``str``（外部 URI，按 external 方式引用）或 ``None``。
    传 ``dict`` 时可为每列指定 :func:`blob_field` 生成的 field 以自定义阈值。
    """
    fields = columns if isinstance(columns, dict) else {c: blob_field(c) for c in columns}

    def convert(tbl: pa.Table) -> pa.Table:
        from lance.blob import blob_array

        for name, field in fields.items():
            i = tbl.schema.get_field_index(name)
            tbl = tbl.set_column(i, field, blob_array(tbl.column(name).to_pylist()))
        return tbl

    return convert


def write_blob_dataset(
    ds: ray.data.Dataset,
    uri: str,
    blob_columns: list[str] | dict[str, pa.Field],
    storage_options: dict[str, str] | None = None,
    *,
    mode: str = "create",
    data_storage_version: str = MIN_STORAGE_VERSION,
    **kwargs: Any,
) -> None:
    """把含 bytes / URI 列的 Ray Dataset 以 Blob V2 格式写入（lance-ray）。"""
    import lance_ray as lr

    lr.write_lance(
        ds.map_batches(to_blob_columns(blob_columns), batch_format="pyarrow"),
        uri,
        mode=mode,
        data_storage_version=data_storage_version,
        storage_options=storage_options,
        **kwargs,
    )


def blob_columns_of(schema: pa.Schema) -> list[str]:
    """返回 schema 中的 blob 列名。"""
    from lance.blob import BlobType

    return [
        f.name
        for f in schema
        if isinstance(f.type, BlobType) or (f.metadata or {}).get(b"lance-encoding:blob") == b"true"
    ]


def read_with_blobs(
    uri: str,
    blob_column: str,
    storage_options: dict[str, str] | None = None,
    *,
    columns: list[str] | None = None,
    filter: str | None = None,
    batch_size: int = 64,
    **read_kwargs: Any,
) -> ray.data.Dataset:
    """先只读普通列和行地址，再在每个 batch 里用 ``read_blobs`` 批量取 blob 内容。

    这样过滤、裁剪等操作都在不带 blob 内容的轻量数据上完成，只有真正需要的
    行才会从 S3 读取 blob；``read_blobs`` 会在 Rust 层合并相邻请求、控制并发。
    ``batch_size`` 控制每批读多少个 blob，blob 越大应设置得越小。
    """
    import lance
    import lance_ray as lr

    dataset = lance.dataset(uri, storage_options=storage_options)
    version = dataset.version
    if columns is None:
        blobs = set(blob_columns_of(dataset.schema))
        columns = [f.name for f in dataset.schema if f.name not in blobs]
    # fetch 会被序列化到 worker 上执行：只引用 pyarrow / lance，不引用本模块，
    # 这样集群节点上不安装 ray_lance_s3 也能运行。
    cache: dict[str, Any] = {}

    def fetch(batch: pa.Table) -> pa.Table:
        import lance

        ds = cache.get("ds")
        if ds is None:  # 同一任务内的多个 batch 复用，并固定在同一版本
            ds = cache["ds"] = lance.dataset(uri, version=version, storage_options=storage_options)
        addresses = batch.column("_rowaddr").to_pylist()
        blobs = ds.read_blobs(blob_column, addresses=addresses, preserve_order=True)
        return batch.append_column(blob_column, pa.array([b for _, b in blobs], pa.large_binary()))

    base = lr.read_lance(
        uri,
        columns=columns,
        filter=filter,
        with_metadata=True,
        dataset_options={"version": version},
        storage_options=storage_options,
        **read_kwargs,
    )
    return base.map_batches(fetch, batch_size=batch_size, batch_format="pyarrow")


def blob_layout(uri: str, blob_column: str, storage_options: dict[str, str] | None = None) -> Counter:
    """统计 blob 列各存储方式的数量，例如 ``Counter({'packed': 10, 'inline': 3})``。"""
    import lance

    descriptions = (
        lance.dataset(uri, storage_options=storage_options).to_table(columns=[blob_column]).column(blob_column)
    )
    return Counter(BLOB_KINDS[d["kind"]] for d in descriptions.to_pylist() if d is not None)
