"""Ray Data 读写 S3 上 Lance 数据集的三种方式。

1. Ray 内置 API：``ray.data.read_lance`` / ``Dataset.write_lance``
   —— 只依赖 ``ray[data]`` + ``pylance``，适合简单的读写。
2. lance-ray API：``lance_ray.read_lance`` / ``lance_ray.write_lance``
   —— Lance 官方维护，参数更全（fragment 级读取、stable row id、流式写入、
   namespace 等），并配套加列、压缩、建索引等分布式运维操作。
3. 底层两阶段写入：``LanceFragmentWriter``（map_batches，各 worker 并行写
   数据文件）+ ``LanceFragmentCommitter``（driver 端一次性提交 manifest）。
   适合在写入前做自定义 transform、或需要精细控制 fragment 大小的场景。

所有函数都接受 ``storage_options``，可由 :class:`ray_lance_s3.S3Config` 生成。
"""

from __future__ import annotations

from typing import Any, Literal

import pyarrow as pa
import ray
import ray.data

WriteMode = Literal["create", "append", "overwrite"]


# ---------------------------------------------------------------------------
# 1. Ray 内置 API
# ---------------------------------------------------------------------------


def ray_read_lance(
    uri: str,
    storage_options: dict[str, str] | None = None,
    *,
    columns: list[str] | None = None,
    filter: str | None = None,
    version: int | str | None = None,
    scanner_options: dict[str, Any] | None = None,
    **kwargs: Any,
) -> ray.data.Dataset:
    """用 ``ray.data.read_lance`` 读取 S3 上的 Lance 数据集。

    - ``columns``：列裁剪，只从 S3 拉取需要的列；
    - ``filter``：SQL 风格谓词（如 ``"label = 'cat' AND score > 0.5"``），
      下推到 Lance 扫描器执行；
    - ``version``：读取历史版本（time travel），可以是版本号或 tag 名；
    - ``scanner_options``：透传给 ``LanceDataset.scanner``，如
      ``{"batch_size": 8192}``。

    注意：带 ``filter`` 时直接 ``ds.count()`` 可能返回 **未过滤** 的元数据行数
    （Ray 2.59 实测），需要准确行数请用 ``ds.materialize().count()``。
    """
    return ray.data.read_lance(
        uri,
        columns=columns,
        filter=filter,
        version=version,
        storage_options=storage_options,
        scanner_options=scanner_options,
        **kwargs,
    )


def ray_native_write_supported() -> bool:
    """当前环境中 Ray 内置的 ``Dataset.write_lance`` 能否工作。

    Ray (>= 2.5x) 的内置 Lance datasink 会给 ``lance.fragment.write_fragments``
    传 ``storage_options_provider`` 参数，而 pylance >= 6 已把它替换为
    ``namespace_client``，于是写入时报
    ``TypeError: write_fragments() got an unexpected keyword argument
    'storage_options_provider'``。读取（``ray.data.read_lance``）不受影响。
    """
    import inspect

    from lance.fragment import write_fragments

    try:
        from ray.data._internal.datasource import lance_datasink
    except ImportError:
        return True
    ray_needs_provider = "storage_options_provider" in inspect.getsource(lance_datasink)
    lance_has_provider = "storage_options_provider" in inspect.signature(write_fragments).parameters
    return lance_has_provider or not ray_needs_provider


def ray_write_lance(
    ds: ray.data.Dataset,
    uri: str,
    storage_options: dict[str, str] | None = None,
    *,
    mode: WriteMode = "create",
    schema: pa.Schema | None = None,
    min_rows_per_file: int = 1024 * 1024,
    max_rows_per_file: int = 64 * 1024 * 1024,
    data_storage_version: str | None = None,
    **kwargs: Any,
) -> None:
    """用 ``Dataset.write_lance`` 写入 S3。

    ``mode``：``create``（已存在则报错）、``append``（追加新版本）、
    ``overwrite``（覆盖，旧版本仍可 time travel）。

    注意 Ray 与 pylance 的版本兼容性，见 :func:`ray_native_write_supported`；
    不兼容时请改用 :func:`lr_write_lance`。
    """
    if not ray_native_write_supported():
        import lance

        raise RuntimeError(
            f"Ray {ray.__version__} 内置的 write_lance 与 pylance {lance.__version__} 不兼容"
            "（需要 pylance<6）。请改用 lr_write_lance()（lance-ray），"
            "或安装 pylance<6 且不安装 lance-ray。"
        )
    try:  # Ray >= 2.46 使用 SaveMode 枚举；更早版本直接接受字符串
        from ray.data import SaveMode

        ray_mode: Any = SaveMode(mode)
    except ImportError:
        ray_mode = mode

    ds.write_lance(
        uri,
        mode=ray_mode,
        schema=schema,
        min_rows_per_file=min_rows_per_file,
        max_rows_per_file=max_rows_per_file,
        data_storage_version=data_storage_version,
        storage_options=storage_options,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# 2. lance-ray API
# ---------------------------------------------------------------------------


def lr_read_lance(
    uri: str,
    storage_options: dict[str, str] | None = None,
    *,
    columns: list[str] | None = None,
    filter: str | None = None,
    fragment_ids: list[int] | None = None,
    version: int | str | None = None,
    with_metadata: bool = False,
    **kwargs: Any,
) -> ray.data.Dataset:
    """用 ``lance_ray.read_lance`` 读取。

    相比 Ray 内置版本额外支持：
    - ``fragment_ids``：只读取指定 fragment；
    - ``version``：通过 ``dataset_options={"version": ...}`` 实现；
    - ``with_metadata``：附带 ``_rowid`` / ``_rowaddr`` 等元数据列。
    """
    import lance_ray as lr

    dataset_options = dict(kwargs.pop("dataset_options", None) or {})
    if version is not None:
        dataset_options["version"] = version
    return lr.read_lance(
        uri,
        columns=columns,
        filter=filter,
        fragment_ids=fragment_ids,
        storage_options=storage_options,
        dataset_options=dataset_options or None,
        with_metadata=with_metadata,
        **kwargs,
    )


def lr_write_lance(
    ds: ray.data.Dataset,
    uri: str,
    storage_options: dict[str, str] | None = None,
    *,
    mode: WriteMode = "create",
    schema: pa.Schema | None = None,
    min_rows_per_file: int = 1024 * 1024,
    max_rows_per_file: int = 64 * 1024 * 1024,
    data_storage_version: str | None = None,
    **kwargs: Any,
) -> None:
    """用 ``lance_ray.write_lance`` 写入。

    ``min_rows_per_file`` 必须 <= ``max_rows_per_file``，调小 max 时要同时调小 min。

    常用的额外参数（通过 ``kwargs`` 传入）：
    ``enable_stable_row_ids``、``stream=True`` + ``batch_size``（流式写入，
    每批提交一次，可配合 ``resume_rows`` 断点续写）、``concurrency``、
    ``ray_remote_args`` 等。
    """
    import lance_ray as lr

    lr.write_lance(
        ds,
        uri,
        mode=mode,
        schema=schema,
        min_rows_per_file=min_rows_per_file,
        max_rows_per_file=max_rows_per_file,
        data_storage_version=data_storage_version,
        storage_options=storage_options,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# 3. 两阶段写入：LanceFragmentWriter + LanceFragmentCommitter
# ---------------------------------------------------------------------------


def write_fragments_then_commit(
    ds: ray.data.Dataset,
    uri: str,
    storage_options: dict[str, str] | None = None,
    *,
    mode: WriteMode = "create",
    schema: pa.Schema | None = None,
    transform=None,
    batch_size: int = 64 * 1024,
    max_rows_per_file: int = 1024 * 1024,
    concurrency: int | None = None,
) -> None:
    """显式两阶段写入。

    阶段一：``map_batches(LanceFragmentWriter(...))`` 在每个 worker 上把一批
    数据写成 Lance 数据文件（直接上传到 S3），返回序列化后的 fragment 元数据；
    阶段二：``write_datasink(LanceFragmentCommitter(...))`` 在 driver 汇总所有
    fragment，一次原子提交生成新版本。任何 worker 失败都不会产生半写入的版本。

    ``transform`` 为 ``Callable[[pa.Table], pa.Table]``，在写入前于 worker 端执行
    （例如计算 embedding、清洗字段）。
    """
    from lance_ray import LanceFragmentCommitter, LanceFragmentWriter

    writer = LanceFragmentWriter(
        uri,
        transform=transform,
        schema=schema,
        max_rows_per_file=max_rows_per_file,
        storage_options=storage_options,
    )
    map_kwargs: dict[str, Any] = {"batch_size": batch_size, "batch_format": "pyarrow"}
    if concurrency is not None:
        map_kwargs["concurrency"] = concurrency
    fragments = ds.map_batches(writer, **map_kwargs)
    fragments.write_datasink(
        LanceFragmentCommitter(uri, schema=schema, mode=mode, storage_options=storage_options)
    )
