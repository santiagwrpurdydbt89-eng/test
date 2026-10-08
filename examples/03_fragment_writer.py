"""方式三：LanceFragmentWriter + LanceFragmentCommitter 两阶段分布式写入。

在 worker 端做 transform（如计算 embedding）后直接把数据文件写到 S3，
最后由 driver 原子提交一个新版本。
"""

from _common import STORAGE_OPTIONS, sample_dataset, uri

import lance
import pyarrow as pa
import pyarrow.compute as pc
from ray_lance_s3 import write_fragments_then_commit

URI = uri("fragment_writer")


def enrich(tbl: pa.Table) -> pa.Table:
    """在写入前于 worker 上执行：新增一列 score_pct。"""
    return tbl.append_column("score_pct", pc.round(pc.multiply(tbl["score"], 100), 1))


write_fragments_then_commit(
    sample_dataset(50_000),
    URI,
    STORAGE_OPTIONS,
    mode="overwrite",
    transform=enrich,
    batch_size=10_000,
    max_rows_per_file=25_000,
)

ds = lance.dataset(URI, storage_options=STORAGE_OPTIONS)
print("rows:", ds.count_rows(), "fragments:", len(ds.get_fragments()))
print(ds.to_table(limit=3).to_pandas())
