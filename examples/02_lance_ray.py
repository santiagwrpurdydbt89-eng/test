"""方式二：lance-ray（Lance 官方 Ray 集成）读写 S3。

依赖：ray[data] + pylance>=9 + lance-ray。
"""

from _common import STORAGE_OPTIONS, sample_dataset, uri

import lance
from ray_lance_s3 import lr_read_lance, lr_write_lance

URI = uri("lance_ray")

# 批量写入：每个 Ray 写任务产出若干 fragment，driver 一次提交
lr_write_lance(
    sample_dataset(20_000),
    URI,
    STORAGE_OPTIONS,
    mode="overwrite",
    min_rows_per_file=2_000,
    max_rows_per_file=5_000,
)

# 流式写入：每个 batch 提交一个版本，失败后可用 resume_rows 断点续写
lr_write_lance(sample_dataset(3_000, offset=20_000), URI, STORAGE_OPTIONS, mode="append", stream=True, batch_size=1_000)

dataset = lance.dataset(URI, storage_options=STORAGE_OPTIONS)
print("version:", dataset.version, "rows:", dataset.count_rows(), "fragments:", len(dataset.get_fragments()))

# 读取：列裁剪 + 过滤
ds = lr_read_lance(URI, STORAGE_OPTIONS, columns=["id", "label", "score"], filter="score > 0.9")
print("score>0.9:", ds.materialize().count())

# 只读部分 fragment（适合自定义分片/增量处理）
frag_ids = [f.fragment_id for f in dataset.get_fragments()][:2]
print("first 2 fragments:", lr_read_lance(URI, STORAGE_OPTIONS, fragment_ids=frag_ids).count())

# 带 _rowid 等元数据列读取
print(lr_read_lance(URI, STORAGE_OPTIONS, columns=["id"], with_metadata=True).take(2))

# time travel
print("version 1 rows:", lr_read_lance(URI, STORAGE_OPTIONS, version=1).count())
