"""用 lance-ray 在 S3 数据集上做分布式运维：加列、合并小文件、建索引、版本管理。"""

from _common import STORAGE_OPTIONS, sample_dataset, uri

import lance
import pyarrow as pa
import pyarrow.compute as pc
from ray_lance_s3 import lr_write_lance, maintenance

URI = uri("maintenance")

# 多次小批量 append → 产生很多小 fragment
lr_write_lance(sample_dataset(1_000), URI, STORAGE_OPTIONS, mode="overwrite")
for i in range(1, 5):
    lr_write_lance(sample_dataset(1_000, offset=i * 1_000), URI, STORAGE_OPTIONS, mode="append")
print("fragments before compaction:", len(lance.dataset(URI, storage_options=STORAGE_OPTIONS).get_fragments()))


# 1) 分布式加列：只写新列的数据文件，不重写已有数据
def is_high(batch: pa.RecordBatch) -> pa.RecordBatch:
    return pa.RecordBatch.from_arrays([pc.greater(batch["score"], 0.8)], names=["is_high"])


maintenance.add_columns(URI, is_high, STORAGE_OPTIONS, read_columns=["score"])

# 2) 合并小文件
metrics = maintenance.compact_files(URI, STORAGE_OPTIONS, num_workers=2)
print("compaction:", metrics)
print("fragments after compaction:", len(lance.dataset(URI, storage_options=STORAGE_OPTIONS).get_fragments()))

# 3) 分布式建标量索引
maintenance.create_scalar_index(URI, "id", STORAGE_OPTIONS, index_type="BTREE", num_workers=2)
print("indices:", [i["name"] for i in lance.dataset(URI, storage_options=STORAGE_OPTIONS).list_indices()])

# 4) 版本历史
for v in maintenance.list_versions(URI, STORAGE_OPTIONS):
    print(v["version"], v["timestamp"])
