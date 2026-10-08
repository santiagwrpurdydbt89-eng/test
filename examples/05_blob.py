"""Blob V2 大字段：用 Ray 把图片/视频等二进制写入 S3 上的 Lance，并按需读取。

依赖：ray[data] + pylance>=13 + lance-ray；数据格式版本 >= 2.2。
"""

import os

from _common import STORAGE_OPTIONS, uri

import lance
import ray
from ray_lance_s3.blob import blob_field, blob_layout, read_with_blobs, write_blob_dataset

URI = uri("media")

# 模拟三种大小的文件：缩略图 8KB、图片 500KB、视频 12MB
SIZES = {"thumb": 8 * 1024, "image": 500 * 1024, "video": 12 * 1024 * 1024}
items = [
    {"id": i, "kind": kind, "payload": os.urandom(size)}
    for i, (kind, size) in enumerate(list(SIZES.items()) * 4)
]
ds = ray.data.from_items(items)

# 1) 写入：bytes 列先转换为 blob 类型；可选地按列调整阈值
write_blob_dataset(
    ds,
    URI,
    {"payload": blob_field("payload", dedicated_size_threshold=8 * 1024 * 1024)},
    STORAGE_OPTIONS,
    mode="overwrite",
)
print("存储分布:", blob_layout(URI, "payload", STORAGE_OPTIONS))  # 预期 inline 4 / packed 4 / dedicated 4

# 2) 在 Ray 中按需读取：先按普通列过滤，只取需要的 blob 内容
images = read_with_blobs(URI, "payload", STORAGE_OPTIONS, filter="kind = 'image'", batch_size=16)
for row in images.take(2):
    print(row["id"], row["kind"], len(row["payload"]), "bytes")

# 3) 单机随机访问：扫描只返回描述信息；BlobFile 支持 seek/read，不必下载整个文件
dataset = lance.dataset(URI, storage_options=STORAGE_OPTIONS)
print(dataset.to_table(columns=["id", "payload"], limit=1).to_pylist())
video_idx = dataset.to_table(columns=["kind"]).column("kind").to_pylist().index("video")
with dataset.take_blobs("payload", indices=[video_idx])[0] as f:
    f.seek(1024 * 1024)
    print("从 1MB 处读取 4KB:", len(f.read(4096)))

# 4) 一次读取多个字节范围（相邻请求会被合并）
ranges = dataset.read_blob_ranges("payload", [(video_idx, 0, 1024), (video_idx, 4096, 1024)], selector="indices")
print("范围读取:", [(r[0], len(r[2])) for r in ranges])
