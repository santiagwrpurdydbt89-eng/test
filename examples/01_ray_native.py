"""方式一：Ray 内置 ray.data.read_lance / Dataset.write_lance。

依赖：ray[data] + pylance<6（Ray 2.59 内置写入与 pylance>=6 不兼容；只读则不受限）。
"""

import sys

from _common import STORAGE_OPTIONS, sample_dataset, uri

from ray_lance_s3 import ray_read_lance, ray_write_lance
from ray_lance_s3.io import ray_native_write_supported

URI = uri("ray_native")

if not ray_native_write_supported():
    sys.exit("当前 pylance 与 Ray 内置 write_lance 不兼容：请运行 02_lance_ray.py，或安装 pylance<6")

# 写入：overwrite → append，产生 2 个版本
ray_write_lance(sample_dataset(10_000), URI, STORAGE_OPTIONS, mode="overwrite")
ray_write_lance(sample_dataset(2_000, offset=10_000), URI, STORAGE_OPTIONS, mode="append")

# 全量读取
ds = ray_read_lance(URI, STORAGE_OPTIONS)
print("rows:", ds.count())
print(ds.schema())

# 列裁剪 + 谓词下推：只从 S3 读需要的列和满足条件的行
cats = ray_read_lance(URI, STORAGE_OPTIONS, columns=["id", "score"], filter="label = 'cat' AND score > 0.5")
print("cats with score>0.5:", cats.materialize().count())  # 带 filter 时先 materialize 再 count

# time travel：读取最新版本之前的版本
import lance  # noqa: E402

first = lance.dataset(URI, storage_options=STORAGE_OPTIONS).versions()[-2]["version"]
print(f"version {first} rows:", ray_read_lance(URI, STORAGE_OPTIONS, version=first).count())
