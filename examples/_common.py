"""示例公共部分：从环境变量读取 S3 配置与目标路径。

    export AWS_ACCESS_KEY_ID=...  AWS_SECRET_ACCESS_KEY=...  AWS_REGION=us-east-1
    export AWS_ENDPOINT_URL=http://localhost:9000   # 仅 MinIO 等 S3 兼容服务需要
    export LANCE_S3_BUCKET=my-bucket  LANCE_S3_PREFIX=demo
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import ray  # noqa: E402

from ray_lance_s3 import S3Config, s3_uri  # noqa: E402

STORAGE_OPTIONS = S3Config.from_env().storage_options()
BUCKET = os.environ.get("LANCE_S3_BUCKET", "lance-demo")
PREFIX = os.environ.get("LANCE_S3_PREFIX", "demo")


def uri(name: str) -> str:
    return s3_uri(BUCKET, PREFIX, f"{name}.lance")


def sample_dataset(n: int = 10_000, offset: int = 0) -> ray.data.Dataset:
    """生成示例数据：id / label / score / 8 维向量。"""
    import numpy as np

    def gen(batch):
        ids = batch["id"] + offset
        rng = np.random.default_rng(int(ids[0]) if len(ids) else 0)
        return {
            "id": ids,
            "label": np.where(ids % 3 == 0, "cat", "dog"),
            "score": rng.random(len(ids)),
            "vector": rng.random((len(ids), 8), dtype=np.float32),
        }

    return ray.data.range(n).map_batches(gen, batch_format="numpy")
