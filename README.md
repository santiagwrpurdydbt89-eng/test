# Ray 读写 S3 上的 Lance 数据集

整理了 Ray Data 读写 S3（及 MinIO 等 S3 兼容存储）上 Lance 数据集的三种方式，附可运行示例、端到端测试和 HTML 文档。

完整文档：[`docs/index.html`](docs/index.html)（直接用浏览器打开）。

## 三种方式

| 方式 | API | 依赖 | 适用场景 |
| --- | --- | --- | --- |
| Ray 内置 | `ray.data.read_lance` / `Dataset.write_lance` | `ray[data]` + `pylance` | 简单读写，不想多装包 |
| lance-ray | `lance_ray.read_lance` / `lance_ray.write_lance` | 另加 `lance-ray` | 推荐。参数更全，支持流式写入、按 fragment 读取、分布式加列/压缩/建索引 |
| 两阶段写入 | `LanceFragmentWriter` + `LanceFragmentCommitter` | `lance-ray` | 写入前在 worker 上做 transform，需要精细控制 fragment |

## 版本兼容性（已实测）

| 组合 | 读 | 写 |
| --- | --- | --- |
| Ray 2.59 + pylance 13 + lance-ray 0.5 | ✅ 两种方式都能读 | ✅ lance-ray；❌ Ray 内置 `write_lance` |
| Ray 2.59 + pylance 4.0.2 / 3.0.2（不装 lance-ray） | ✅ | ✅ Ray 内置 `write_lance` |

Ray 2.59 内置的 Lance 写入会传 `storage_options_provider` 参数，pylance 6 起该参数已被移除，会报 `TypeError: write_fragments() got an unexpected keyword argument 'storage_options_provider'`；而 lance-ray 0.5 要求 `pylance>=9`。因此同一个环境里两者只能选一个，推荐用 lance-ray 写入。`ray_lance_s3.io.ray_native_write_supported()` 可以检测当前环境能否用 Ray 内置写入。

## 快速开始

```python
from ray_lance_s3 import S3Config, lr_read_lance, lr_write_lance

opts = S3Config(
    access_key_id="...", secret_access_key="...", region="us-east-1",
    endpoint="http://minio:9000", allow_http=True,  # 只有 S3 兼容服务需要
).storage_options()
# 或：opts = S3Config.from_env().storage_options()

lr_write_lance(ds, "s3://bucket/path/table.lance", opts, mode="overwrite")
ds = lr_read_lance("s3://bucket/path/table.lance", opts, columns=["id"], filter="score > 0.5")
```

## 目录结构

```
ray_lance_s3/
  s3_config.py      # S3Config → storage_options（AWS / MinIO / 自定义 endpoint）
  io.py             # 三种读写方式的封装 + 兼容性检测
  maintenance.py    # 分布式加列、合并小文件、建标量索引、版本列表
examples/           # 01~04 可运行示例（从环境变量读取 S3 配置）
tests/              # 基于 moto 本地 S3 的端到端测试
docs/index.html     # 完整 HTML 文档
docker-compose.yml  # 本地 MinIO
```

## 运行示例

```bash
pip install -r requirements.txt
docker compose up -d   # 本地 MinIO，并创建 lance-demo 桶
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:9000 LANCE_S3_BUCKET=lance-demo
cd examples && python 02_lance_ray.py
```

## 测试

```bash
pytest -q   # 自动启动 moto S3 服务和本地 Ray，无需真实 S3
```

依赖不满足的用例会被自动跳过（比如装了 pylance≥6 时会跳过 Ray 内置写入的用例）。
