"""Blob V2 大字段在 moto S3 上的端到端测试。"""

from __future__ import annotations

import importlib.util
import os

import lance
import pyarrow as pa
import pytest
import ray

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("lance_ray") is None or not hasattr(lance.LanceDataset, "read_blobs"),
    reason="需要 lance-ray 和支持 read_blobs 的 pylance",
)

KIB, MIB = 1024, 1024 * 1024
SIZES = [1 * KIB, 64 * KIB, 64 * KIB + 1, 2 * MIB, 4 * MIB + 1]
EXPECTED = ["inline", "inline", "packed", "packed", "dedicated"]


def _media_ds():
    return ray.data.from_items(
        [{"id": i, "tag": "big" if n > MIB else "small", "payload": bytes([i]) * n} for i, n in enumerate(SIZES)]
    )


def test_write_and_layout(s3_config, table_uri):
    from ray_lance_s3.blob import blob_columns_of, blob_layout, write_blob_dataset

    opts = s3_config.storage_options()
    write_blob_dataset(_media_ds(), table_uri, ["payload"], opts)

    ds = lance.dataset(table_uri, storage_options=opts)
    assert blob_columns_of(ds.schema) == ["payload"]
    kinds = {d["size"]: d["kind"] for d in ds.to_table(columns=["payload"]).column("payload").to_pylist()}
    names = {0: "inline", 1: "packed", 2: "dedicated"}
    assert [names[kinds[n]] for n in SIZES] == EXPECTED
    assert blob_layout(table_uri, "payload", opts) == {"inline": 2, "packed": 2, "dedicated": 1}


def test_custom_thresholds(s3_config, table_uri):
    from ray_lance_s3.blob import blob_field, blob_layout, write_blob_dataset

    opts = s3_config.storage_options()
    field = blob_field("payload", inline_size_threshold=0, dedicated_size_threshold=MIB)
    write_blob_dataset(_media_ds(), table_uri, {"payload": field}, opts)
    assert blob_layout(table_uri, "payload", opts) == {"packed": 3, "dedicated": 2}


def test_bytes_need_conversion(s3_config, table_uri):
    """直接把 bytes 列按 blob schema 写入会失败，必须先转换。"""
    import lance_ray as lr
    from ray_lance_s3.blob import blob_field

    schema = pa.schema([pa.field("id", pa.int64()), pa.field("tag", pa.string()), blob_field("payload")])
    with pytest.raises(Exception, match="binary to struct"):
        lr.write_lance(_media_ds(), table_uri, schema=schema, data_storage_version="2.2",
                       storage_options=s3_config.storage_options())


def test_read_with_blobs(s3_config, table_uri):
    import lance_ray as lr
    from ray_lance_s3.blob import read_with_blobs, write_blob_dataset

    opts = s3_config.storage_options()
    write_blob_dataset(_media_ds(), table_uri, ["payload"], opts)

    rows = read_with_blobs(table_uri, "payload", opts, filter="tag = 'big'", batch_size=1).take_all()
    assert sorted(r["id"] for r in rows) == [3, 4]
    for r in rows:
        assert r["payload"] == bytes([r["id"]]) * SIZES[r["id"]]

    # 对照：ray.data.read_lance 只返回描述信息，lance_ray.read_lance 返回完整内容
    desc = ray.data.read_lance(table_uri, storage_options=opts).take(1)[0]["payload"]
    assert isinstance(desc, dict) and "kind" in desc
    full = lr.read_lance(table_uri, storage_options=opts).take(1)[0]["payload"]
    assert isinstance(full, bytes)


def test_random_and_range_reads(s3_config, table_uri):
    from ray_lance_s3.blob import write_blob_dataset

    opts = s3_config.storage_options()
    write_blob_dataset(_media_ds(), table_uri, ["payload"], opts)
    ds = lance.dataset(table_uri, storage_options=opts)
    idx = {r["id"]: i for i, r in enumerate(ds.to_table(columns=["id"]).to_pylist())}[4]

    with ds.take_blobs("payload", indices=[idx])[0] as f:
        assert f.size() == SIZES[4]
        f.seek(MIB)
        assert f.read(10) == bytes([4]) * 10

    got = ds.read_blob_ranges("payload", [(idx, 0, 16), (idx, 2 * MIB, 16)], selector="indices")
    assert [r[2] for r in got] == [bytes([4]) * 16] * 2


def test_external_reference(s3_config, bucket, table_uri):
    """external：只记录外部 S3 对象地址，读取时再去取。"""
    import boto3
    from ray_lance_s3.blob import blob_layout, write_blob_dataset

    opts = s3_config.storage_options()
    key = f"raw/{os.path.basename(table_uri)}.bin"
    boto3.client(
        "s3", endpoint_url=s3_config.endpoint, aws_access_key_id="testing",
        aws_secret_access_key="testing", region_name="us-east-1",
    ).put_object(Bucket=bucket, Key=key, Body=b"e" * 5000)

    ds = ray.data.from_items([{"id": 0, "payload": f"s3://{bucket}/{key}"}])
    write_blob_dataset(ds, table_uri, ["payload"], opts, allow_external_blob_outside_bases=True)
    assert blob_layout(table_uri, "payload", opts) == {"external": 1}
    assert lance.dataset(table_uri, storage_options=opts).read_blobs("payload", indices=[0])[0][1] == b"e" * 5000
