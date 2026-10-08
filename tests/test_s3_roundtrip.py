"""在本地 moto S3 上端到端验证全部读写路径。"""

from __future__ import annotations

import importlib.util

import lance
import pyarrow as pa
import pytest
import pyarrow.compute as pc
import ray

from ray_lance_s3 import (
    S3Config,
    lr_read_lance,
    lr_write_lance,
    ray_read_lance,
    ray_write_lance,
    write_fragments_then_commit,
)
from ray_lance_s3.io import ray_native_write_supported

HAS_LANCE_RAY = importlib.util.find_spec("lance_ray") is not None
needs_lance_ray = pytest.mark.skipif(not HAS_LANCE_RAY, reason="lance-ray 未安装")


def _make_ds(n: int, offset: int = 0) -> ray.data.Dataset:
    return ray.data.range(n).map_batches(
        lambda b: {
            "id": b["id"] + offset,
            "label": ["cat" if i % 2 else "dog" for i in b["id"]],
            "score": b["id"] / max(n, 1),
        },
        batch_format="numpy",
    )


def test_storage_options():
    opts = S3Config(
        access_key_id="ak",
        secret_access_key="sk",
        region="us-east-1",
        endpoint="http://minio:9000",
        allow_http=True,
        virtual_hosted_style=False,
    ).storage_options()
    assert opts == {
        "aws_access_key_id": "ak",
        "aws_secret_access_key": "sk",
        "aws_region": "us-east-1",
        "aws_endpoint": "http://minio:9000",
        "allow_http": "true",
        "aws_virtual_hosted_style_request": "false",
    }


def test_from_env(monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:9000")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ak")
    cfg = S3Config.from_env()
    assert cfg.endpoint == "http://localhost:9000"
    assert cfg.allow_http is True
    assert cfg.access_key_id == "ak"


@pytest.mark.skipif(
    not ray_native_write_supported(),
    reason="Ray 内置 write_lance 与当前 pylance 不兼容（需要 pylance<6）",
)
def test_ray_native_roundtrip(s3_config, table_uri):
    opts = s3_config.storage_options()
    ray_write_lance(_make_ds(1000), table_uri, opts)
    ray_write_lance(_make_ds(500, offset=1000), table_uri, opts, mode="append")

    ds = ray_read_lance(table_uri, opts)
    assert ds.count() == 1500

    cats = ray_read_lance(table_uri, opts, columns=["id", "label"], filter="label = 'cat'")
    rows = cats.take_all()
    assert len(rows) == 750
    assert set(rows[0]) == {"id", "label"}

    # time travel：版本 1 只有第一次写入的 1000 行
    assert ray_read_lance(table_uri, opts, version=1).count() == 1000


def test_ray_native_read(s3_config, table_uri):
    """ray.data.read_lance 与任意 pylance 版本兼容；用 lance 直接写入准备数据。"""
    opts = s3_config.storage_options()
    lance.write_dataset(pa.table({"id": list(range(100)), "label": ["a", "b"] * 50}), table_uri, storage_options=opts)
    ds = ray_read_lance(table_uri, opts, filter="label = 'a'")
    # 注意：带 filter 时 ds.count() 会返回元数据里的未过滤行数，必须先 materialize
    assert ds.materialize().count() == 50


@needs_lance_ray
def test_lance_ray_roundtrip(s3_config, table_uri):
    opts = s3_config.storage_options()
    lr_write_lance(_make_ds(800), table_uri, opts, min_rows_per_file=100, max_rows_per_file=200)
    lr_write_lance(_make_ds(100, offset=800), table_uri, opts, mode="overwrite")

    assert lr_read_lance(table_uri, opts).count() == 100
    assert lr_read_lance(table_uri, opts, version=1).count() == 800

    frag_ids = [f.fragment_id for f in lance.dataset(table_uri, version=1, storage_options=opts).get_fragments()]
    assert len(frag_ids) >= 4  # max_rows_per_file=200 → 至少 4 个 fragment
    one = lr_read_lance(table_uri, opts, version=1, fragment_ids=frag_ids[:1])
    assert 0 < one.count() <= 200


@needs_lance_ray
def test_lance_ray_stream_write(s3_config, table_uri):
    opts = s3_config.storage_options()
    lr_write_lance(_make_ds(300), table_uri, opts, stream=True, batch_size=100)
    assert lance.dataset(table_uri, storage_options=opts).count_rows() == 300


@needs_lance_ray
def test_fragment_writer_committer(s3_config, table_uri):
    opts = s3_config.storage_options()

    def add_double(tbl: pa.Table) -> pa.Table:
        return tbl.append_column("score2", pc.multiply(tbl["score"], 2))

    write_fragments_then_commit(_make_ds(600), table_uri, opts, transform=add_double, batch_size=200)
    tbl = lance.dataset(table_uri, storage_options=opts).to_table()
    assert tbl.num_rows == 600
    assert "score2" in tbl.column_names

    write_fragments_then_commit(_make_ds(100, offset=600), table_uri, opts, mode="append", transform=add_double)
    assert lance.dataset(table_uri, storage_options=opts).count_rows() == 700


@needs_lance_ray
def test_maintenance(s3_config, table_uri):
    opts = s3_config.storage_options()
    for i in range(4):
        lr_write_lance(_make_ds(50, offset=i * 50), table_uri, opts, mode="create" if i == 0 else "append")
    assert len(lance.dataset(table_uri, storage_options=opts).get_fragments()) >= 4

    def id_plus_one(batch: pa.RecordBatch) -> pa.RecordBatch:
        return pa.RecordBatch.from_arrays([pc.add(batch["id"], 1)], names=["id_plus_one"])

    from ray_lance_s3 import maintenance

    maintenance.add_columns(table_uri, id_plus_one, opts, read_columns=["id"])
    tbl = lance.dataset(table_uri, storage_options=opts).to_table()
    assert pc.all(pc.equal(tbl["id_plus_one"], pc.add(tbl["id"], 1))).as_py()

    maintenance.compact_files(table_uri, opts, num_workers=2)
    assert len(lance.dataset(table_uri, storage_options=opts).get_fragments()) == 1

    maintenance.create_scalar_index(table_uri, "id", opts, num_workers=2)
    ds = lance.dataset(table_uri, storage_options=opts)
    assert any(idx["fields"] == ["id"] for idx in ds.list_indices())
    assert len(maintenance.list_versions(table_uri, opts)) >= 6
