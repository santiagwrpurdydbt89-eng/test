"""测试夹具：启动本地 moto S3 服务 + 本地 Ray 集群。"""

from __future__ import annotations

import socket
import subprocess
import sys
import time
import uuid

import pytest

from ray_lance_s3 import S3Config


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def s3_endpoint():
    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "moto.server", "-H", "127.0.0.1", "-p", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    endpoint = f"http://127.0.0.1:{port}"
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                break
        except OSError:
            time.sleep(0.2)
    else:
        proc.kill()
        pytest.skip("moto server failed to start")
    yield endpoint
    proc.terminate()
    proc.wait(timeout=10)


@pytest.fixture(scope="session")
def s3_config(s3_endpoint) -> S3Config:
    return S3Config(
        access_key_id="testing",
        secret_access_key="testing",
        region="us-east-1",
        endpoint=s3_endpoint,
        allow_http=True,
    )


@pytest.fixture(scope="session")
def bucket(s3_config) -> str:
    import boto3

    name = "lance-test"
    boto3.client(
        "s3",
        endpoint_url=s3_config.endpoint,
        aws_access_key_id=s3_config.access_key_id,
        aws_secret_access_key=s3_config.secret_access_key,
        region_name=s3_config.region,
    ).create_bucket(Bucket=name)
    return name


@pytest.fixture
def table_uri(bucket) -> str:
    return f"s3://{bucket}/tables/{uuid.uuid4().hex}.lance"


@pytest.fixture(scope="session", autouse=True)
def ray_cluster():
    import ray

    ray.init(num_cpus=4, include_dashboard=False, ignore_reinit_error=True)
    yield
    ray.shutdown()
