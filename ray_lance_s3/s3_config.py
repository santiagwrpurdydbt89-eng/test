"""S3 / S3 兼容对象存储（MinIO、TOS、OSS 等）的 Lance ``storage_options`` 构造。

Lance 底层使用 Rust ``object_store`` 访问对象存储，所有配置都以
``storage_options: dict[str, str]`` 传入。Ray / lance-ray 的读写函数都会把
这个字典原样序列化到每个 Ray worker 上，因此 **不要依赖 driver 进程的
环境变量**——worker 可能运行在其他节点上，看不到这些环境变量。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class S3Config:
    """描述一个 S3 端点及凭证。

    所有字段都可留空：留空时 Lance 会回退到 AWS 默认凭证链
    （环境变量 → ~/.aws → IAM Role / IRSA 等）。
    """

    access_key_id: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None
    region: str | None = None
    # 自建/第三方 S3 兼容服务的地址，例如 "http://minio:9000"
    endpoint: str | None = None
    # endpoint 为 http:// 时必须为 True
    allow_http: bool = False
    # MinIO 等通常使用 path-style；部分云厂商（如 TOS）要求 virtual-hosted-style
    virtual_hosted_style: bool | None = None
    # 透传给 object_store 的其他选项，例如 {"timeout": "60s"}
    extra: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls, prefix: str = "") -> "S3Config":
        """从环境变量读取配置。

        读取 ``{prefix}AWS_ACCESS_KEY_ID`` / ``{prefix}AWS_SECRET_ACCESS_KEY`` /
        ``{prefix}AWS_SESSION_TOKEN`` / ``{prefix}AWS_REGION`` /
        ``{prefix}AWS_ENDPOINT_URL`` / ``{prefix}AWS_ALLOW_HTTP`` /
        ``{prefix}AWS_VIRTUAL_HOSTED_STYLE_REQUEST``。
        """

        def get(name: str) -> str | None:
            return os.environ.get(prefix + name) or None

        endpoint = get("AWS_ENDPOINT_URL") or get("AWS_ENDPOINT")
        allow_http = get("AWS_ALLOW_HTTP")
        vhost = get("AWS_VIRTUAL_HOSTED_STYLE_REQUEST")
        return cls(
            access_key_id=get("AWS_ACCESS_KEY_ID"),
            secret_access_key=get("AWS_SECRET_ACCESS_KEY"),
            session_token=get("AWS_SESSION_TOKEN"),
            region=get("AWS_REGION") or get("AWS_DEFAULT_REGION"),
            endpoint=endpoint,
            allow_http=(
                _truthy(allow_http)
                if allow_http is not None
                else bool(endpoint and endpoint.startswith("http://"))
            ),
            virtual_hosted_style=_truthy(vhost) if vhost is not None else None,
        )

    def storage_options(self) -> dict[str, str]:
        """转换为 Lance / Ray / lance-ray 通用的 ``storage_options``。"""
        opts: dict[str, str] = {}
        if self.access_key_id:
            opts["aws_access_key_id"] = self.access_key_id
        if self.secret_access_key:
            opts["aws_secret_access_key"] = self.secret_access_key
        if self.session_token:
            opts["aws_session_token"] = self.session_token
        if self.region:
            opts["aws_region"] = self.region
        if self.endpoint:
            opts["aws_endpoint"] = self.endpoint
        if self.allow_http:
            opts["allow_http"] = "true"
        if self.virtual_hosted_style is not None:
            opts["aws_virtual_hosted_style_request"] = str(self.virtual_hosted_style).lower()
        opts.update(self.extra)
        return opts


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def s3_uri(bucket: str, *parts: str) -> str:
    """拼接 ``s3://bucket/a/b.lance`` 形式的 URI。"""
    path = "/".join(p.strip("/") for p in parts if p)
    return f"s3://{bucket}/{path}" if path else f"s3://{bucket}"
