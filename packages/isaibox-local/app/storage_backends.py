from __future__ import annotations

import os
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def load_local_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


load_local_env()


class NullSharedCache:
    enabled = False

    def fetch_to_path(self, key: str, destination: Path) -> bool:
        return False

    def upload_path(self, key: str, source: Path) -> bool:
        return False


class R2SharedCache:
    def __init__(self):
        self.bucket_name = os.environ.get("R2_BUCKET_NAME", "").strip()
        self.account_id = os.environ.get("R2_ACCOUNT_ID", "").strip()
        self.access_key_id = os.environ.get("R2_ACCESS_KEY_ID", "").strip()
        self.secret_access_key = os.environ.get("R2_SECRET_ACCESS_KEY", "").strip()
        self.endpoint_url = os.environ.get("R2_ENDPOINT_URL", "").strip() or (
            f"https://{self.account_id}.r2.cloudflarestorage.com" if self.account_id else ""
        )
        self.enabled = all(
            [
                self.bucket_name,
                self.access_key_id,
                self.secret_access_key,
                self.endpoint_url,
            ]
        )
        self._client = None
        self._init_error = None

    def _get_client(self):
        if not self.enabled:
            return None
        if self._client is not None:
            return self._client
        if self._init_error:
            return None
        try:
            import boto3
        except Exception as exc:  # pragma: no cover
            self._init_error = exc
            return None
        self._client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name="auto",
        )
        return self._client

    def fetch_to_path(self, key: str, destination: Path) -> bool:
        client = self._get_client()
        if client is None:
            return False
        destination.parent.mkdir(parents=True, exist_ok=True)
        temp_path = destination.with_suffix(".r2part")
        try:
            client.download_file(self.bucket_name, key, str(temp_path))
            temp_path.replace(destination)
            return True
        except Exception:
            if temp_path.exists():
                temp_path.unlink()
            return False

    def upload_path(self, key: str, source: Path) -> bool:
        client = self._get_client()
        if client is None or not source.exists():
            return False
        try:
            client.upload_file(
                str(source),
                self.bucket_name,
                key,
                ExtraArgs={"ContentType": "audio/mpeg", "CacheControl": "public, max-age=31536000"},
            )
            return True
        except Exception:
            return False


def get_shared_cache():
    cache = R2SharedCache()
    if cache.enabled:
        return cache
    return NullSharedCache()
