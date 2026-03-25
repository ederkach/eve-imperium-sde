import os
import hashlib
from pathlib import Path

import sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils.http_client import create_session
from typing import Dict, Optional, Iterator


class CacheError(Exception):
    pass


class IndexEntry:
    def __init__(self, path: str, hash_value: str, size: int):
        self.path = path
        self.hash = hash_value
        self.size = size


class SharedCache:
    def client_version(self) -> str:
        raise NotImplementedError

    def iter_resources(self) -> Iterator[str]:
        raise NotImplementedError

    def has_resource(self, resource: str) -> bool:
        raise NotImplementedError

    def fetch(self, resource: str) -> bytes:
        raise NotImplementedError

    def path_of(self, resource: str) -> Path:
        raise NotImplementedError

    def hash_of(self, resource: str) -> str:
        raise NotImplementedError


class CacheDownloader(SharedCache):
    def __init__(self, cache_dir: Path, user_agent: str, use_macos_build: bool = False):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        if (self.cache_dir / "updater.exe").exists() or (self.cache_dir / "tq").exists():
            raise CacheError("Cannot use game install directory as cache")

        self.session = create_session()
        self.session.session.headers.update({'User-Agent': user_agent})

        response = self.session.get("https://binaries.eveonline.com/eveclient_TQ.json")
        client_data = response.json()

        if client_data.get('protected'):
            raise CacheError("Game server is in protected state")

        self._client_version = client_data.get('build_number', client_data.get('buildNumber', 0))
        self.app_index: Dict[str, IndexEntry] = {}
        self.res_index: Dict[str, IndexEntry] = {}

        index_filename = f"eveonline_{self._client_version}.txt"
        if use_macos_build:
            index_url = f"https://binaries.eveonline.com/eveonlinemacOS_{self._client_version}.txt"
        else:
            index_url = f"https://binaries.eveonline.com/eveonline_{self._client_version}.txt"

        index_content = self._fetch_file(self.cache_dir / index_filename, index_url)
        self._load_index(index_content.decode('utf-8'), self.app_index)

        res_index_content = self.fetch("app:/resfileindex.txt")
        self._load_index(res_index_content.decode('utf-8'), self.res_index)

    def _load_index(self, content: str, index_dict: Dict[str, IndexEntry]):
        for line in content.strip().split('\n'):
            if not line.strip():
                continue
            parts = line.split(',')
            if len(parts) >= 3:
                resource_path = parts[0].strip()
                file_path = parts[1].strip()
                hash_value = parts[2].strip()
                size = int(parts[3].strip()) if len(parts) > 3 else 0

                resource_key = resource_path.lower().replace('\\', '/')
                index_dict[resource_key] = IndexEntry(file_path, hash_value, size)

    def _ensure_cached(self, file_path: Path, url: str) -> Optional[bytes]:
        if file_path.exists():
            return None

        response = self.session.get(url)
        data = response.content

        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_bytes(data)

        return data

    def _fetch_file(self, file_path: Path, url: str) -> bytes:
        cached_data = self._ensure_cached(file_path, url)
        if cached_data is not None:
            return cached_data
        return file_path.read_bytes()

    def client_version(self) -> str:
        return self._client_version

    def iter_resources(self) -> Iterator[str]:
        yield from self.app_index.keys()
        yield from self.res_index.keys()

    def has_resource(self, resource: str) -> bool:
        resource = resource.lower().replace('\\', '/')
        return resource in self.app_index or resource in self.res_index

    def fetch(self, resource: str) -> bytes:
        resource = resource.lower().replace('\\', '/')

        if resource in self.app_index:
            entry = self.app_index[resource]
            url = f"https://binaries.eveonline.com/{entry.path}"
            return self._fetch_file(self.cache_dir / entry.path, url)
        elif resource in self.res_index:
            entry = self.res_index[resource]
            url = f"https://resources.eveonline.com/{entry.path}"
            return self._fetch_file(self.cache_dir / entry.path, url)
        else:
            raise CacheError(f"Resource not found: {resource}")

    def path_of(self, resource: str) -> Path:
        resource = resource.lower().replace('\\', '/')

        if resource in self.app_index:
            entry = self.app_index[resource]
            file_path = self.cache_dir / entry.path
            url = f"https://binaries.eveonline.com/{entry.path}"
            self._ensure_cached(file_path, url)
            return file_path
        elif resource in self.res_index:
            entry = self.res_index[resource]
            file_path = self.cache_dir / entry.path
            url = f"https://resources.eveonline.com/{entry.path}"
            self._ensure_cached(file_path, url)
            return file_path
        else:
            raise CacheError(f"Resource not found: {resource}")

    def hash_of(self, resource: str) -> str:
        resource = resource.lower().replace('\\', '/')

        if resource in self.app_index:
            return self.app_index[resource].hash
        elif resource in self.res_index:
            return self.res_index[resource].hash
        else:
            raise CacheError(f"Resource not found: {resource}")

    def purge(self, keep_files: list[str]):
        valid_paths = set()
        for entry in self.app_index.values():
            valid_paths.add(entry.path)
        for entry in self.res_index.values():
            valid_paths.add(entry.path)

        for keep_file in keep_files:
            valid_paths.add(keep_file)

        for root, dirs, files in os.walk(self.cache_dir):
            for file in files:
                file_path = Path(root) / file
                relative_path = file_path.relative_to(self.cache_dir)
                if str(relative_path) not in valid_paths:
                    try:
                        file_path.unlink()
                    except Exception:
                        pass
