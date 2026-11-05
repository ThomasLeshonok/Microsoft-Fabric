# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "22222222-2222-2222-2222-222222222222",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "11111111-1111-1111-1111-111111111111"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_http_download
#
# Validates a URL, confirms it exists, then streams the response (any file type) into OneLake.
#
# * Run directly or through `notebookutils.notebook.run("nb_http_download", ...)` with `url` set.
# * `%run nb_http_download` with the default empty `url` only loads the classes for reuse.
# * `target_folder` accepts `Files/...` (default lakehouse) or a full `abfss://` path.
# * Empty `file_name` derives the name from the URL, adding an extension from `Content-Type` when needed.

# PARAMETERS CELL ********************

url = ""
target_folder = "Files"
file_name = ""
headers_json = "{}"
request_timeout_seconds = 300

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import mimetypes
import os
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import PurePosixPath
from typing import Mapping, Optional, Protocol
from urllib.parse import unquote, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
HEAD_UNSUPPORTED_STATUSES = frozenset({403, 405, 501})
GENERIC_MIME_TYPES = frozenset({None, "", "application/octet-stream", "binary/octet-stream"})
DATA_FILE_TYPES = {
    ".parquet": "application/vnd.apache.parquet",
    ".avro": "application/avro",
    ".orc": "application/x-orc",
    ".jsonl": "application/jsonl",
    ".ndjson": "application/x-ndjson",
    ".geojson": "application/geo+json",
}


class DownloadError(Exception):
    pass


class InvalidUrlError(DownloadError):
    pass


class UrlNotFoundError(DownloadError):
    pass


@dataclass(frozen=True)
class DownloadRequest:
    url: str
    target_folder: str
    file_name: str = ""
    headers: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class DownloadedFile:
    local_path: str
    content_type: str
    size_bytes: int


@dataclass(frozen=True)
class DownloadResult:
    url: str
    destination: str
    content_type: str
    size_bytes: int


class ExistenceCheck(Protocol):
    def ensure_exists(self, url: str, headers: Mapping[str, str]) -> None: ...


class FileFetcher(Protocol):
    def fetch(self, url: str, headers: Mapping[str, str]) -> DownloadedFile: ...


class FileStorage(Protocol):
    def save(self, local_path: str, destination: str) -> None: ...


class UrlValidator:
    def __init__(self, allowed_schemes: frozenset = frozenset({"http", "https"})):
        self._allowed_schemes = allowed_schemes

    def validate(self, url: str) -> str:
        cleaned = (url or "").strip()
        parsed = urlparse(cleaned)
        if any(character.isspace() for character in cleaned):
            raise InvalidUrlError(f"Url contains whitespace: {cleaned!r}")
        if parsed.scheme.lower() not in self._allowed_schemes:
            raise InvalidUrlError(f"Unsupported scheme {parsed.scheme!r} in url: {cleaned!r}")
        if not parsed.hostname:
            raise InvalidUrlError(f"Missing host in url: {cleaned!r}")
        return cleaned


class UrlExistenceChecker:
    def __init__(self, session: requests.Session, timeout_seconds: float):
        self._session = session
        self._timeout_seconds = timeout_seconds

    def ensure_exists(self, url: str, headers: Mapping[str, str]) -> None:
        status = self._probe(url, headers)
        if status >= 400:
            raise UrlNotFoundError(f"{url} answered HTTP {status}")

    def _probe(self, url: str, headers: Mapping[str, str]) -> int:
        try:
            status = self._head_status(url, headers)
            return self._get_status(url, headers) if status in HEAD_UNSUPPORTED_STATUSES else status
        except requests.RequestException as error:
            raise UrlNotFoundError(f"{url} is unreachable: {error}") from error

    def _head_status(self, url: str, headers: Mapping[str, str]) -> int:
        response = self._session.head(url, headers=headers, timeout=self._timeout_seconds, allow_redirects=True)
        return response.status_code

    def _get_status(self, url: str, headers: Mapping[str, str]) -> int:
        with self._session.get(url, headers=headers, timeout=self._timeout_seconds, stream=True) as response:
            return response.status_code


class HttpFileFetcher:
    def __init__(self, session: requests.Session, timeout_seconds: float, chunk_size: int = 8 * 1024 * 1024):
        self._session = session
        self._timeout_seconds = timeout_seconds
        self._chunk_size = chunk_size

    def fetch(self, url: str, headers: Mapping[str, str]) -> DownloadedFile:
        with self._session.get(url, headers=headers, timeout=self._timeout_seconds, stream=True) as response:
            if response.status_code >= 400:
                raise UrlNotFoundError(f"{url} answered HTTP {response.status_code}")
            local_path = self._write_to_temp_file(response)
            return DownloadedFile(local_path, response.headers.get("Content-Type", ""), os.path.getsize(local_path))

    def _write_to_temp_file(self, response: requests.Response) -> str:
        with tempfile.NamedTemporaryFile(delete=False) as handle:
            for chunk in response.iter_content(chunk_size=self._chunk_size):
                handle.write(chunk)
            return handle.name


class FileNameResolver:
    def __init__(self, extra_types: Mapping[str, str] = DATA_FILE_TYPES):
        for extension, mime_type in extra_types.items():
            mimetypes.add_type(mime_type, extension)

    def resolve(self, url: str, content_type: str) -> str:
        name = PurePosixPath(unquote(urlparse(url).path)).name or "download"
        if self._has_specific_type(name):
            return name
        return name + self._extension_for(content_type)

    @staticmethod
    def _has_specific_type(name: str) -> bool:
        return mimetypes.guess_type(name)[0] not in GENERIC_MIME_TYPES

    @staticmethod
    def _extension_for(content_type: str) -> str:
        mime_type = (content_type or "").split(";")[0].strip().lower()
        if mime_type in GENERIC_MIME_TYPES:
            return ""
        return mimetypes.guess_extension(mime_type) or ""


class OneLakeStorage:
    def save(self, local_path: str, destination: str) -> None:
        notebookutils.fs.mkdirs(destination.rsplit("/", 1)[0])
        if notebookutils.fs.exists(destination):
            notebookutils.fs.rm(destination)
        notebookutils.fs.cp(f"file:{local_path}", destination)


class UrlDownloader:
    def __init__(
        self,
        validator: UrlValidator,
        existence_checker: ExistenceCheck,
        fetcher: FileFetcher,
        name_resolver: FileNameResolver,
        storage: FileStorage,
    ):
        self._validator = validator
        self._existence_checker = existence_checker
        self._fetcher = fetcher
        self._name_resolver = name_resolver
        self._storage = storage

    def download(self, request: DownloadRequest) -> DownloadResult:
        url = self._validator.validate(request.url)
        self._existence_checker.ensure_exists(url, request.headers)
        downloaded = self._fetcher.fetch(url, request.headers)
        try:
            destination = self._destination_for(request, url, downloaded.content_type)
            self._storage.save(downloaded.local_path, destination)
        finally:
            os.remove(downloaded.local_path)
        return DownloadResult(url, destination, downloaded.content_type, downloaded.size_bytes)

    def _destination_for(self, request: DownloadRequest, url: str, content_type: str) -> str:
        name = request.file_name or self._name_resolver.resolve(url, content_type)
        return f"{request.target_folder.rstrip('/')}/{name}"


def build_session(retries: int = 5, backoff_factor: float = 2.0) -> requests.Session:
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=RETRYABLE_STATUSES,
        allowed_methods=frozenset({"HEAD", "GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    session = requests.Session()
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def build_downloader(timeout_seconds: float, storage: Optional[FileStorage] = None) -> UrlDownloader:
    session = build_session()
    return UrlDownloader(
        validator=UrlValidator(),
        existence_checker=UrlExistenceChecker(session, timeout_seconds),
        fetcher=HttpFileFetcher(session, timeout_seconds),
        name_resolver=FileNameResolver(),
        storage=storage or OneLakeStorage(),
    )


def parse_headers(raw: str) -> dict:
    headers = json.loads(raw or "{}")
    if not isinstance(headers, dict):
        raise ValueError("headers_json must be a JSON object")
    return {str(key): str(value) for key, value in headers.items()}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if url:
    download_request = DownloadRequest(url, target_folder, file_name, parse_headers(headers_json))
    download_result = build_downloader(request_timeout_seconds).download(download_request)
    print(json.dumps(asdict(download_result), indent=2))
    notebookutils.notebook.exit(json.dumps(asdict(download_result)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
