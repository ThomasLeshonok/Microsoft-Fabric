# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d8e27bc1-d18a-4eb0-b5cb-a0d3f492a089",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_download_zip_members
#
# Downloads a ZIP archive and lands only the requested members in OneLake; everything else in the archive is discarded.
#
# * `members_json` lists member file names. They match by base name, so folders inside the archive don't matter.
# * Default: the `taxi_zones` shapefile set (`.shp` geometry, `.shx` index, `.dbf` attributes such as `LocationID`,
#   `.prj` coordinate system) from the NYC TLC `taxi_zones.zip` into `Files/nyctaxi_yellow/taxi_zones`.
# * URL validation, existence check, HTTP fetch and OneLake storage come from `nb_http_download`.

# CELL ********************

%run nb_http_download

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
target_folder = "Files/nyctaxi_yellow/taxi_zones"
members_json = '["taxi_zones.shp", "taxi_zones.shx", "taxi_zones.dbf", "taxi_zones.prj"]'
headers_json = "{}"
request_timeout_seconds = 300

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import os
import shutil
import tempfile
import zipfile
from dataclasses import asdict, dataclass, field
from pathlib import PurePosixPath
from typing import Mapping, Optional, Protocol, Sequence


class MissingArchiveMemberError(DownloadError):
    pass


@dataclass(frozen=True)
class ZipMembersRequest:
    url: str
    target_folder: str
    member_names: tuple
    headers: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ExtractedMember:
    name: str
    local_path: str
    size_bytes: int


@dataclass(frozen=True)
class SavedMember:
    name: str
    destination: str
    size_bytes: int


class MemberExtractor(Protocol):
    def extract(self, archive_path: str, member_names: Sequence[str], output_folder: str) -> list: ...


class ZipMemberExtractor:
    def extract(self, archive_path: str, member_names: Sequence[str], output_folder: str) -> list:
        with zipfile.ZipFile(archive_path) as archive:
            selected = self._select(archive, member_names)
            return [self._extract_one(archive, info, output_folder) for info in selected]

    @staticmethod
    def _select(archive: zipfile.ZipFile, member_names: Sequence[str]) -> list:
        members_by_name = {PurePosixPath(info.filename).name: info for info in archive.infolist() if not info.is_dir()}
        missing = [name for name in member_names if name not in members_by_name]
        if missing:
            raise MissingArchiveMemberError(f"Not in archive: {missing}; available: {sorted(members_by_name)}")
        return [members_by_name[name] for name in member_names]

    @staticmethod
    def _extract_one(archive: zipfile.ZipFile, info: zipfile.ZipInfo, output_folder: str) -> ExtractedMember:
        name = PurePosixPath(info.filename).name
        local_path = os.path.join(output_folder, name)
        with archive.open(info) as source, open(local_path, "wb") as target:
            shutil.copyfileobj(source, target)
        return ExtractedMember(name, local_path, os.path.getsize(local_path))


class ZipMembersDownloader:
    def __init__(
        self,
        validator: UrlValidator,
        existence_checker: ExistenceCheck,
        fetcher: FileFetcher,
        extractor: MemberExtractor,
        storage: FileStorage,
    ):
        self._validator = validator
        self._existence_checker = existence_checker
        self._fetcher = fetcher
        self._extractor = extractor
        self._storage = storage

    def download(self, request: ZipMembersRequest) -> list:
        url = self._validator.validate(request.url)
        self._existence_checker.ensure_exists(url, request.headers)
        archive = self._fetcher.fetch(url, request.headers)
        try:
            return self._save_members(archive.local_path, request)
        finally:
            os.remove(archive.local_path)

    def _save_members(self, archive_path: str, request: ZipMembersRequest) -> list:
        with tempfile.TemporaryDirectory() as output_folder:
            extracted = self._extractor.extract(archive_path, request.member_names, output_folder)
            return [self._save(member, request.target_folder) for member in extracted]

    def _save(self, member: ExtractedMember, target_folder: str) -> SavedMember:
        destination = f"{target_folder.rstrip('/')}/{member.name}"
        self._storage.save(member.local_path, destination)
        return SavedMember(member.name, destination, member.size_bytes)


def build_zip_members_downloader(timeout_seconds: float, storage: Optional[FileStorage] = None) -> ZipMembersDownloader:
    session = build_session()
    return ZipMembersDownloader(
        validator=UrlValidator(),
        existence_checker=UrlExistenceChecker(session, timeout_seconds),
        fetcher=HttpFileFetcher(session, timeout_seconds),
        extractor=ZipMemberExtractor(),
        storage=storage or OneLakeStorage(),
    )


def parse_member_names(raw: str) -> tuple:
    names = json.loads(raw or "[]")
    if not isinstance(names, list) or not names:
        raise ValueError("members_json must be a non-empty JSON array of file names")
    return tuple(str(name) for name in names)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

zip_request = ZipMembersRequest(url, target_folder, parse_member_names(members_json), parse_headers(headers_json))
saved_members = build_zip_members_downloader(request_timeout_seconds).download(zip_request)
zip_report = json.dumps([asdict(member) for member in saved_members], indent=2)
print(zip_report)
runtime_context = notebookutils.runtime.context
if runtime_context["isForInteractive"] and not runtime_context["isReferenceRun"]:
    notebookutils.session.stop()
notebookutils.notebook.exit(zip_report)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
