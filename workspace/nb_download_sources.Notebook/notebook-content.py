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

# # nb_download_sources
#
# Calls `nb_http_download` once per entry in `sources_json` and fails at the end if any download failed.
#
# Each source: `{"url": "...", "target_folder": "Files/...", "file_name": "optional", "headers": {}}`

# PARAMETERS CELL ********************

sources_json = """[
  {
    "url": "https://data-api.ecb.europa.eu/service/data/EXR/D..EUR.SP00.A?format=csvdata&startPeriod=2016-03-01",
    "target_folder": "Files/ECB_FX",
    "file_name": "exr_daily_eur_reference_rates.csv"
  },
  {
    "url": "https://api.worldbank.org/v2/country/all/indicator/NY.GDP.PCAP.CD?format=json&per_page=1000&date=2016:2026&page=1",
    "target_folder": "Files/NY_GDP",
    "file_name": "NY.GDP.PCAP.CD_page1.json"
  },
  {
    "url": "https://api.worldbank.org/v2/country/all/indicator/NY.GDP.PCAP.CD?format=json&per_page=1000&date=2016:2026&page=2",
    "target_folder": "Files/NY_GDP",
    "file_name": "NY.GDP.PCAP.CD_page2.json"
  },
  {
    "url": "https://api.worldbank.org/v2/country/all/indicator/NY.GDP.PCAP.CD?format=json&per_page=1000&date=2016:2026&page=3",
    "target_folder": "Files/NY_GDP",
    "file_name": "NY.GDP.PCAP.CD_page3.json"
  }
]"""
downloader_notebook = "nb_http_download"
notebook_timeout_seconds = 1800

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from dataclasses import asdict, dataclass, field
from typing import Iterable, Protocol


@dataclass(frozen=True)
class Source:
    url: str
    target_folder: str
    file_name: str = ""
    headers: dict = field(default_factory=dict)


@dataclass(frozen=True)
class SourceOutcome:
    url: str
    succeeded: bool
    detail: dict


@dataclass(frozen=True)
class BatchReport:
    outcomes: list

    @property
    def failures(self) -> list:
        return [outcome for outcome in self.outcomes if not outcome.succeeded]

    def to_json(self) -> str:
        return json.dumps([asdict(outcome) for outcome in self.outcomes], indent=2)


class DownloadRunner(Protocol):
    def run(self, source: Source) -> dict: ...


class NotebookDownloadRunner:
    def __init__(self, notebook_name: str, timeout_seconds: int):
        self._notebook_name = notebook_name
        self._timeout_seconds = timeout_seconds

    def run(self, source: Source) -> dict:
        exit_value = notebookutils.notebook.run(self._notebook_name, self._timeout_seconds, self._arguments(source))
        return json.loads(exit_value)

    @staticmethod
    def _arguments(source: Source) -> dict:
        return {
            "url": source.url,
            "target_folder": source.target_folder,
            "file_name": source.file_name,
            "headers_json": json.dumps(source.headers),
        }


class BatchDownloader:
    def __init__(self, runner: DownloadRunner):
        self._runner = runner

    def download_all(self, sources: Iterable[Source]) -> BatchReport:
        return BatchReport([self._download_one(source) for source in sources])

    def _download_one(self, source: Source) -> SourceOutcome:
        try:
            return SourceOutcome(source.url, True, self._runner.run(source))
        except Exception as error:
            return SourceOutcome(source.url, False, {"error": str(error)})


def parse_sources(raw: str) -> list:
    entries = json.loads(raw)
    if not isinstance(entries, list):
        raise ValueError("sources_json must be a JSON array")
    return [Source(**entry) for entry in entries]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

batch_report = BatchDownloader(NotebookDownloadRunner(downloader_notebook, notebook_timeout_seconds)).download_all(
    parse_sources(sources_json)
)
print(batch_report.to_json())

if batch_report.failures:
    raise RuntimeError(f"{len(batch_report.failures)} of {len(batch_report.outcomes)} downloads failed")

notebookutils.notebook.exit(batch_report.to_json())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
