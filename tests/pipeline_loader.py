import json
import re

from notebook_loader import WORKSPACE, code_cells, item_folder

CURRENT_WORKSPACE_ID = "00000000-0000-0000-0000-000000000000"


def logical_id(item: str) -> str:
    return json.loads((item_folder(item) / ".platform").read_text(encoding="utf-8"))["config"]["logicalId"]


def notebooks_by_logical_id() -> dict:
    platforms = WORKSPACE.rglob("*.Notebook/.platform")
    return {json.loads(path.read_text(encoding="utf-8"))["config"]["logicalId"]: path.parent.stem for path in platforms}


def notebook_parameters(notebook: str) -> set:
    return set(re.findall(r"^(\w+) = ", code_cells(notebook)[0], re.MULTILINE))


def pipeline_properties(pipeline: str) -> dict:
    return json.loads((item_folder(f"{pipeline}.DataPipeline") / "pipeline-content.json").read_text(encoding="utf-8"))["properties"]


def notebook_activities(pipeline: str) -> dict:
    notebooks = notebooks_by_logical_id()
    activities = [activity for activity in pipeline_properties(pipeline)["activities"] if activity["type"] == "TridentNotebook"]
    return {notebooks[activity["typeProperties"]["notebookId"]]: activity for activity in activities}


def parameter_values(activity: dict) -> dict:
    return {name: parameter["value"] for name, parameter in activity["typeProperties"].get("parameters", {}).items()}
