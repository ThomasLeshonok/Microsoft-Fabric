import argparse
import time
from pathlib import Path

import requests
from azure.identity import AzureCliCredential
from fabric_cicd import FabricWorkspace, publish_all_items

REPOSITORY_DIRECTORY = Path(__file__).resolve().parents[1] / "workspace"
ITEM_TYPES = ["Lakehouse", "Warehouse", "Notebook", "DataPipeline", "Dataflow", "SemanticModel", "Report"]
FABRIC_API = "https://api.fabric.microsoft.com/v1"
JOB_DONE_STATUSES = {"Completed", "Failed", "Cancelled", "Deduped"}


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish workspace/ items to a Fabric workspace with fabric-cicd.")
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--environment", default="DEV")
    parser.add_argument("--tenant-id", default=None, help="Entra tenant that owns the Fabric workspace")
    return parser.parse_args()


def apply_dataflow_changes(workspace_id: str, credential: AzureCliCredential) -> None:
    """Updating a CI/CD dataflow definition only saves it; refreshes keep running the last published version until ApplyChanges runs."""
    headers = {"Authorization": f"Bearer {credential.get_token('https://api.fabric.microsoft.com/.default').token}"}
    repository_dataflows = {path.name.removesuffix(".Dataflow") for path in REPOSITORY_DIRECTORY.rglob("*.Dataflow")}
    response = requests.get(f"{FABRIC_API}/workspaces/{workspace_id}/dataflows", headers=headers, timeout=60)
    response.raise_for_status()
    for dataflow in response.json()["value"]:
        if dataflow["displayName"] not in repository_dataflows:
            continue
        started = requests.post(f"{FABRIC_API}/workspaces/{workspace_id}/dataflows/{dataflow['id']}/jobs/ApplyChanges/instances", headers=headers, timeout=60)
        started.raise_for_status()
        status = {}
        while status.get("status") not in JOB_DONE_STATUSES:
            time.sleep(5)
            status = requests.get(started.headers["Location"], headers=headers, timeout=60).json()
        if status["status"] != "Completed":
            raise RuntimeError(f"ApplyChanges for {dataflow['displayName']} ended {status['status']}: {status.get('failureReason')}")
        print(f"Applied changes to dataflow '{dataflow['displayName']}'")


def main() -> None:
    arguments = parse_arguments()
    credential = AzureCliCredential(tenant_id=arguments.tenant_id)
    workspace = FabricWorkspace(
        workspace_id=arguments.workspace_id,
        repository_directory=str(REPOSITORY_DIRECTORY),
        item_type_in_scope=ITEM_TYPES,
        environment=arguments.environment,
        token_credential=credential,
    )
    publish_all_items(workspace)
    apply_dataflow_changes(arguments.workspace_id, credential)


if __name__ == "__main__":
    main()
