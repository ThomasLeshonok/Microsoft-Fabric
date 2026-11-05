import argparse
from pathlib import Path

from azure.identity import AzureCliCredential
from fabric_cicd import FabricWorkspace, publish_all_items

REPOSITORY_DIRECTORY = Path(__file__).resolve().parents[1] / "workspace"
ITEM_TYPES = ["Lakehouse", "Notebook", "DataPipeline", "Dataflow"]


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish workspace/ items to a Fabric workspace with fabric-cicd.")
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--environment", default="DEV")
    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()
    workspace = FabricWorkspace(
        workspace_id=arguments.workspace_id,
        repository_directory=str(REPOSITORY_DIRECTORY),
        item_type_in_scope=ITEM_TYPES,
        environment=arguments.environment,
        token_credential=AzureCliCredential(),
    )
    publish_all_items(workspace)


if __name__ == "__main__":
    main()
