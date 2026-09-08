import argparse
import subprocess
from pathlib import Path, PurePosixPath

from src.salesforce_cdc.landing import DEFAULT_LANDING_ROOT

DEFAULT_CATALOG = "salesforce_realtime_lakehouse"
DEFAULT_SCHEMA = "bronze"
DEFAULT_VOLUME = "salesforce_cdc_landing"


def volume_landing_uri(catalog: str, schema: str, volume: str) -> str:
    return f"dbfs:/Volumes/{catalog}/{schema}/{volume}/salesforce/opportunity"


def destination_uri(
    local_file: Path,
    local_root: Path,
    catalog: str,
    schema: str,
    volume: str,
) -> str:
    relative_path = local_file.relative_to(local_root)
    return str(PurePosixPath(volume_landing_uri(catalog, schema, volume)) / relative_path)


def event_files(local_root: Path) -> list[Path]:
    if not local_root.exists():
        return []
    return sorted(path for path in local_root.rglob("event-*.json") if path.is_file())


def upload_events(
    local_root: Path,
    catalog: str,
    schema: str,
    volume: str,
    profile: str | None = None,
) -> int:
    files = event_files(local_root)
    for local_file in files:
        destination = destination_uri(
            local_file, local_root, catalog, schema, volume
        )
        destination_directory = destination.rsplit("/", maxsplit=1)[0]
        mkdir_command = ["databricks", "fs", "mkdir", destination_directory]
        command = [
            "databricks",
            "fs",
            "cp",
            str(local_file),
            destination,
            "--overwrite",
        ]
        if profile:
            mkdir_command.extend(["--profile", profile])
            command.extend(["--profile", profile])
        subprocess.run(mkdir_command, check=True)
        subprocess.run(command, check=True)
        print(f"Uploaded: {local_file.relative_to(local_root)}")
    return len(files)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload local Salesforce CDC events to a Databricks Volume"
    )
    parser.add_argument("--local-root", type=Path, default=DEFAULT_LANDING_ROOT)
    parser.add_argument("--catalog", default=DEFAULT_CATALOG)
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--volume", default=DEFAULT_VOLUME)
    parser.add_argument("--profile")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_arguments()
    uploaded = upload_events(
        arguments.local_root,
        arguments.catalog,
        arguments.schema,
        arguments.volume,
        arguments.profile,
    )
    print(f"Upload complete: {uploaded} event file(s)")