"""GeoDiff - Core logic for geospatial file comparison using pygeodiff."""

import json
import tempfile
from pathlib import Path
from typing import Any

import pygeodiff


class GeoDiffError(Exception):
    """Custom exception for GeoDiff errors."""

    pass


# Supported file extensions
SUPPORTED_EXTENSIONS = {".gpkg", ".sqlite", ".db"}


def validate_file(file_path: str) -> Path:
    """
    Validate that a file exists and has a supported extension.

    Args:
        file_path: Path to the file to validate.

    Returns:
        Path object for the validated file.

    Raises:
        GeoDiffError: If the file doesn't exist or has unsupported extension.
    """
    path = Path(file_path)

    if not path.exists():
        raise GeoDiffError(f"File not found: {file_path}")

    if path.suffix.lower() not in SUPPORTED_EXTENSIONS:
        raise GeoDiffError(
            f"Unsupported file format: {path.suffix}. Supported formats: {', '.join(SUPPORTED_EXTENSIONS)}"
        )

    return path


def create_changeset(base_file: str, compare_file: str) -> tuple[str, Path]:
    """
    Create a changeset between two GeoPackage files.

    Args:
        base_file: Path to the base GeoPackage file.
        compare_file: Path to the file to compare against base.

    Returns:
        Tuple of (changeset_path, temp_dir) - caller should clean up temp_dir.

    Raises:
        GeoDiffError: If changeset creation fails.
    """
    validate_file(base_file)
    validate_file(compare_file)

    temp_dir = Path(tempfile.mkdtemp())
    changeset_path = temp_dir / "changeset.diff"

    try:
        geodiff = pygeodiff.GeoDiff()
        geodiff.create_changeset(base_file, compare_file, str(changeset_path))
    except pygeodiff.GeoDiffLibError as e:
        err = str(e).replace("\n", " ").replace("\r", " ")
        raise GeoDiffError(f"Failed to create changeset: {err}") from e

    return str(changeset_path), temp_dir


def list_changes_json(changeset_path: str) -> dict[str, Any]:
    """
    Export changes from a binary diff file to JSON format.

    Uses read_changeset to iterate through changes and build a structured
    representation compatible with the expected format.

    Args:
        changeset_path: Path to the changeset diff file.

    Returns:
        Dictionary containing the changes in format:
        {
            "geodiff": [
                {
                    "table": "table_name",
                    "changes": [{"type": "insert"}, {"type": "update"}, ...]
                }
            ]
        }

    Raises:
        GeoDiffError: If listing changes fails.
    """
    try:
        geodiff = pygeodiff.GeoDiff()
        reader = geodiff.read_changeset(changeset_path)

        # Group changes by table
        tables_changes: dict[str, list[dict[str, str]]] = {}

        for entry in reader:
            table_name = entry.table.name
            op = entry.operation

            # Determine change type
            if op == entry.OP_INSERT:
                change_type = "insert"
            elif op == entry.OP_UPDATE:
                change_type = "update"
            elif op == entry.OP_DELETE:
                change_type = "delete"
            else:
                continue  # Skip unknown operations

            if table_name not in tables_changes:
                tables_changes[table_name] = []

            tables_changes[table_name].append({"type": change_type})

        # Build result in expected format
        result = {"geodiff": [{"table": table, "changes": changes} for table, changes in tables_changes.items()]}

        return result

    except pygeodiff.GeoDiffLibError as e:
        raise GeoDiffError(f"Failed to list changes: {e}") from e


def has_changes(changeset_path: str) -> bool:
    """
    Check if a changeset contains any changes.

    Args:
        changeset_path: Path to the changeset diff file.

    Returns:
        True if there are changes, False otherwise.
    """
    try:
        geodiff = pygeodiff.GeoDiff()
        return geodiff.has_changes(changeset_path)
    except pygeodiff.GeoDiffLibError:
        return False


def count_changes(changeset_path: str) -> int:
    """
    Count the number of changes in a changeset.

    Args:
        changeset_path: Path to the changeset diff file.

    Returns:
        Number of changes in the changeset.
    """
    try:
        geodiff = pygeodiff.GeoDiff()
        return geodiff.changes_count(changeset_path)
    except pygeodiff.GeoDiffLibError:
        return 0


def compute_diff(base_file: str, compare_file: str) -> dict[str, Any]:
    """
    Compute the difference between two GeoPackage files.

    Args:
        base_file: Path to the base GeoPackage file.
        compare_file: Path to the comparison GeoPackage file.

    Returns:
        Dictionary containing the diff results.

    Raises:
        GeoDiffError: If files cannot be loaded or compared.
    """
    # Validate files
    base_path = validate_file(base_file)
    compare_path = validate_file(compare_file)

    # Create changeset
    changeset_path, temp_dir = create_changeset(base_file, compare_file)

    try:
        # Check if there are changes
        changes_exist = has_changes(changeset_path)
        change_count = count_changes(changeset_path)

        # Get detailed changes
        if changes_exist:
            changes_detail = list_changes_json(changeset_path)
        else:
            changes_detail = {"geodiff": []}

        # Parse changes to get summary
        geodiff_changes = changes_detail.get("geodiff", [])

        # Count by operation type
        insert_count = 0
        update_count = 0
        delete_count = 0

        for table_changes in geodiff_changes:
            for change in table_changes.get("changes", []):
                op = change.get("type", "")
                if op == "insert":
                    insert_count += 1
                elif op == "update":
                    update_count += 1
                elif op == "delete":
                    delete_count += 1

        return {
            "base_file": str(base_path),
            "compare_file": str(compare_path),
            "has_changes": changes_exist,
            "summary": {
                "total_changes": change_count,
                "inserts": insert_count,
                "updates": update_count,
                "deletes": delete_count,
            },
            "changes": changes_detail,
        }

    finally:
        # Cleanup temp files
        changeset_file = Path(changeset_path)
        if changeset_file.exists():
            changeset_file.unlink()
        if temp_dir.exists():
            temp_dir.rmdir()


def format_output(diff_result: dict[str, Any], output_format: str = "json") -> str:
    """
    Format the diff result in the specified format.

    Args:
        diff_result: The diff result from compute_diff.
        output_format: Output format (json, summary).

    Returns:
        Formatted string output.
    """
    if output_format == "summary":
        summary = diff_result["summary"]
        lines = [
            f"GeoDiff Summary: {diff_result['base_file']} vs {diff_result['compare_file']}",
            f"  Has Changes:   {'Yes' if diff_result['has_changes'] else 'No'}",
            f"  Total Changes: {summary['total_changes']}",
            f"  Inserts:       {summary['inserts']}",
            f"  Updates:       {summary['updates']}",
            f"  Deletes:       {summary['deletes']}",
        ]

        # Add table details if available
        geodiff_changes = diff_result.get("changes", {}).get("geodiff", [])
        if geodiff_changes:
            lines.append("\n  Tables affected:")
            for table in geodiff_changes:
                table_name = table.get("table", "unknown")
                table_changes = len(table.get("changes", []))
                lines.append(f"    - {table_name}: {table_changes} change(s)")

        return "\n".join(lines)

    else:  # json (default)
        return json.dumps(diff_result, indent=2)
