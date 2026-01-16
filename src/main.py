import os
import json
import subprocess
from pathlib import Path

from actions import context, core

import functions
from geodiff import GeoDiffError, compute_diff, format_output
from git_utils import GitError, find_repo_root, get_file_from_commit, get_previous_commit, has_file_in_commit

# Configure git to trust all directories (needed for Docker containers)
# This must be done early before any git operations
subprocess.run(
    ["git", "config", "--global", "--add", "safe.directory", "*"],
    capture_output=True,
    check=False,
)

version: str = core.get_version()
core.info(f"Starting GeoDiff Action - \033[32;1m{version}")


# Inputs

base_file: str = core.get_input("base_file", True)
core.info(f"base_file: \033[36;1m{base_file}")
compare_file: str = core.get_input("compare_file") or ""
core.info(f"compare_file: \033[36;1m{compare_file or '(not provided - using git history)'}")
output_format: str = core.get_input("output_format") or "json"
core.info(f"output_format: \033[35;1m{output_format}")
summary: bool = core.get_bool("summary")
core.info(f"summary: \033[33;1m{summary}")
token: str = core.get_input("token", True)


# Debug

with core.group("uv"):
    functions.check_output("uv -V", False)
    functions.check_output("uv python dir", False)


event: dict = core.get_event()
with core.group("GitHub Event Data"):
    core.info(json.dumps(event, indent=4))


ctx = {k: v for k, v in vars(context).items() if not k.startswith("__")}
del ctx["os"]
with core.group("GitHub Context Data"):
    core.info(json.dumps(ctx, indent=4))


repository: dict = event.get("repository", {})
html_url: str = repository.get("html_url", "")
core.info(f"repository.html_url: {html_url}")


# Action Logic

core.info("Performing geospatial diff using pygeodiff...")

# Track temp files for cleanup
temp_files_to_cleanup: list[str] = []

# Initialize variables
actual_base: str | None = None
actual_compare: str | None = None
diff_result: dict = {}
has_changes: bool = False
formatted_output: str = ""

try:
    # Determine comparison mode
    if compare_file:
        # Standard mode: compare two provided files
        actual_base = base_file
        actual_compare = compare_file
        core.info("Mode: comparing two provided files")
    else:
        # Git history mode: compare current file with previous commit
        core.info("Mode: comparing with previous git commit")

        # Debug: show current working directory and file info
        import os

        cwd = os.getcwd()
        core.info(f"Current working directory: {cwd}")
        abs_base = Path(base_file).resolve()
        core.info(f"Absolute base_file path: {abs_base}")
        core.info(f"File exists: {abs_base.exists()}")
        core.info(f"Parent directory exists: {abs_base.parent.exists()}")

        # Find the repository root from the file's location
        repo_path = find_repo_root(base_file)
        core.info(f"find_repo_root returned: {repo_path}")

        if repo_path is None:
            # Additional debug info
            core.info("Checking if cwd is git repo...")
            from git_utils import is_git_repo

            core.info(f"is_git_repo(cwd): {is_git_repo(cwd)}")
            core.info(f"is_git_repo(parent): {is_git_repo(str(abs_base.parent))}")
            core.set_failed("Not a git repository. Cannot compare with previous commit.")
            raise SystemExit(1)

        core.info(f"Git repository root: {repo_path}")

        # Calculate the file path relative to the repo root
        abs_base_file = Path(base_file).resolve()
        file_rel_path = str(abs_base_file.relative_to(repo_path))

        # Check if file exists in previous commit
        try:
            prev_commit = get_previous_commit(repo_path)
            core.info(f"Previous commit: {prev_commit[:8]}")
        except GitError as e:
            core.set_failed(f"Cannot get previous commit: {e}")
            raise SystemExit(1) from e

        file_exists_in_prev = has_file_in_commit(repo_path, file_rel_path, prev_commit)
        core.info(f"File exists in previous commit: {file_exists_in_prev}")

        if not file_exists_in_prev:
            core.info(f"File {file_rel_path} does not exist in previous commit. This is a new file.")
            # Create empty result for new file
            diff_result = {
                "base_file": "(new file)",
                "compare_file": base_file,
                "has_changes": True,
                "summary": {
                    "total_changes": 0,
                    "inserts": 0,
                    "updates": 0,
                    "deletes": 0,
                },
                "changes": {"geodiff": []},
                "note": "File is new in this commit",
            }
            has_changes = True
            core.info(f"New file: has_changes={has_changes}, diff_result keys={list(diff_result.keys())}")
            try:
                core.info("Calling format_output...")
                formatted_output = format_output(diff_result, output_format)
                core.info(f"format_output returned, length={len(formatted_output)}")

                with core.group("Diff Result"):
                    core.info(formatted_output)
                core.info("Finished new file handling")
            except Exception as e:
                core.info(f"ERROR in new file handling: {type(e).__name__}: {e}")
                raise
            # actual_base and actual_compare remain None, skipping diff computation
        else:
            # Extract file from previous commit
            try:
                prev_file_path = get_file_from_commit(repo_path, file_rel_path, prev_commit)
                temp_files_to_cleanup.append(prev_file_path)
                core.info(f"Extracted previous version to: {prev_file_path}")
            except GitError as e:
                core.set_failed(f"Failed to extract file from previous commit: {e}")
                raise SystemExit(1) from e

            # Previous commit version is the base, current file is what we compare against
            actual_base = prev_file_path
            actual_compare = base_file

    # Perform diff if we have files to compare
    if actual_base is not None and actual_compare is not None:
        diff_result = compute_diff(actual_base, actual_compare)
        has_changes = diff_result["has_changes"]
        formatted_output = format_output(diff_result, output_format)

        with core.group("Diff Result"):
            core.info(formatted_output)

except GeoDiffError as e:
    core.set_failed(f"GeoDiff error: {e}")
    raise SystemExit(1) from e
finally:
    # Cleanup temp files
    for temp_file in temp_files_to_cleanup:
        try:
            Path(temp_file).unlink(missing_ok=True)
        except Exception:
            pass


# Outputs
github_output = os.environ.get("GITHUB_OUTPUT", "NOT SET")
core.info(f"GITHUB_OUTPUT env var: {github_output}")
core.info(f"Setting outputs: has_changes={has_changes}, diff_result type={type(diff_result).__name__}")
# For JSON output, use compact format to avoid multiline issues with GitHub Actions
if output_format == "json":
    compact_output = json.dumps(diff_result)
    core.set_output("diff_result", compact_output)
    core.info(f"Set diff_result output (json, length={len(compact_output)})")
else:
    # For summary format, escape newlines
    escaped_output = formatted_output.replace("\n", "%0A")
    core.set_output("diff_result", escaped_output)
    core.info(f"Set diff_result output (summary, length={len(escaped_output)})")
core.set_output("has_changes", str(has_changes).lower())
core.info(f"Set has_changes output: {str(has_changes).lower()}")


# Summary

if summary:
    diff_summary: dict = diff_result["summary"]

    inputs_table = ["<table><tr><th>Input</th><th>Value</th></tr>"]
    for name, value in [("base_file", base_file), ("compare_file", compare_file), ("output_format", output_format)]:
        inputs_table.append(f"<tr><td>{name}</td><td>{value or '-'}</td></tr>")
    inputs_table.append("</table>")

    results_table = ["<table><tr><th>Change Type</th><th>Count</th></tr>"]
    results_table.append(f"<tr><td>Total Changes</td><td>{diff_summary['total_changes']}</td></tr>")
    results_table.append(f"<tr><td>Inserts</td><td>{diff_summary['inserts']}</td></tr>")
    results_table.append(f"<tr><td>Updates</td><td>{diff_summary['updates']}</td></tr>")
    results_table.append(f"<tr><td>Deletes</td><td>{diff_summary['deletes']}</td></tr>")
    results_table.append("</table>")

    core.summary("### GeoDiff Action Results")
    core.summary(f"**Base file:** `{base_file}`")
    core.summary(f"**Compare file:** `{compare_file}`")
    core.summary(f"**Changes detected:** {'Yes' if has_changes else 'No'}")
    core.summary(f"\n{''.join(results_table)}\n")
    core.summary(f"<details><summary>Inputs</summary>{''.join(inputs_table)}</details>\n")
    if html_url:
        core.summary(f"[Report an issue or request a feature]({html_url}/issues)")


print("\033[32;1mGeoDiff Action completed successfully")
