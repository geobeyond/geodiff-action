import json

from actions import context, core

import functions
from geodiff import GeoDiffError, compute_diff, format_output


version: str = core.get_version()
core.info(f"Starting GeoDiff Action - \033[32;1m{version}")


# Inputs

base_file: str = core.get_input("base_file", True)
core.info(f"base_file: \033[36;1m{base_file}")
compare_file: str = core.get_input("compare_file", True)
core.info(f"compare_file: \033[36;1m{compare_file}")
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

try:
    diff_result = compute_diff(base_file, compare_file)
    has_changes = diff_result["has_changes"]
    formatted_output = format_output(diff_result, output_format)

    with core.group("Diff Result"):
        core.info(formatted_output)

except GeoDiffError as e:
    core.set_failed(f"GeoDiff error: {e}")
    raise SystemExit(1)


# Outputs

core.set_output("diff_result", formatted_output)
core.set_output("has_changes", str(has_changes).lower())


# Summary

if summary:
    diff_summary = diff_result["summary"]

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
