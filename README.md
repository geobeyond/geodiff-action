# GeoDiff Action

> Compare GeoPackage and SQLite files in your CI/CD pipeline with detailed change detection

A GitHub Action for comparing GeoPackage and SQLite database files using [pygeodiff](https://github.com/MerginMaps/geodiff). Automatically detect insertions, updates, and deletions in your geospatial data during pull requests or CI workflows. Get structured JSON or human-readable summaries of all changes across tables, with full integration into GitHub Actions job summaries.

## Features

- Compare GeoPackage (.gpkg) and SQLite (.sqlite, .db) files
- Detect inserted, updated, and deleted records
- **Auto-compare with previous git commit** when only one file is provided
- Output diff results in JSON or summary format
- Generate job summaries with detailed change reports
- Uses the powerful [pygeodiff](https://pypi.org/project/pygeodiff/) library from Mergin Maps

## Usage

### Compare two files

```yaml
- uses: geobeyond/geodiff-action@v1
  with:
    base_file: 'path/to/base.gpkg'
    compare_file: 'path/to/compare.gpkg'
    output_format: 'json'
```

### Auto-compare with previous commit

When `compare_file` is not provided, the action automatically compares the current version of `base_file` with its version from the previous commit:

```yaml
- uses: actions/checkout@v4
  with:
    fetch-depth: 2  # Need at least 2 commits for comparison

- uses: geobeyond/geodiff-action@v1
  with:
    base_file: 'data/spatial.gpkg'
    # compare_file omitted - will compare with previous commit
```

## Inputs

| Input | Description | Required | Default |
|-------|-------------|----------|---------|
| `base_file` | Path to the GeoPackage/SQLite file | Yes | - |
| `compare_file` | Path to the file to compare against base. If not provided, compares with previous git commit. | No | - |
| `output_format` | Output format for the diff result (json, summary) | No | `json` |
| `summary` | Add Summary to Job | No | `true` |
| `token` | GitHub Token | No | `${{ github.token }}` |

## Outputs

| Output | Description |
|--------|-------------|
| `diff_result` | The diff result output |
| `has_changes` | Boolean indicating if changes were detected |

## Example Workflow

```yaml
name: GeoPackage Diff Check

on:
  pull_request:
    paths:
      - '**.gpkg'

jobs:
  diff:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Get base file from main branch
        run: |
          git show origin/main:data/spatial.gpkg > base.gpkg

      - name: Run GeoDiff
        id: geodiff
        uses: geobeyond/geodiff-action@v1
        with:
          base_file: 'base.gpkg'
          compare_file: 'data/spatial.gpkg'
          output_format: 'summary'

      - name: Check for changes
        if: steps.geodiff.outputs.has_changes == 'true'
        run: |
          echo "Changes detected in GeoPackage!"
          echo "${{ steps.geodiff.outputs.diff_result }}"
```

## Output Format

### JSON Output

```json
{
  "base_file": "base.gpkg",
  "compare_file": "compare.gpkg",
  "has_changes": true,
  "summary": {
    "total_changes": 5,
    "inserts": 2,
    "updates": 2,
    "deletes": 1
  },
  "changes": {
    "geodiff": [...]
  }
}
```

### Summary Output

```
GeoDiff Summary: base.gpkg vs compare.gpkg
  Has Changes:   Yes
  Total Changes: 5
  Inserts:       2
  Updates:       2
  Deletes:       1

  Tables affected:
    - my_layer: 5 change(s)
```

## Supported File Formats

- GeoPackage (`.gpkg`)
- SQLite (`.sqlite`, `.db`)

## Development

This action is built using Python and UV package manager with [pygeodiff](https://github.com/MerginMaps/geodiff).

### Prerequisites

- Python 3.10+
- UV package manager

### Local Development

```bash
# Install dependencies
uv sync --group test

# Run linters
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/

# Run tests
uv run pytest -v

# Run tests with coverage
uv run pytest --cov=src --cov-report=term-missing
```

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Acknowledgements

- [pygeodiff](https://github.com/MerginMaps/geodiff) - The underlying library for geospatial diff operations
- [Mergin Maps](https://merginmaps.com/) - Creators of the geodiff library
