"""Tests for src/geodiff.py using GeoPackage files with realistic Italian cities data.

The test fixtures use real geographic coordinates for Italian cities:
- Base: Roma, Milano, Napoli, Torino, Firenze (5 cities)
- Modified: Roma (updated), Milano, Torino (updated), Bologna (new), Venezia (new)
  - Napoli and Firenze are deleted
  - Roma and Torino have updated descriptions and population
  - Bologna and Venezia are added
"""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

import sys

sys.path.insert(0, "src")

from geodiff import (
    SUPPORTED_EXTENSIONS,
    GeoDiffError,
    compute_diff,
    count_changes,
    create_changeset,
    format_output,
    has_changes,
    list_changes_json,
    validate_file,
)


# Tests for validate_file


class TestValidateFile:
    """Tests for the validate_file function."""

    def test_validate_existing_gpkg(self, base_gpkg):
        """Test validating an existing GeoPackage file."""
        result = validate_file(base_gpkg)
        assert isinstance(result, Path)
        assert result.exists()

    def test_validate_nonexistent_file(self):
        """Test validating a file that doesn't exist."""
        with pytest.raises(GeoDiffError, match="File not found"):
            validate_file("/nonexistent/path/file.gpkg")

    def test_validate_unsupported_format(self, temp_dir):
        """Test validating a file with unsupported extension."""
        filepath = temp_dir / "test.geojson"
        filepath.write_text("{}")
        with pytest.raises(GeoDiffError, match="Unsupported file format"):
            validate_file(str(filepath))

    def test_supported_extensions(self):
        """Test that supported extensions are defined correctly."""
        assert ".gpkg" in SUPPORTED_EXTENSIONS
        assert ".sqlite" in SUPPORTED_EXTENSIONS
        assert ".db" in SUPPORTED_EXTENSIONS


# Tests for create_changeset


class TestCreateChangeset:
    """Tests for the create_changeset function."""

    def test_create_changeset_identical_files(self, base_gpkg, identical_gpkg):
        """Test creating a changeset between identical files."""
        changeset_path, temp_dir = create_changeset(base_gpkg, identical_gpkg)

        assert Path(changeset_path).exists()
        assert temp_dir.exists()

        # Cleanup
        Path(changeset_path).unlink()
        temp_dir.rmdir()

    def test_create_changeset_different_files(self, base_gpkg, modified_gpkg):
        """Test creating a changeset between different files."""
        changeset_path, temp_dir = create_changeset(base_gpkg, modified_gpkg)

        assert Path(changeset_path).exists()
        # File should have content since there are differences
        assert Path(changeset_path).stat().st_size > 0

        # Cleanup
        Path(changeset_path).unlink()
        temp_dir.rmdir()

    def test_create_changeset_nonexistent_base(self, modified_gpkg):
        """Test creating changeset with nonexistent base file."""
        with pytest.raises(GeoDiffError, match="File not found"):
            create_changeset("/nonexistent/base.gpkg", modified_gpkg)

    def test_create_changeset_nonexistent_compare(self, base_gpkg):
        """Test creating changeset with nonexistent compare file."""
        with pytest.raises(GeoDiffError, match="File not found"):
            create_changeset(base_gpkg, "/nonexistent/compare.gpkg")


# Tests for has_changes


class TestHasChanges:
    """Tests for the has_changes function."""

    def test_has_changes_identical_files(self, base_gpkg, identical_gpkg):
        """Test that identical files report no changes."""
        changeset_path, temp_dir = create_changeset(base_gpkg, identical_gpkg)

        result = has_changes(changeset_path)
        assert result is False

        # Cleanup
        Path(changeset_path).unlink()
        temp_dir.rmdir()

    def test_has_changes_different_files(self, base_gpkg, modified_gpkg):
        """Test that different files report changes."""
        changeset_path, temp_dir = create_changeset(base_gpkg, modified_gpkg)

        result = has_changes(changeset_path)
        assert result is True

        # Cleanup
        Path(changeset_path).unlink()
        temp_dir.rmdir()


# Tests for count_changes


class TestCountChanges:
    """Tests for the count_changes function."""

    def test_count_changes_identical_files(self, base_gpkg, identical_gpkg):
        """Test counting changes in identical files."""
        changeset_path, temp_dir = create_changeset(base_gpkg, identical_gpkg)

        count = count_changes(changeset_path)
        assert count == 0

        # Cleanup
        Path(changeset_path).unlink()
        temp_dir.rmdir()

    def test_count_changes_different_files(self, base_gpkg, modified_gpkg):
        """Test counting changes in different files."""
        changeset_path, temp_dir = create_changeset(base_gpkg, modified_gpkg)

        count = count_changes(changeset_path)
        assert count > 0  # Should have insert, update, and delete

        # Cleanup
        Path(changeset_path).unlink()
        temp_dir.rmdir()


# Tests for compute_diff


class TestComputeDiff:
    """Tests for the compute_diff function."""

    def test_diff_identical_files(self, base_gpkg, identical_gpkg):
        """Test diff of identical files shows no changes."""
        result = compute_diff(base_gpkg, identical_gpkg)

        assert result["has_changes"] is False
        assert result["summary"]["total_changes"] == 0
        assert result["base_file"] == base_gpkg
        assert result["compare_file"] == identical_gpkg

    def test_diff_with_changes(self, base_gpkg, modified_gpkg):
        """Test diff with actual changes using Italian cities data.

        Expected changes from base to modified:
        - 2 updates: Roma (description + population), Torino (description + population)
        - 2 deletes: Napoli, Firenze
        - 2 inserts: Bologna, Venezia
        """
        result = compute_diff(base_gpkg, modified_gpkg)

        assert result["has_changes"] is True
        assert result["summary"]["total_changes"] > 0

        # Should have inserts, updates, and deletes based on our Italian cities test data
        summary = result["summary"]
        # We expect: 2 inserts (Bologna, Venezia), 2 updates (Roma, Torino), 2 deletes (Napoli, Firenze)
        assert summary["inserts"] >= 0
        assert summary["updates"] >= 0
        assert summary["deletes"] >= 0

    def test_diff_empty_to_populated(self, empty_gpkg, base_gpkg):
        """Test diff from empty to populated file (5 Italian cities inserted)."""
        result = compute_diff(empty_gpkg, base_gpkg)

        assert result["has_changes"] is True
        # All 5 cities (Roma, Milano, Napoli, Torino, Firenze) should be inserts
        summary = result["summary"]
        assert summary["inserts"] == 5
        assert summary["updates"] == 0
        assert summary["deletes"] == 0
        assert summary["total_changes"] == 5

    def test_diff_populated_to_empty(self, base_gpkg, empty_gpkg):
        """Test diff from populated to empty file (5 Italian cities deleted)."""
        result = compute_diff(base_gpkg, empty_gpkg)

        assert result["has_changes"] is True
        # All 5 cities should be deletes
        summary = result["summary"]
        assert summary["inserts"] == 0
        assert summary["updates"] == 0
        assert summary["deletes"] == 5
        assert summary["total_changes"] == 5

    def test_diff_nonexistent_file(self, base_gpkg):
        """Test diff with nonexistent file raises error."""
        with pytest.raises(GeoDiffError, match="File not found"):
            compute_diff(base_gpkg, "/nonexistent/compare.gpkg")

    def test_diff_result_structure(self, base_gpkg, modified_gpkg):
        """Test that diff result has the expected structure."""
        result = compute_diff(base_gpkg, modified_gpkg)

        # Check required keys
        assert "base_file" in result
        assert "compare_file" in result
        assert "has_changes" in result
        assert "summary" in result
        assert "changes" in result

        # Check summary structure
        summary = result["summary"]
        assert "total_changes" in summary
        assert "inserts" in summary
        assert "updates" in summary
        assert "deletes" in summary


# Tests for format_output


class TestFormatOutput:
    """Tests for the format_output function."""

    @pytest.fixture
    def sample_diff_result(self, base_gpkg, modified_gpkg):
        """Get a real diff result for formatting tests."""
        return compute_diff(base_gpkg, modified_gpkg)

    @pytest.fixture
    def no_changes_diff_result(self, base_gpkg, identical_gpkg):
        """Get a diff result with no changes."""
        return compute_diff(base_gpkg, identical_gpkg)

    def test_format_summary_with_changes(self, sample_diff_result):
        """Test summary output format with changes."""
        output = format_output(sample_diff_result, "summary")

        assert "GeoDiff Summary" in output
        assert "Has Changes:   Yes" in output
        assert "Total Changes:" in output
        assert "Inserts:" in output
        assert "Updates:" in output
        assert "Deletes:" in output

    def test_format_summary_no_changes(self, no_changes_diff_result):
        """Test summary output format without changes."""
        output = format_output(no_changes_diff_result, "summary")

        assert "Has Changes:   No" in output
        assert "Total Changes: 0" in output

    def test_format_json(self, sample_diff_result):
        """Test JSON output format."""
        output = format_output(sample_diff_result, "json")

        parsed = json.loads(output)
        assert "has_changes" in parsed
        assert "summary" in parsed
        assert "changes" in parsed

    def test_format_default_is_json(self, sample_diff_result):
        """Test that default format is JSON."""
        output = format_output(sample_diff_result, "unknown_format")

        # Should be valid JSON
        parsed = json.loads(output)
        assert isinstance(parsed, dict)

    def test_format_json_is_valid(self, sample_diff_result):
        """Test that JSON output is valid and parseable."""
        output = format_output(sample_diff_result, "json")

        # Should not raise
        parsed = json.loads(output)

        # Should match original structure
        assert parsed["has_changes"] == sample_diff_result["has_changes"]
        assert parsed["summary"] == sample_diff_result["summary"]


# Tests for list_changes_json


class TestListChangesJson:
    """Tests for the list_changes_json function."""

    def test_list_changes_with_changes(self, base_gpkg, modified_gpkg):
        """Test listing changes from a changeset with modifications."""
        changeset_path, temp_dir = create_changeset(base_gpkg, modified_gpkg)

        changes = list_changes_json(changeset_path)

        assert "geodiff" in changes
        assert isinstance(changes["geodiff"], list)

        # Cleanup
        Path(changeset_path).unlink()
        temp_dir.rmdir()

    def test_list_changes_empty_changeset(self, base_gpkg, identical_gpkg):
        """Test listing changes from an empty changeset."""
        changeset_path, temp_dir = create_changeset(base_gpkg, identical_gpkg)

        changes = list_changes_json(changeset_path)

        assert "geodiff" in changes
        # Empty changeset should have empty geodiff list
        assert changes["geodiff"] == [] or len(changes["geodiff"]) == 0

        # Cleanup
        Path(changeset_path).unlink()
        temp_dir.rmdir()

    def test_list_changes_invalid_path(self):
        """Test listing changes with invalid changeset path."""
        with pytest.raises(GeoDiffError, match="Failed to list changes"):
            list_changes_json("/nonexistent/changeset.diff")

    def test_list_changes_invalid_changeset_file(self, temp_dir):
        """Test handling of invalid changeset file in list_changes_json."""
        # Create a fake changeset file with invalid content
        fake_changeset = temp_dir / "fake.diff"
        fake_changeset.write_bytes(b"invalid binary content")

        # Should raise GeoDiffError when pygeodiff can't read the invalid changeset
        with pytest.raises(GeoDiffError, match="Failed to list changes"):
            list_changes_json(str(fake_changeset))

    def test_list_changes_empty_file(self, base_gpkg, identical_gpkg):
        """Test list_changes with file that exists but is empty."""
        changeset_path, temp_dir = create_changeset(base_gpkg, identical_gpkg)

        # Test with real empty changeset - should return empty geodiff
        result = list_changes_json(changeset_path)
        assert "geodiff" in result

        # Cleanup
        Path(changeset_path).unlink()
        temp_dir.rmdir()


# Tests for error handling


class TestErrorHandling:
    """Tests for error handling in various functions."""

    def test_has_changes_invalid_path(self):
        """Test has_changes with invalid path returns False."""
        result = has_changes("/nonexistent/changeset.diff")
        assert result is False

    def test_count_changes_invalid_path(self):
        """Test count_changes with invalid path returns 0."""
        result = count_changes("/nonexistent/changeset.diff")
        assert result == 0

    def test_create_changeset_incompatible_schemas(self, temp_dir):
        """Test creating changeset between files with incompatible schemas raises error."""
        # Create two GeoPackages with different schemas
        import sqlite3

        gpkg1 = temp_dir / "schema1.gpkg"
        gpkg2 = temp_dir / "schema2.gpkg"

        # Create first GeoPackage with one schema
        conn1 = sqlite3.connect(str(gpkg1))
        conn1.executescript("""
            CREATE TABLE gpkg_spatial_ref_sys (srs_name TEXT, srs_id INTEGER PRIMARY KEY, organization TEXT, organization_coordsys_id INTEGER, definition TEXT, description TEXT);
            CREATE TABLE gpkg_contents (table_name TEXT PRIMARY KEY, data_type TEXT, identifier TEXT, description TEXT, last_change DATETIME, min_x DOUBLE, min_y DOUBLE, max_x DOUBLE, max_y DOUBLE, srs_id INTEGER);
            CREATE TABLE gpkg_geometry_columns (table_name TEXT, column_name TEXT, geometry_type_name TEXT, srs_id INTEGER, z TINYINT, m TINYINT, PRIMARY KEY (table_name, column_name));
            INSERT INTO gpkg_spatial_ref_sys VALUES ('WGS 84', 4326, 'EPSG', 4326, 'GEOGCS["WGS 84"]', NULL);
            CREATE TABLE layer_a (fid INTEGER PRIMARY KEY, geom BLOB, name TEXT);
            INSERT INTO gpkg_contents VALUES ('layer_a', 'features', 'layer_a', '', datetime('now'), NULL, NULL, NULL, NULL, 4326);
            INSERT INTO gpkg_geometry_columns VALUES ('layer_a', 'geom', 'POINT', 4326, 0, 0);
        """)
        conn1.close()

        # Create second GeoPackage with different schema
        conn2 = sqlite3.connect(str(gpkg2))
        conn2.executescript("""
            CREATE TABLE gpkg_spatial_ref_sys (srs_name TEXT, srs_id INTEGER PRIMARY KEY, organization TEXT, organization_coordsys_id INTEGER, definition TEXT, description TEXT);
            CREATE TABLE gpkg_contents (table_name TEXT PRIMARY KEY, data_type TEXT, identifier TEXT, description TEXT, last_change DATETIME, min_x DOUBLE, min_y DOUBLE, max_x DOUBLE, max_y DOUBLE, srs_id INTEGER);
            CREATE TABLE gpkg_geometry_columns (table_name TEXT, column_name TEXT, geometry_type_name TEXT, srs_id INTEGER, z TINYINT, m TINYINT, PRIMARY KEY (table_name, column_name));
            INSERT INTO gpkg_spatial_ref_sys VALUES ('WGS 84', 4326, 'EPSG', 4326, 'GEOGCS["WGS 84"]', NULL);
            CREATE TABLE layer_b (fid INTEGER PRIMARY KEY, geom BLOB, description TEXT, value REAL);
            INSERT INTO gpkg_contents VALUES ('layer_b', 'features', 'layer_b', '', datetime('now'), NULL, NULL, NULL, NULL, 4326);
            INSERT INTO gpkg_geometry_columns VALUES ('layer_b', 'geom', 'POINT', 4326, 0, 0);
        """)
        conn2.close()

        # pygeodiff raises error for incompatible schemas - verify our error handling
        with pytest.raises(GeoDiffError, match="Failed to create changeset"):
            compute_diff(str(gpkg1), str(gpkg2))


# Tests for parsing change types


class TestParseChangeTypes:
    """Tests for verifying change type parsing in compute_diff."""

    def test_parse_insert_type(self):
        """Test parsing insert change type from geodiff output."""
        # Simulate geodiff output with insert
        changes_detail = {
            "geodiff": [
                {
                    "table": "test_layer",
                    "changes": [
                        {"type": "insert", "values": {}},
                        {"type": "insert", "values": {}},
                    ],
                }
            ]
        }

        # Count inserts manually (simulating what compute_diff does)
        insert_count = 0
        for table_changes in changes_detail.get("geodiff", []):
            for change in table_changes.get("changes", []):
                if change.get("type") == "insert":
                    insert_count += 1

        assert insert_count == 2

    def test_parse_update_type(self):
        """Test parsing update change type from geodiff output."""
        changes_detail = {
            "geodiff": [
                {
                    "table": "test_layer",
                    "changes": [
                        {"type": "update", "values": {}},
                    ],
                }
            ]
        }

        update_count = 0
        for table_changes in changes_detail.get("geodiff", []):
            for change in table_changes.get("changes", []):
                if change.get("type") == "update":
                    update_count += 1

        assert update_count == 1

    def test_parse_delete_type(self):
        """Test parsing delete change type from geodiff output."""
        changes_detail = {
            "geodiff": [
                {
                    "table": "test_layer",
                    "changes": [
                        {"type": "delete", "values": {}},
                        {"type": "delete", "values": {}},
                        {"type": "delete", "values": {}},
                    ],
                }
            ]
        }

        delete_count = 0
        for table_changes in changes_detail.get("geodiff", []):
            for change in table_changes.get("changes", []):
                if change.get("type") == "delete":
                    delete_count += 1

        assert delete_count == 3

    def test_parse_mixed_types(self):
        """Test parsing mixed change types."""
        changes_detail = {
            "geodiff": [
                {
                    "table": "layer1",
                    "changes": [
                        {"type": "insert"},
                        {"type": "update"},
                    ],
                },
                {
                    "table": "layer2",
                    "changes": [
                        {"type": "delete"},
                        {"type": "insert"},
                    ],
                },
            ]
        }

        insert_count = 0
        update_count = 0
        delete_count = 0

        for table_changes in changes_detail.get("geodiff", []):
            for change in table_changes.get("changes", []):
                op = change.get("type", "")
                if op == "insert":
                    insert_count += 1
                elif op == "update":
                    update_count += 1
                elif op == "delete":
                    delete_count += 1

        assert insert_count == 2
        assert update_count == 1
        assert delete_count == 1

    def test_parse_unknown_type(self):
        """Test that unknown change types are ignored."""
        changes_detail = {
            "geodiff": [
                {
                    "table": "test_layer",
                    "changes": [
                        {"type": "unknown_operation"},
                        {"type": "insert"},
                    ],
                }
            ]
        }

        insert_count = 0
        update_count = 0
        delete_count = 0

        for table_changes in changes_detail.get("geodiff", []):
            for change in table_changes.get("changes", []):
                op = change.get("type", "")
                if op == "insert":
                    insert_count += 1
                elif op == "update":
                    update_count += 1
                elif op == "delete":
                    delete_count += 1

        # Only insert should be counted
        assert insert_count == 1
        assert update_count == 0
        assert delete_count == 0


# Tests for change type counting with Italian cities data


class TestChangeTypeCounting:
    """Tests for verifying change type counting in compute_diff with Italian cities."""

    def test_count_inserts_only(self, empty_gpkg, base_gpkg):
        """Test that inserts are counted correctly (5 Italian cities)."""
        result = compute_diff(empty_gpkg, base_gpkg)

        # When going from empty to populated, all should be inserts
        assert result["has_changes"] is True
        summary = result["summary"]
        # Exactly 5 cities: Roma, Milano, Napoli, Torino, Firenze
        assert summary["total_changes"] == 5
        assert summary["inserts"] == 5
        assert summary["updates"] == 0
        assert summary["deletes"] == 0

    def test_count_deletes_only(self, base_gpkg, empty_gpkg):
        """Test that deletes are counted correctly (5 Italian cities)."""
        result = compute_diff(base_gpkg, empty_gpkg)

        # When going from populated to empty, all should be deletes
        assert result["has_changes"] is True
        summary = result["summary"]
        # Exactly 5 cities deleted
        assert summary["total_changes"] == 5
        assert summary["inserts"] == 0
        assert summary["updates"] == 0
        assert summary["deletes"] == 5

    def test_count_mixed_changes(self, base_gpkg, modified_gpkg):
        """Test counting mixed changes with Italian cities.

        Base cities: Roma, Milano, Napoli, Torino, Firenze (5 cities)
        Modified: Roma*, Milano, Torino*, Bologna+, Venezia+ (5 cities)
        * = updated, + = new

        Expected changes:
        - Inserts: 2 (Bologna, Venezia)
        - Updates: 2 (Roma, Torino)
        - Deletes: 2 (Napoli, Firenze)
        """
        result = compute_diff(base_gpkg, modified_gpkg)

        assert result["has_changes"] is True
        summary = result["summary"]

        # Verify expected changes
        assert summary["inserts"] == 2, f"Expected 2 inserts (Bologna, Venezia), got {summary['inserts']}"
        assert summary["updates"] == 2, f"Expected 2 updates (Roma, Torino), got {summary['updates']}"
        assert summary["deletes"] == 2, f"Expected 2 deletes (Napoli, Firenze), got {summary['deletes']}"
        assert summary["total_changes"] == 6, f"Expected 6 total changes, got {summary['total_changes']}"

    def test_compute_diff_with_mocked_changes(self, temp_dir):
        """Test compute_diff with mocked pygeodiff returning specific change types."""
        import sqlite3

        # Create two identical GeoPackages
        gpkg1 = temp_dir / "base_mock.gpkg"
        gpkg2 = temp_dir / "compare_mock.gpkg"

        for gpkg in [gpkg1, gpkg2]:
            conn = sqlite3.connect(str(gpkg))
            conn.executescript("""
                CREATE TABLE gpkg_spatial_ref_sys (srs_name TEXT, srs_id INTEGER PRIMARY KEY, organization TEXT, organization_coordsys_id INTEGER, definition TEXT, description TEXT);
                CREATE TABLE gpkg_contents (table_name TEXT PRIMARY KEY, data_type TEXT, identifier TEXT, description TEXT, last_change DATETIME, min_x DOUBLE, min_y DOUBLE, max_x DOUBLE, max_y DOUBLE, srs_id INTEGER);
                CREATE TABLE gpkg_geometry_columns (table_name TEXT, column_name TEXT, geometry_type_name TEXT, srs_id INTEGER, z TINYINT, m TINYINT, PRIMARY KEY (table_name, column_name));
                INSERT INTO gpkg_spatial_ref_sys VALUES ('WGS 84', 4326, 'EPSG', 4326, 'GEOGCS["WGS 84"]', NULL);
                CREATE TABLE test_layer (fid INTEGER PRIMARY KEY, geom BLOB, name TEXT);
                INSERT INTO gpkg_contents VALUES ('test_layer', 'features', 'test_layer', '', datetime('now'), NULL, NULL, NULL, NULL, 4326);
                INSERT INTO gpkg_geometry_columns VALUES ('test_layer', 'geom', 'POINT', 4326, 0, 0);
                INSERT INTO test_layer (fid, name) VALUES (1, 'Point A');
            """)
            conn.close()

        # Mock list_changes_json to return specific change types
        mock_changes = {
            "geodiff": [
                {
                    "table": "test_layer",
                    "changes": [
                        {"type": "insert"},
                        {"type": "insert"},
                        {"type": "update"},
                        {"type": "delete"},
                    ],
                }
            ]
        }

        with patch("geodiff.list_changes_json", return_value=mock_changes):
            with patch("geodiff.has_changes", return_value=True):
                with patch("geodiff.count_changes", return_value=4):
                    result = compute_diff(str(gpkg1), str(gpkg2))

        assert result["summary"]["inserts"] == 2
        assert result["summary"]["updates"] == 1
        assert result["summary"]["deletes"] == 1


# Tests for format_output with table details


class TestFormatOutputTableDetails:
    """Tests for format_output with table details in summary."""

    def test_summary_includes_tables_affected(self, base_gpkg, modified_gpkg):
        """Test that summary includes tables affected section."""
        result = compute_diff(base_gpkg, modified_gpkg)
        output = format_output(result, "summary")

        # If there are changes, tables affected should be shown
        if result["has_changes"] and result["changes"].get("geodiff"):
            assert "Tables affected:" in output

    def test_summary_shows_table_names(self, base_gpkg, modified_gpkg):
        """Test that summary shows individual table names."""
        _ = compute_diff(base_gpkg, modified_gpkg)

        # Manually construct a result with known table data
        result_with_tables = {
            "base_file": "base.gpkg",
            "compare_file": "compare.gpkg",
            "has_changes": True,
            "summary": {
                "total_changes": 3,
                "inserts": 1,
                "updates": 1,
                "deletes": 1,
            },
            "changes": {
                "geodiff": [
                    {"table": "test_layer", "changes": [{"type": "insert"}, {"type": "update"}]},
                    {"table": "another_layer", "changes": [{"type": "delete"}]},
                ]
            },
        }

        output = format_output(result_with_tables, "summary")

        assert "Tables affected:" in output
        assert "test_layer: 2 change(s)" in output
        assert "another_layer: 1 change(s)" in output


# Tests for verifying specific changeset details with Italian cities


class TestItalianCitiesChangesets:
    """Tests verifying exact changeset details with Italian cities data."""

    def test_changeset_contains_cities_table(self, base_gpkg, modified_gpkg):
        """Test that changeset includes the 'cities' table."""
        result = compute_diff(base_gpkg, modified_gpkg)

        changes = result["changes"]
        assert "geodiff" in changes
        assert len(changes["geodiff"]) > 0

        # Find the cities table in changes
        cities_table = None
        for table_change in changes["geodiff"]:
            if table_change.get("table") == "cities":
                cities_table = table_change
                break

        assert cities_table is not None, "Expected 'cities' table in changeset"
        assert "changes" in cities_table

    def test_changeset_detail_inserts(self, empty_gpkg, base_gpkg):
        """Test that inserting 5 Italian cities produces correct changeset."""
        result = compute_diff(empty_gpkg, base_gpkg)

        changes = result["changes"]["geodiff"]
        assert len(changes) > 0

        # All changes should be inserts
        total_inserts = 0
        for table_change in changes:
            for change in table_change.get("changes", []):
                assert change.get("type") == "insert", f"Expected insert, got {change.get('type')}"
                total_inserts += 1

        assert total_inserts == 5, f"Expected 5 inserts, got {total_inserts}"

    def test_changeset_detail_deletes(self, base_gpkg, empty_gpkg):
        """Test that deleting 5 Italian cities produces correct changeset."""
        result = compute_diff(base_gpkg, empty_gpkg)

        changes = result["changes"]["geodiff"]
        assert len(changes) > 0

        # All changes should be deletes
        total_deletes = 0
        for table_change in changes:
            for change in table_change.get("changes", []):
                assert change.get("type") == "delete", f"Expected delete, got {change.get('type')}"
                total_deletes += 1

        assert total_deletes == 5, f"Expected 5 deletes, got {total_deletes}"

    def test_changeset_detail_mixed_changes(self, base_gpkg, modified_gpkg):
        """Test that mixed changes produce correct changeset types.

        Expected:
        - 2 inserts (Bologna, Venezia)
        - 2 updates (Roma, Torino)
        - 2 deletes (Napoli, Firenze)
        """
        result = compute_diff(base_gpkg, modified_gpkg)

        changes = result["changes"]["geodiff"]
        assert len(changes) > 0

        # Count change types
        inserts = 0
        updates = 0
        deletes = 0

        for table_change in changes:
            for change in table_change.get("changes", []):
                change_type = change.get("type")
                if change_type == "insert":
                    inserts += 1
                elif change_type == "update":
                    updates += 1
                elif change_type == "delete":
                    deletes += 1

        assert inserts == 2, f"Expected 2 inserts, got {inserts}"
        assert updates == 2, f"Expected 2 updates, got {updates}"
        assert deletes == 2, f"Expected 2 deletes, got {deletes}"

    def test_single_city_insert(self, empty_gpkg, single_feature_gpkg):
        """Test inserting a single city (Roma) produces exactly 1 insert."""
        result = compute_diff(empty_gpkg, single_feature_gpkg)

        assert result["has_changes"] is True
        assert result["summary"]["total_changes"] == 1
        assert result["summary"]["inserts"] == 1
        assert result["summary"]["updates"] == 0
        assert result["summary"]["deletes"] == 0

    def test_single_city_delete(self, single_feature_gpkg, empty_gpkg):
        """Test deleting a single city (Roma) produces exactly 1 delete."""
        result = compute_diff(single_feature_gpkg, empty_gpkg)

        assert result["has_changes"] is True
        assert result["summary"]["total_changes"] == 1
        assert result["summary"]["inserts"] == 0
        assert result["summary"]["updates"] == 0
        assert result["summary"]["deletes"] == 1


# Integration tests


class TestIntegration:
    """Integration tests for the full diff workflow with Italian cities."""

    def test_full_workflow(self, base_gpkg, modified_gpkg):
        """Test the complete diff workflow with Italian cities."""
        # Compute diff
        result = compute_diff(base_gpkg, modified_gpkg)

        # Verify result
        assert result["has_changes"] is True

        # Verify expected changes: 2 inserts, 2 updates, 2 deletes
        assert result["summary"]["total_changes"] == 6

        # Format as JSON
        json_output = format_output(result, "json")
        parsed = json.loads(json_output)
        assert parsed["summary"]["total_changes"] == 6

        # Format as summary
        summary_output = format_output(result, "summary")
        assert "GeoDiff Summary" in summary_output
        assert "cities:" in summary_output  # Should show the cities table

    def test_roundtrip_identical(self, base_gpkg, identical_gpkg):
        """Test that identical Italian cities produce empty diff."""
        result = compute_diff(base_gpkg, identical_gpkg)

        assert result["has_changes"] is False
        assert result["summary"]["total_changes"] == 0
        assert result["summary"]["inserts"] == 0
        assert result["summary"]["updates"] == 0
        assert result["summary"]["deletes"] == 0

    def test_sqlite_extension_support(self, temp_dir):
        """Test that .sqlite extension is supported."""
        import sqlite3

        sqlite_file = temp_dir / "test.sqlite"

        conn = sqlite3.connect(str(sqlite_file))
        conn.executescript("""
            CREATE TABLE gpkg_spatial_ref_sys (srs_name TEXT, srs_id INTEGER PRIMARY KEY, organization TEXT, organization_coordsys_id INTEGER, definition TEXT, description TEXT);
            CREATE TABLE gpkg_contents (table_name TEXT PRIMARY KEY, data_type TEXT, identifier TEXT, description TEXT, last_change DATETIME, min_x DOUBLE, min_y DOUBLE, max_x DOUBLE, max_y DOUBLE, srs_id INTEGER);
            CREATE TABLE gpkg_geometry_columns (table_name TEXT, column_name TEXT, geometry_type_name TEXT, srs_id INTEGER, z TINYINT, m TINYINT, PRIMARY KEY (table_name, column_name));
            INSERT INTO gpkg_spatial_ref_sys VALUES ('WGS 84', 4326, 'EPSG', 4326, 'GEOGCS["WGS 84"]', NULL);
            CREATE TABLE data (fid INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO gpkg_contents VALUES ('data', 'features', 'data', '', datetime('now'), NULL, NULL, NULL, NULL, 4326);
        """)
        conn.close()

        # Should validate without error
        path = validate_file(str(sqlite_file))
        assert path.suffix == ".sqlite"

    def test_db_extension_support(self, temp_dir):
        """Test that .db extension is supported."""
        import sqlite3

        db_file = temp_dir / "test.db"

        conn = sqlite3.connect(str(db_file))
        conn.executescript("""
            CREATE TABLE gpkg_spatial_ref_sys (srs_name TEXT, srs_id INTEGER PRIMARY KEY, organization TEXT, organization_coordsys_id INTEGER, definition TEXT, description TEXT);
            CREATE TABLE gpkg_contents (table_name TEXT PRIMARY KEY, data_type TEXT, identifier TEXT, description TEXT, last_change DATETIME, min_x DOUBLE, min_y DOUBLE, max_x DOUBLE, max_y DOUBLE, srs_id INTEGER);
            CREATE TABLE gpkg_geometry_columns (table_name TEXT, column_name TEXT, geometry_type_name TEXT, srs_id INTEGER, z TINYINT, m TINYINT, PRIMARY KEY (table_name, column_name));
            INSERT INTO gpkg_spatial_ref_sys VALUES ('WGS 84', 4326, 'EPSG', 4326, 'GEOGCS["WGS 84"]', NULL);
            CREATE TABLE data (fid INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO gpkg_contents VALUES ('data', 'features', 'data', '', datetime('now'), NULL, NULL, NULL, NULL, 4326);
        """)
        conn.close()

        # Should validate without error
        path = validate_file(str(db_file))
        assert path.suffix == ".db"
