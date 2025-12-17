"""Pytest configuration and fixtures for geodiff tests."""

import shutil
import sqlite3
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    tmpdir = tempfile.mkdtemp()
    yield Path(tmpdir)
    # Cleanup
    shutil.rmtree(tmpdir, ignore_errors=True)


def create_geopackage(filepath: str, table_name: str = "test_layer", features: list[dict] | None = None) -> str:
    """
    Create a minimal GeoPackage file with a point layer.

    Args:
        filepath: Path where to create the GeoPackage.
        table_name: Name of the layer/table.
        features: List of feature dicts with 'id', 'name', 'x', 'y' keys.

    Returns:
        Path to the created GeoPackage.
    """
    if features is None:
        features = []

    conn = sqlite3.connect(filepath)
    cursor = conn.cursor()

    # Create GeoPackage metadata tables
    cursor.executescript("""
        -- GeoPackage required tables
        CREATE TABLE IF NOT EXISTS gpkg_spatial_ref_sys (
            srs_name TEXT NOT NULL,
            srs_id INTEGER NOT NULL PRIMARY KEY,
            organization TEXT NOT NULL,
            organization_coordsys_id INTEGER NOT NULL,
            definition TEXT NOT NULL,
            description TEXT
        );

        CREATE TABLE IF NOT EXISTS gpkg_contents (
            table_name TEXT NOT NULL PRIMARY KEY,
            data_type TEXT NOT NULL,
            identifier TEXT UNIQUE,
            description TEXT DEFAULT '',
            last_change DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            min_x DOUBLE,
            min_y DOUBLE,
            max_x DOUBLE,
            max_y DOUBLE,
            srs_id INTEGER,
            CONSTRAINT fk_gc_r_srs_id FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys(srs_id)
        );

        CREATE TABLE IF NOT EXISTS gpkg_geometry_columns (
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            geometry_type_name TEXT NOT NULL,
            srs_id INTEGER NOT NULL,
            z TINYINT NOT NULL,
            m TINYINT NOT NULL,
            CONSTRAINT pk_geom_cols PRIMARY KEY (table_name, column_name),
            CONSTRAINT fk_gc_tn FOREIGN KEY (table_name) REFERENCES gpkg_contents(table_name),
            CONSTRAINT fk_gc_srs FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys(srs_id)
        );

        -- Insert WGS84 spatial reference system
        INSERT OR IGNORE INTO gpkg_spatial_ref_sys (srs_name, srs_id, organization, organization_coordsys_id, definition)
        VALUES ('WGS 84', 4326, 'EPSG', 4326,
            'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]');
    """)

    # Create the feature table
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            fid INTEGER PRIMARY KEY AUTOINCREMENT,
            geom BLOB,
            name TEXT
        )
    """)

    # Register in gpkg_contents
    cursor.execute(f"""
        INSERT OR REPLACE INTO gpkg_contents (table_name, data_type, identifier, srs_id)
        VALUES ('{table_name}', 'features', '{table_name}', 4326)
    """)

    # Register in gpkg_geometry_columns
    cursor.execute(f"""
        INSERT OR REPLACE INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m)
        VALUES ('{table_name}', 'geom', 'POINT', 4326, 0, 0)
    """)

    # Insert features
    for feature in features:
        fid = feature.get("id")
        name = feature.get("name", "")
        x = feature.get("x", 0)
        y = feature.get("y", 0)

        # Create a simple WKB point geometry (little-endian)
        # WKB format: byte order (1) + type (1=point, 4 bytes) + x (8 bytes) + y (8 bytes)
        import struct

        wkb = struct.pack("<bI", 1, 1)  # Little-endian, Point type
        wkb += struct.pack("<dd", x, y)  # x, y coordinates

        # GeoPackage uses GP header + WKB
        # GP header: 'GP' (2 bytes) + version (1 byte) + flags (1 byte) + srs_id (4 bytes) + envelope (optional)
        gp_header = b"GP"  # Magic
        gp_header += struct.pack("<bb", 0, 1)  # Version 0, flags (little-endian WKB, no envelope)
        gp_header += struct.pack("<i", 4326)  # SRS ID

        gpkg_geom = gp_header + wkb

        if fid is not None:
            cursor.execute(
                f"INSERT INTO {table_name} (fid, geom, name) VALUES (?, ?, ?)",
                (fid, gpkg_geom, name),
            )
        else:
            cursor.execute(
                f"INSERT INTO {table_name} (geom, name) VALUES (?, ?)",
                (gpkg_geom, name),
            )

    conn.commit()
    conn.close()

    return filepath


@pytest.fixture
def base_gpkg(temp_dir):
    """Create a base GeoPackage with sample features."""
    filepath = temp_dir / "base.gpkg"
    features = [
        {"id": 1, "name": "Point A", "x": 0.0, "y": 0.0},
        {"id": 2, "name": "Point B", "x": 1.0, "y": 1.0},
        {"id": 3, "name": "Point C", "x": 2.0, "y": 2.0},
    ]
    return create_geopackage(str(filepath), features=features)


@pytest.fixture
def identical_gpkg(temp_dir):
    """Create a GeoPackage identical to base."""
    filepath = temp_dir / "identical.gpkg"
    features = [
        {"id": 1, "name": "Point A", "x": 0.0, "y": 0.0},
        {"id": 2, "name": "Point B", "x": 1.0, "y": 1.0},
        {"id": 3, "name": "Point C", "x": 2.0, "y": 2.0},
    ]
    return create_geopackage(str(filepath), features=features)


@pytest.fixture
def modified_gpkg(temp_dir):
    """Create a GeoPackage with modifications compared to base."""
    filepath = temp_dir / "modified.gpkg"
    features = [
        {"id": 1, "name": "Point A Modified", "x": 0.0, "y": 0.0},  # Updated
        {"id": 2, "name": "Point B", "x": 1.0, "y": 1.0},  # Unchanged
        # Point C (id=3) removed
        {"id": 4, "name": "Point D", "x": 3.0, "y": 3.0},  # Added
    ]
    return create_geopackage(str(filepath), features=features)


@pytest.fixture
def empty_gpkg(temp_dir):
    """Create an empty GeoPackage (schema only, no features)."""
    filepath = temp_dir / "empty.gpkg"
    return create_geopackage(str(filepath), features=[])
