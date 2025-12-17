"""Tests for src/functions.py"""

import subprocess  # noqa: F401

import pytest

import sys

sys.path.insert(0, "src")

from functions import check_output


class TestCheckOutput:
    """Tests for the check_output function."""

    def test_check_output_returns_string(self):
        """Test that check_output returns a string."""
        result = check_output("echo hello")
        assert isinstance(result, str)

    def test_check_output_captures_stdout(self):
        """Test that check_output captures command stdout."""
        result = check_output("echo hello")
        assert result == "hello"

    def test_check_output_with_list_command(self):
        """Test that check_output works with list commands."""
        result = check_output(["echo", "hello"])
        assert result == "hello"

    def test_check_output_silent_mode(self, capsys):
        """Test that silent mode suppresses output."""
        check_output("echo hello", silent=True)
        captured = capsys.readouterr()
        assert captured.out == ""

    def test_check_output_non_silent_mode(self, capsys):
        """Test that non-silent mode prints output."""
        check_output("echo hello", silent=False)
        captured = capsys.readouterr()
        assert "hello" in captured.out

    def test_check_output_invalid_command_returns_empty(self):
        """Test that invalid command returns empty string when throw=False."""
        result = check_output("nonexistent_command_xyz", throw=False)
        assert result == ""

    def test_check_output_invalid_command_throws(self):
        """Test that invalid command raises exception when throw=True."""
        with pytest.raises((subprocess.CalledProcessError, FileNotFoundError)):
            check_output("nonexistent_command_xyz", throw=True)

    def test_check_output_strips_whitespace(self):
        """Test that output is stripped of leading/trailing whitespace."""
        result = check_output(["echo", "  hello  "])
        assert result == "hello"

    def test_check_output_multiword_command(self):
        """Test command with multiple words/arguments."""
        result = check_output("echo one two three")
        assert result == "one two three"

    def test_check_output_empty_output(self):
        """Test command that produces no output."""
        result = check_output("true")
        assert result == ""
