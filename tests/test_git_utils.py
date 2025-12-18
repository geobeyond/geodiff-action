"""Tests for git utility functions."""

import os
from pathlib import Path

import pytest

import sys

sys.path.insert(0, "src")

from git_utils import (
    GitError,
    find_repo_root,
    get_file_from_commit,
    get_previous_commit,
    has_file_in_commit,
    is_git_repo,
)


@pytest.fixture
def git_repo(tmp_path):
    """Create a temporary git repository with some commits."""
    repo_dir = tmp_path / "test_repo"
    repo_dir.mkdir()

    # Initialize git repo
    os.system(f"cd {repo_dir} && git init")
    os.system(f"cd {repo_dir} && git config user.email 'test@test.com'")
    os.system(f"cd {repo_dir} && git config user.name 'Test User'")

    # Create initial file and commit
    test_file = repo_dir / "test.gpkg"
    test_file.write_text("initial content")
    os.system(f"cd {repo_dir} && git add test.gpkg && git commit -m 'Initial commit'")

    # Modify file and commit
    test_file.write_text("modified content")
    os.system(f"cd {repo_dir} && git add test.gpkg && git commit -m 'Second commit'")

    return repo_dir


@pytest.fixture
def non_git_dir(tmp_path):
    """Create a temporary directory that is not a git repository."""
    non_git = tmp_path / "non_git"
    non_git.mkdir()
    return non_git


class TestIsGitRepo:
    """Tests for is_git_repo function."""

    def test_is_git_repo_true(self, git_repo):
        """Test that a git repository is detected."""
        assert is_git_repo(str(git_repo)) is True

    def test_is_git_repo_false(self, non_git_dir):
        """Test that a non-git directory returns False."""
        assert is_git_repo(str(non_git_dir)) is False

    def test_is_git_repo_nonexistent(self):
        """Test that nonexistent directory returns False."""
        assert is_git_repo("/nonexistent/path") is False


class TestFindRepoRoot:
    """Tests for find_repo_root function."""

    def test_find_repo_root_from_file(self, git_repo):
        """Test finding repo root from a file path."""
        test_file = git_repo / "test.gpkg"
        repo_root = find_repo_root(str(test_file))
        assert repo_root == str(git_repo)

    def test_find_repo_root_from_subdirectory_file(self, git_repo):
        """Test finding repo root from a file in a subdirectory."""
        subdir = git_repo / "data" / "subdir"
        subdir.mkdir(parents=True)
        test_file = subdir / "nested.gpkg"
        test_file.write_text("content")

        repo_root = find_repo_root(str(test_file))
        assert repo_root == str(git_repo)

    def test_find_repo_root_from_directory(self, git_repo):
        """Test finding repo root from a directory path."""
        subdir = git_repo / "data"
        subdir.mkdir(exist_ok=True)

        repo_root = find_repo_root(str(subdir))
        assert repo_root == str(git_repo)

    def test_find_repo_root_non_git(self, non_git_dir):
        """Test that non-git directory returns None."""
        test_file = non_git_dir / "test.txt"
        test_file.write_text("content")

        repo_root = find_repo_root(str(test_file))
        assert repo_root is None

    def test_find_repo_root_nonexistent(self):
        """Test that nonexistent path returns None."""
        repo_root = find_repo_root("/nonexistent/path/file.gpkg")
        assert repo_root is None


class TestGetPreviousCommit:
    """Tests for get_previous_commit function."""

    def test_get_previous_commit(self, git_repo):
        """Test getting the previous commit hash."""
        prev_commit = get_previous_commit(str(git_repo))
        assert prev_commit is not None
        assert len(prev_commit) == 40  # Full SHA hash

    def test_get_previous_commit_with_offset(self, git_repo):
        """Test getting commit with offset."""
        # HEAD~1 should be the first commit
        prev_commit = get_previous_commit(str(git_repo), offset=1)
        assert prev_commit is not None

    def test_get_previous_commit_non_git(self, non_git_dir):
        """Test that non-git directory raises error."""
        with pytest.raises(GitError, match="Not a git repository"):
            get_previous_commit(str(non_git_dir))


class TestHasFileInCommit:
    """Tests for has_file_in_commit function."""

    def test_has_file_in_commit_exists(self, git_repo):
        """Test that existing file is found in commit."""
        prev_commit = get_previous_commit(str(git_repo))
        assert has_file_in_commit(str(git_repo), "test.gpkg", prev_commit) is True

    def test_has_file_in_commit_not_exists(self, git_repo):
        """Test that non-existing file returns False."""
        prev_commit = get_previous_commit(str(git_repo))
        assert has_file_in_commit(str(git_repo), "nonexistent.gpkg", prev_commit) is False

    def test_has_file_in_commit_head(self, git_repo):
        """Test checking file in HEAD commit."""
        assert has_file_in_commit(str(git_repo), "test.gpkg", "HEAD") is True


class TestGetFileFromCommit:
    """Tests for get_file_from_commit function."""

    def test_get_file_from_commit(self, git_repo):
        """Test extracting a file from a previous commit."""
        prev_commit = get_previous_commit(str(git_repo))
        extracted_path = get_file_from_commit(str(git_repo), "test.gpkg", prev_commit)

        assert extracted_path is not None
        assert Path(extracted_path).exists()

        # Content should be from the previous commit
        content = Path(extracted_path).read_text()
        assert content == "initial content"

        # Cleanup
        Path(extracted_path).unlink()

    def test_get_file_from_commit_head(self, git_repo):
        """Test extracting file from HEAD."""
        extracted_path = get_file_from_commit(str(git_repo), "test.gpkg", "HEAD")

        assert extracted_path is not None
        content = Path(extracted_path).read_text()
        assert content == "modified content"

        # Cleanup
        Path(extracted_path).unlink()

    def test_get_file_from_commit_nonexistent(self, git_repo):
        """Test that extracting nonexistent file raises error."""
        with pytest.raises(GitError, match="File not found in commit"):
            get_file_from_commit(str(git_repo), "nonexistent.gpkg", "HEAD")

    def test_get_file_from_commit_preserves_extension(self, git_repo):
        """Test that extracted file preserves the original extension."""
        prev_commit = get_previous_commit(str(git_repo))
        extracted_path = get_file_from_commit(str(git_repo), "test.gpkg", prev_commit)

        assert extracted_path.endswith(".gpkg")

        # Cleanup
        Path(extracted_path).unlink()


class TestGitErrorHandling:
    """Tests for error handling in git utils."""

    def test_get_file_invalid_commit(self, git_repo):
        """Test that invalid commit hash raises error."""
        with pytest.raises(GitError):
            get_file_from_commit(str(git_repo), "test.gpkg", "invalidhash123")

    def test_get_previous_commit_no_history(self, tmp_path):
        """Test getting previous commit when there's no history."""
        # Create a repo with only one commit
        repo_dir = tmp_path / "single_commit_repo"
        repo_dir.mkdir()
        os.system(f"cd {repo_dir} && git init")
        os.system(f"cd {repo_dir} && git config user.email 'test@test.com'")
        os.system(f"cd {repo_dir} && git config user.name 'Test User'")

        test_file = repo_dir / "test.txt"
        test_file.write_text("content")
        os.system(f"cd {repo_dir} && git add test.txt && git commit -m 'Only commit'")

        # Trying to get HEAD~1 when there's only one commit should fail
        with pytest.raises(GitError, match="No previous commit"):
            get_previous_commit(str(repo_dir), offset=1)
