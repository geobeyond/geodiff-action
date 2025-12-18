"""Git utility functions for extracting files from commit history."""

import subprocess
import tempfile
from pathlib import Path


class GitError(Exception):
    """Custom exception for Git-related errors."""

    pass


def is_git_repo(path: str) -> bool:
    """
    Check if a path is inside a git repository.

    Args:
        path: Directory path to check.

    Returns:
        True if the path is in a git repository, False otherwise.
    """
    try:
        result = subprocess.run(
            ["git", "-C", path, "rev-parse", "--git-dir"],
            capture_output=True,
            text=True,
            check=False,
        )
        return result.returncode == 0
    except Exception:
        return False


def find_repo_root(file_path: str) -> str | None:
    """
    Find the git repository root that contains the given file.

    Args:
        file_path: Path to a file (can be relative or absolute).

    Returns:
        Absolute path to the repository root, or None if not in a git repo.
    """
    # Resolve to absolute path and get the directory containing the file
    abs_path = Path(file_path).resolve()
    if abs_path.is_file():
        search_dir = abs_path.parent
    else:
        search_dir = abs_path

    try:
        result = subprocess.run(
            ["git", "-C", str(search_dir), "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return None
    except Exception:
        return None


def get_previous_commit(repo_path: str, offset: int = 1) -> str:
    """
    Get the commit hash of a previous commit.

    Args:
        repo_path: Path to the git repository.
        offset: Number of commits to go back (1 = HEAD~1, 2 = HEAD~2, etc.)

    Returns:
        The full commit hash.

    Raises:
        GitError: If not a git repository or no previous commit exists.
    """
    if not is_git_repo(repo_path):
        raise GitError(f"Not a git repository: {repo_path}")

    try:
        result = subprocess.run(
            ["git", "-C", repo_path, "rev-parse", f"HEAD~{offset}"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        raise GitError(f"No previous commit found at HEAD~{offset}") from e


def has_file_in_commit(repo_path: str, file_path: str, commit: str) -> bool:
    """
    Check if a file exists in a specific commit.

    Args:
        repo_path: Path to the git repository.
        file_path: Relative path to the file within the repository.
        commit: Commit hash or reference (e.g., "HEAD", "HEAD~1").

    Returns:
        True if the file exists in the commit, False otherwise.
    """
    try:
        result = subprocess.run(
            ["git", "-C", repo_path, "cat-file", "-e", f"{commit}:{file_path}"],
            capture_output=True,
            check=False,
        )
        return result.returncode == 0
    except Exception:
        return False


def get_file_from_commit(repo_path: str, file_path: str, commit: str) -> str:
    """
    Extract a file from a specific commit to a temporary location.

    Args:
        repo_path: Path to the git repository.
        file_path: Relative path to the file within the repository.
        commit: Commit hash or reference (e.g., "HEAD", "HEAD~1").

    Returns:
        Path to the extracted temporary file. Caller is responsible for cleanup.

    Raises:
        GitError: If the file doesn't exist in the commit or extraction fails.
    """
    if not has_file_in_commit(repo_path, file_path, commit):
        raise GitError(f"File not found in commit {commit}: {file_path}")

    # Preserve the original file extension
    original_path = Path(file_path)
    suffix = original_path.suffix

    # Create a temporary file with the same extension
    temp_fd, temp_path = tempfile.mkstemp(suffix=suffix)

    try:
        result = subprocess.run(
            ["git", "-C", repo_path, "show", f"{commit}:{file_path}"],
            capture_output=True,
            check=True,
        )

        # Write the content to the temp file
        with open(temp_fd, "wb") as f:
            f.write(result.stdout)

        return temp_path

    except subprocess.CalledProcessError as e:
        # Clean up temp file on error
        Path(temp_path).unlink(missing_ok=True)
        raise GitError(f"Failed to extract file from commit: {e.stderr.decode()}") from e
