"""Tests for CLI commands using Click's test runner."""

import json
import os
import tempfile

import pytest
from click.testing import CliRunner

from beads.cli import cli


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def beads_dir():
    """Create a temporary directory with beads initialized."""
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        r = CliRunner()
        result = r.invoke(cli, ["init", "--prefix", "test"])
        assert result.exit_code == 0, result.output
        yield tmpdir


class TestInit:
    def test_init(self, runner: CliRunner):
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ["init", "--prefix", "myproj"])
            assert result.exit_code == 0
            assert "Initialized beads" in result.output
            assert os.path.exists(".beads/config.yaml")
            assert os.path.exists(".beads/metadata.json")
            assert os.path.exists(".beads/beads.db")


class TestCreate:
    def test_create_basic(self, runner: CliRunner, beads_dir: str):
        result = runner.invoke(cli, [
            "create", "--title", "Test Issue", "--type", "task", "--priority", "2"
        ])
        assert result.exit_code == 0
        assert "Created task test-" in result.output

    def test_create_silent(self, runner: CliRunner, beads_dir: str):
        result = runner.invoke(cli, [
            "create", "--title", "Silent", "--silent"
        ])
        assert result.exit_code == 0
        assert result.output.strip().startswith("test-")

    def test_create_with_labels(self, runner: CliRunner, beads_dir: str):
        result = runner.invoke(cli, [
            "create", "--title", "Labeled", "-l", "urgent", "-l", "backend"
        ])
        assert result.exit_code == 0
        # Verify via show
        issue_id = result.output.split()[-1].rstrip(":")
        # Extract ID from "Created task test-xxx: Labeled"
        parts = result.output.strip().split()
        issue_id = parts[2].rstrip(":")


class TestList:
    def test_list_empty(self, runner: CliRunner, beads_dir: str):
        result = runner.invoke(cli, ["list"])
        assert result.exit_code == 0
        assert "No issues found" in result.output

    def test_list_with_issues(self, runner: CliRunner, beads_dir: str):
        runner.invoke(cli, ["create", "--title", "Issue 1"])
        runner.invoke(cli, ["create", "--title", "Issue 2"])
        result = runner.invoke(cli, ["list"])
        assert result.exit_code == 0
        assert "2 issue(s)" in result.output


class TestClose:
    def test_close(self, runner: CliRunner, beads_dir: str):
        result = runner.invoke(cli, ["create", "--title", "To Close", "--silent"])
        issue_id = result.output.strip()
        result = runner.invoke(cli, ["close", issue_id, "--reason", "Done"])
        assert result.exit_code == 0
        assert "Closed" in result.output

    def test_close_batch(self, runner: CliRunner, beads_dir: str):
        r1 = runner.invoke(cli, ["create", "--title", "Close 1", "--silent"])
        r2 = runner.invoke(cli, ["create", "--title", "Close 2", "--silent"])
        id1 = r1.output.strip()
        id2 = r2.output.strip()
        result = runner.invoke(cli, ["close", id1, id2])
        assert result.exit_code == 0


class TestReady:
    def test_ready_shows_unblocked(self, runner: CliRunner, beads_dir: str):
        runner.invoke(cli, ["create", "--title", "Ready One"])
        result = runner.invoke(cli, ["ready"])
        assert result.exit_code == 0
        assert "1 ready issue(s)" in result.output


class TestSearch:
    def test_search(self, runner: CliRunner, beads_dir: str):
        runner.invoke(cli, ["create", "--title", "Unique Searchable Title"])
        result = runner.invoke(cli, ["search", "Searchable"])
        assert result.exit_code == 0
        assert "Unique Searchable Title" in result.output


class TestStats:
    def test_stats(self, runner: CliRunner, beads_dir: str):
        runner.invoke(cli, ["create", "--title", "Issue"])
        result = runner.invoke(cli, ["stats"])
        assert result.exit_code == 0
        assert "Total:" in result.output


class TestSync:
    def test_sync_flush_only(self, runner: CliRunner, beads_dir: str):
        runner.invoke(cli, ["create", "--title", "Sync Test"])
        result = runner.invoke(cli, ["sync", "--flush-only"])
        assert result.exit_code == 0
        assert "Flushed" in result.output
        assert os.path.exists(os.path.join(beads_dir, ".beads", "issues.jsonl"))


class TestDoctor:
    def test_doctor(self, runner: CliRunner, beads_dir: str):
        result = runner.invoke(cli, ["doctor"])
        assert result.exit_code == 0
        assert "Beads Doctor" in result.output
