"""Configuration management for beads.

Handles:
- .beads/config.yaml parsing (user-facing config)
- .beads/metadata.json parsing (internal metadata config)
- Environment variable overrides
- .beads/ directory discovery
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


CONFIG_YAML = "config.yaml"
METADATA_JSON = "metadata.json"
BEADS_DIR = ".beads"
DEFAULT_DB_NAME = "beads.db"
DEFAULT_JSONL_NAME = "issues.jsonl"


@dataclass
class BeadsConfig:
    """User-facing config from config.yaml."""
    issue_prefix: str = ""
    no_db: bool = False
    no_daemon: bool = False
    no_auto_flush: bool = False
    no_auto_import: bool = False
    json_output: bool = False
    actor: str = ""
    db: str = ""
    sync_branch: str = ""
    flush_debounce: str = "5s"

    @classmethod
    def load(cls, beads_dir: str) -> BeadsConfig:
        """Load config.yaml from beads directory."""
        config_path = os.path.join(beads_dir, CONFIG_YAML)
        cfg = cls()
        if os.path.exists(config_path):
            with open(config_path) as f:
                data = yaml.safe_load(f) or {}
            cfg.issue_prefix = data.get("issue-prefix", "")
            cfg.no_db = data.get("no-db", False)
            cfg.no_daemon = data.get("no-daemon", False)
            cfg.no_auto_flush = data.get("no-auto-flush", False)
            cfg.no_auto_import = data.get("no-auto-import", False)
            cfg.json_output = data.get("json", False)
            cfg.actor = data.get("actor", "")
            cfg.db = data.get("db", "")
            cfg.sync_branch = data.get("sync-branch", "")
            cfg.flush_debounce = data.get("flush-debounce", "5s")

        # Environment variable overrides
        if os.environ.get("BD_ACTOR"):
            cfg.actor = os.environ["BD_ACTOR"]
        if os.environ.get("BEADS_DB"):
            cfg.db = os.environ["BEADS_DB"]
        if os.environ.get("BD_JSON"):
            cfg.json_output = os.environ["BD_JSON"].lower() in ("1", "true", "yes")

        return cfg

    def save(self, beads_dir: str) -> None:
        """Save config to config.yaml."""
        config_path = os.path.join(beads_dir, CONFIG_YAML)
        data: dict[str, Any] = {}
        if self.issue_prefix:
            data["issue-prefix"] = self.issue_prefix
        if self.no_db:
            data["no-db"] = self.no_db
        if self.no_auto_flush:
            data["no-auto-flush"] = self.no_auto_flush
        if self.no_auto_import:
            data["no-auto-import"] = self.no_auto_import
        if self.json_output:
            data["json"] = self.json_output
        if self.actor:
            data["actor"] = self.actor
        if self.db:
            data["db"] = self.db
        if self.sync_branch:
            data["sync-branch"] = self.sync_branch

        with open(config_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False)


@dataclass
class MetadataConfig:
    """Internal metadata from metadata.json (Go's configfile.Config)."""
    database: str = DEFAULT_DB_NAME
    jsonl_export: str = DEFAULT_JSONL_NAME
    backend: str = "sqlite"

    @classmethod
    def load(cls, beads_dir: str) -> MetadataConfig:
        """Load metadata.json from beads directory."""
        meta_path = os.path.join(beads_dir, METADATA_JSON)
        cfg = cls()
        if os.path.exists(meta_path):
            with open(meta_path) as f:
                data = json.load(f)
            cfg.database = data.get("database", DEFAULT_DB_NAME)
            cfg.jsonl_export = data.get("jsonl_export", DEFAULT_JSONL_NAME)
            cfg.backend = data.get("backend", "sqlite")
        return cfg

    def save(self, beads_dir: str) -> None:
        """Save metadata.json."""
        meta_path = os.path.join(beads_dir, METADATA_JSON)
        data = {
            "database": self.database,
            "jsonl_export": self.jsonl_export,
        }
        if self.backend and self.backend != "sqlite":
            data["backend"] = self.backend
        with open(meta_path, "w") as f:
            json.dump(data, f, indent=2)
            f.write("\n")


def find_beads_dir(start: str | None = None) -> str | None:
    """Walk up from start directory to find .beads/ directory.

    Returns absolute path to .beads/ directory, or None if not found.
    """
    if start is None:
        start = os.getcwd()
    current = os.path.abspath(start)
    while True:
        candidate = os.path.join(current, BEADS_DIR)
        if os.path.isdir(candidate):
            return candidate
        parent = os.path.dirname(current)
        if parent == current:
            return None
        current = parent


def get_db_path(beads_dir: str, config: BeadsConfig | None = None) -> str:
    """Get the full path to the SQLite database."""
    # Check environment override
    env_db = os.environ.get("BEADS_DB")
    if env_db:
        return env_db
    # Check config override
    if config and config.db:
        if os.path.isabs(config.db):
            return config.db
        return os.path.join(beads_dir, config.db)
    # Try metadata.json
    meta = MetadataConfig.load(beads_dir)
    return os.path.join(beads_dir, meta.database)


def get_jsonl_path(beads_dir: str) -> str:
    """Get the full path to the JSONL export file."""
    meta = MetadataConfig.load(beads_dir)
    return os.path.join(beads_dir, meta.jsonl_export)


def get_actor(config: BeadsConfig | None = None) -> str:
    """Get the actor name for audit trails."""
    if os.environ.get("BD_ACTOR"):
        return os.environ["BD_ACTOR"]
    if config and config.actor:
        return config.actor
    # Try git user
    try:
        import subprocess
        result = subprocess.run(
            ["git", "config", "user.email"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return os.environ.get("USER", "unknown")
