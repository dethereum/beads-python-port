"""bd init - initialize a new .beads/ directory."""

from __future__ import annotations

import os

import click

from beads.cli import BeadsContext, pass_ctx
from beads.config import (
    BEADS_DIR, BeadsConfig, MetadataConfig, get_db_path,
)
from beads.storage.sqlite_store import SQLiteStorage


@click.command("init")
@click.option("--prefix", help="Issue prefix (default: directory name)")
@pass_ctx
def init_cmd(ctx: BeadsContext, prefix: str | None) -> None:
    """Initialize a new beads project in the current directory."""
    beads_dir = os.path.join(os.getcwd(), BEADS_DIR)

    if os.path.exists(beads_dir):
        click.echo(f"Beads already initialized at {beads_dir}")
        return

    # Determine prefix
    if not prefix:
        prefix = os.path.basename(os.getcwd()).lower()
        # Sanitize: only keep alphanumeric and hyphens
        prefix = "".join(c if c.isalnum() or c == "-" else "-" for c in prefix)
        prefix = prefix.strip("-")
        if not prefix:
            prefix = "bd"

    os.makedirs(beads_dir, exist_ok=True)

    # Create config.yaml
    config = BeadsConfig(issue_prefix=prefix)
    config.save(beads_dir)

    # Create metadata.json
    meta = MetadataConfig()
    meta.save(beads_dir)

    # Create .gitignore
    gitignore_path = os.path.join(beads_dir, ".gitignore")
    with open(gitignore_path, "w") as f:
        f.write("# Beads local files (not shared via git)\n")
        f.write("*.db\n")
        f.write("*.db-wal\n")
        f.write("*.db-shm\n")
        f.write(".sync.lock\n")
        f.write(".local_version\n")

    # Create README.md
    readme_path = os.path.join(beads_dir, "README.md")
    with open(readme_path, "w") as f:
        f.write("# Beads Issue Tracker\n\n")
        f.write("This directory contains beads issue tracking data.\n\n")
        f.write("- `issues.jsonl` - Issue data (shared via git)\n")
        f.write("- `config.yaml` - Project configuration (shared via git)\n")
        f.write("- `*.db` - Local SQLite database (gitignored)\n\n")
        f.write("Run `bd --help` for usage information.\n")

    # Initialize SQLite database
    db_path = os.path.join(beads_dir, meta.database)
    store = SQLiteStorage(db_path)
    store.set_config("issue_prefix", prefix)
    store.close()

    click.echo(f"Initialized beads in {beads_dir}")
    click.echo(f"  Issue prefix: {prefix}")
    click.echo(f"  Database: {meta.database}")
