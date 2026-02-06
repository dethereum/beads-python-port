"""Click CLI root and global flags for beads (bd)."""

from __future__ import annotations

import json
import os
import sys

import click

from beads import __version__
from beads.config import BeadsConfig, find_beads_dir, get_actor, get_db_path, get_jsonl_path
from beads.storage.sqlite_store import SQLiteStorage


class BeadsContext:
    """Shared context for all commands."""

    def __init__(self) -> None:
        self.beads_dir: str | None = None
        self.store: SQLiteStorage | None = None
        self.config: BeadsConfig | None = None
        self.actor: str = ""
        self.json_output: bool = False
        self.verbose: bool = False
        self.quiet: bool = False

    def ensure_initialized(self) -> None:
        """Ensure beads directory and storage are available."""
        if self.store is not None:
            return
        self.beads_dir = find_beads_dir()
        if self.beads_dir is None:
            click.echo("Error: not in a beads project (no .beads/ directory found)", err=True)
            click.echo("Run 'bd init' to create one", err=True)
            sys.exit(1)
        self.config = BeadsConfig.load(self.beads_dir)
        if not self.actor:
            self.actor = get_actor(self.config)
        if not self.json_output and self.config:
            self.json_output = self.config.json_output
        db_path = get_db_path(self.beads_dir, self.config)
        self.store = SQLiteStorage(db_path)

        # Set issue prefix in DB config if not already set
        if self.config and self.config.issue_prefix:
            current = self.store.get_config("issue_prefix")
            if not current:
                self.store.set_config("issue_prefix", self.config.issue_prefix)

        # Auto-import JSONL if newer
        if self.config and not self.config.no_auto_import:
            from beads.importer import auto_import_if_needed
            jsonl_path = get_jsonl_path(self.beads_dir)
            auto_import_if_needed(self.store, jsonl_path, verbose=self.verbose)

    def auto_flush(self) -> None:
        """Auto-flush dirty issues to JSONL if configured."""
        if self.config and self.config.no_auto_flush:
            return
        if not self.beads_dir or not self.store:
            return
        dirty = self.store.get_dirty_issues()
        if dirty:
            from beads.export import flush_to_jsonl
            jsonl_path = get_jsonl_path(self.beads_dir)
            flush_to_jsonl(self.store, jsonl_path, verbose=self.verbose)

    def resolve_issue_id(self, partial: str) -> str:
        """Resolve a partial issue ID or exit with error."""
        assert self.store is not None
        full_id = self.store.resolve_id(partial)
        if full_id is None:
            click.echo(f"Error: issue not found or ambiguous: {partial}", err=True)
            sys.exit(1)
        return full_id

    def output(self, data: dict | list) -> None:
        """Output data as JSON or formatted text."""
        click.echo(json.dumps(data, indent=2, default=str))


pass_ctx = click.make_pass_decorator(BeadsContext, ensure=True)


@click.group(invoke_without_command=True)
@click.option("--db", envvar="BEADS_DB", help="Path to database file")
@click.option("--actor", envvar="BD_ACTOR", help="Actor name for audit trails")
@click.option("--json", "json_output", is_flag=True, help="Output as JSON")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.option("--quiet", "-q", is_flag=True, help="Suppress non-essential output")
@click.version_option(__version__, prog_name="bd")
@click.pass_context
def cli(ctx: click.Context, db: str | None, actor: str | None,
        json_output: bool, verbose: bool, quiet: bool) -> None:
    """bd - distributed issue tracker"""
    bctx = ctx.ensure_object(BeadsContext)
    bctx.verbose = verbose
    bctx.quiet = quiet
    if json_output:
        bctx.json_output = True
    if actor:
        bctx.actor = actor
    if db:
        os.environ["BEADS_DB"] = db

    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


# --- Register all command groups ---

from beads.commands.init_cmd import init_cmd
from beads.commands.create import create
from beads.commands.list_cmd import list_cmd
from beads.commands.show import show
from beads.commands.update import update
from beads.commands.close import close
from beads.commands.reopen import reopen
from beads.commands.ready import ready
from beads.commands.blocked import blocked
from beads.commands.dep import dep
from beads.commands.search import search
from beads.commands.stats import stats
from beads.commands.sync_cmd import sync_cmd
from beads.commands.doctor import doctor
from beads.commands.labels import label
from beads.commands.comments import comments, comment_add
from beads.commands.config_cmd import config_cmd

cli.add_command(init_cmd, "init")
cli.add_command(create, "create")
cli.add_command(create, "new")  # Alias
cli.add_command(list_cmd, "list")
cli.add_command(show, "show")
cli.add_command(show, "view")  # Alias
cli.add_command(update, "update")
cli.add_command(close, "close")
cli.add_command(reopen, "reopen")
cli.add_command(ready, "ready")
cli.add_command(blocked, "blocked")
cli.add_command(dep, "dep")
cli.add_command(search, "search")
cli.add_command(stats, "stats")
cli.add_command(sync_cmd, "sync")
cli.add_command(doctor, "doctor")
cli.add_command(label, "label")
cli.add_command(comments, "comments")
cli.add_command(comment_add, "comment")
cli.add_command(config_cmd, "config")


def main() -> None:
    cli(auto_envvar_prefix="BD")
