"""bd sync - sync database with JSONL."""

from __future__ import annotations

import click

from beads.cli import BeadsContext, pass_ctx
from beads.config import get_jsonl_path
from beads.export import flush_to_jsonl


@click.command("sync")
@click.option("--flush-only", is_flag=True, help="Only export DB to JSONL (no git)")
@pass_ctx
def sync_cmd(ctx: BeadsContext, flush_only: bool) -> None:
    """Sync database with JSONL file."""
    ctx.ensure_initialized()
    assert ctx.store is not None and ctx.beads_dir is not None

    jsonl_path = get_jsonl_path(ctx.beads_dir)

    if flush_only:
        count = flush_to_jsonl(ctx.store, jsonl_path, verbose=True)
        if not ctx.quiet:
            click.echo(f"Flushed {count} issues to {jsonl_path}")
        return

    # Full sync: import then export
    from beads.importer import import_jsonl
    result = import_jsonl(ctx.store, jsonl_path, verbose=ctx.verbose)

    if not ctx.quiet and result:
        click.echo(
            f"Imported: {result.created} new, {result.updated} updated, "
            f"{result.unchanged} unchanged"
        )

    count = flush_to_jsonl(ctx.store, jsonl_path, verbose=ctx.verbose)

    if not ctx.quiet:
        click.echo(f"Exported {count} issues to JSONL")
