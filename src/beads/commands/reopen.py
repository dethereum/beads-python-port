"""bd reopen - reopen a closed issue."""

from __future__ import annotations

import sys

import click

from beads.cli import BeadsContext, pass_ctx


@click.command("reopen")
@click.argument("issue_id")
@pass_ctx
def reopen(ctx: BeadsContext, issue_id: str) -> None:
    """Reopen a closed issue."""
    ctx.ensure_initialized()
    assert ctx.store is not None

    full_id = ctx.resolve_issue_id(issue_id)
    issue = ctx.store.get_issue(full_id)
    if issue is None:
        click.echo(f"Error: issue not found: {issue_id}", err=True)
        sys.exit(1)

    if issue.status != "closed":
        click.echo(f"Error: issue is not closed (status: {issue.status})", err=True)
        sys.exit(1)

    ctx.store.reopen_issue(full_id, ctx.actor)
    ctx.auto_flush()

    if ctx.json_output:
        ctx.output({"id": full_id, "status": "open"})
    elif not ctx.quiet:
        click.echo(f"Reopened {full_id}: {issue.title}")
