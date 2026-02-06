"""bd search - search issues."""

from __future__ import annotations

import click

from beads.cli import BeadsContext, pass_ctx
from beads.models import IssueFilter, Status
from beads.utils import format_issue_row


@click.command("search")
@click.argument("query")
@click.option("--status", "-s", default=None, help="Filter by status")
@click.option("--type", "issue_type", default=None, help="Filter by type")
@click.option("--limit", default=20, type=int, help="Max results")
@click.option("--all", "show_all", is_flag=True, help="Include closed")
@click.option("--long", "-L", "long_format", is_flag=True, help="Long format")
@pass_ctx
def search(ctx: BeadsContext, query: str, status: str | None,
           issue_type: str | None, limit: int, show_all: bool,
           long_format: bool) -> None:
    """Search issues by text query."""
    ctx.ensure_initialized()
    assert ctx.store is not None

    f = IssueFilter(limit=limit)
    if status:
        f.status = status
    elif not show_all:
        f.exclude_status = [Status.CLOSED, Status.TOMBSTONE]
    if issue_type:
        f.issue_type = issue_type

    issues = ctx.store.search_issues(query, f)

    if ctx.json_output:
        ctx.output([i.to_dict() for i in issues])
        return

    if not issues:
        click.echo(f"No issues matching '{query}'")
        return

    for issue in issues:
        click.echo(format_issue_row(issue, long_format=long_format))

    click.echo(f"\n{len(issues)} result(s)")
