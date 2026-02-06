"""bd list - list issues."""

from __future__ import annotations

import json

import click

from beads.cli import BeadsContext, pass_ctx
from beads.models import IssueFilter, Status
from beads.utils import format_issue_row


@click.command("list")
@click.option("--status", "-s", "status", default=None, help="Filter by status")
@click.option("--priority", "-p", type=int, default=None, help="Filter by priority")
@click.option("--assignee", "-a", default=None, help="Filter by assignee")
@click.option("--type", "issue_type", default=None, help="Filter by issue type")
@click.option("--label", "-l", multiple=True, help="Filter by label (AND)")
@click.option("--limit", default=0, type=int, help="Max issues to show")
@click.option("--all", "show_all", is_flag=True, help="Include closed issues")
@click.option("--sort", "sort_by", default="created",
              type=click.Choice(["created", "updated", "priority", "status", "title", "id"]),
              help="Sort field")
@click.option("--reverse", "-r", is_flag=True, help="Reverse sort order")
@click.option("--long", "-L", "long_format", is_flag=True, help="Long format with extra fields")
@click.option("--ready", is_flag=True, help="Only show ready (unblocked) issues")
@pass_ctx
def list_cmd(ctx: BeadsContext, status: str | None, priority: int | None,
             assignee: str | None, issue_type: str | None,
             label: tuple[str, ...], limit: int, show_all: bool,
             sort_by: str, reverse: bool, long_format: bool,
             ready: bool) -> None:
    """List issues with filters."""
    ctx.ensure_initialized()
    assert ctx.store is not None

    if ready:
        # Delegate to ready work query
        work_filter = {}
        if issue_type:
            work_filter["type"] = issue_type
        if priority is not None:
            work_filter["priority"] = priority
        if assignee:
            work_filter["assignee"] = assignee
        if label:
            work_filter["labels"] = list(label)
        if limit:
            work_filter["limit"] = limit
        issues = ctx.store.get_ready_work(work_filter if work_filter else None)
    else:
        f = IssueFilter()
        if status:
            f.status = status
        elif not show_all:
            f.exclude_status = [Status.CLOSED, Status.TOMBSTONE]
        if priority is not None:
            f.priority = priority
        if assignee:
            f.assignee = assignee
        if issue_type:
            f.issue_type = issue_type
        if label:
            f.labels = list(label)
        if limit:
            f.limit = limit

        issues = ctx.store.list_issues(f, sort_by=sort_by, reverse=reverse)

    if ctx.json_output:
        ctx.output([i.to_dict() for i in issues])
        return

    if not issues:
        click.echo("No issues found.")
        return

    for issue in issues:
        click.echo(format_issue_row(issue, long_format=long_format))

    if not ctx.quiet:
        click.echo(f"\n{len(issues)} issue(s)")
