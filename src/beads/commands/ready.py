"""bd ready - show issues ready to work on."""

from __future__ import annotations

import click

from beads.cli import BeadsContext, pass_ctx
from beads.utils import format_issue_row


@click.command("ready")
@click.option("--type", "issue_type", default=None, help="Filter by type")
@click.option("--priority", "-p", type=int, default=None, help="Filter by priority")
@click.option("--assignee", "-a", default=None, help="Filter by assignee")
@click.option("--unassigned", is_flag=True, help="Only unassigned issues")
@click.option("--label", "-l", multiple=True, help="Filter by label")
@click.option("--limit", default=0, type=int, help="Max issues")
@click.option("--long", "-L", "long_format", is_flag=True, help="Long format")
@pass_ctx
def ready(ctx: BeadsContext, issue_type: str | None, priority: int | None,
          assignee: str | None, unassigned: bool, label: tuple[str, ...],
          limit: int, long_format: bool) -> None:
    """Show issues that are ready to work on (open, unblocked)."""
    ctx.ensure_initialized()
    assert ctx.store is not None

    work_filter: dict = {}
    if issue_type:
        work_filter["type"] = issue_type
    if priority is not None:
        work_filter["priority"] = priority
    if assignee:
        work_filter["assignee"] = assignee
    if unassigned:
        work_filter["unassigned"] = True
    if label:
        work_filter["labels"] = list(label)
    if limit:
        work_filter["limit"] = limit

    issues = ctx.store.get_ready_work(work_filter if work_filter else None)

    if ctx.json_output:
        ctx.output([i.to_dict() for i in issues])
        return

    if not issues:
        click.echo("No ready issues.")
        return

    for issue in issues:
        click.echo(format_issue_row(issue, long_format=long_format))

    if not ctx.quiet:
        click.echo(f"\n{len(issues)} ready issue(s)")
