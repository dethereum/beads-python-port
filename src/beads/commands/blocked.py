"""bd blocked - show blocked issues."""

from __future__ import annotations

import click

from beads.cli import BeadsContext, pass_ctx
from beads.utils import format_priority, truncate


@click.command("blocked")
@pass_ctx
def blocked(ctx: BeadsContext) -> None:
    """Show issues that are blocked by other issues."""
    ctx.ensure_initialized()
    assert ctx.store is not None

    blocked_list = ctx.store.get_blocked_issues()

    if ctx.json_output:
        data = []
        for issue, blocker_ids in blocked_list:
            d = issue.to_dict()
            d["blocked_by"] = blocker_ids
            data.append(d)
        ctx.output(data)
        return

    if not blocked_list:
        click.echo("No blocked issues.")
        return

    for issue, blocker_ids in blocked_list:
        pri = format_priority(issue.priority)
        title = truncate(issue.title, 45)
        blockers = ", ".join(blocker_ids)
        click.echo(f"  {issue.id:<20} {pri} {title}")
        click.echo(f"    blocked by: {blockers}")

    if not ctx.quiet:
        click.echo(f"\n{len(blocked_list)} blocked issue(s)")
