"""bd stats - show project statistics."""

from __future__ import annotations

import click

from beads.cli import BeadsContext, pass_ctx
from beads.utils import format_priority


@click.command("stats")
@pass_ctx
def stats(ctx: BeadsContext) -> None:
    """Show project statistics."""
    ctx.ensure_initialized()
    assert ctx.store is not None

    s = ctx.store.get_statistics()

    if ctx.json_output:
        ctx.output({
            "total_issues": s.total_issues,
            "open_issues": s.open_issues,
            "in_progress_issues": s.in_progress_issues,
            "closed_issues": s.closed_issues,
            "blocked_issues": s.blocked_issues,
            "deferred_issues": s.deferred_issues,
            "ready_issues": s.ready_issues,
            "tombstone_issues": s.tombstone_issues,
            "pinned_issues": s.pinned_issues,
            "by_type": s.by_type,
            "by_priority": s.by_priority,
        })
        return

    click.echo("Project Statistics")
    click.echo("─" * 40)
    click.echo(f"  Total:       {s.total_issues}")
    click.echo(f"  Open:        {s.open_issues}")
    click.echo(f"  In Progress: {s.in_progress_issues}")
    click.echo(f"  Blocked:     {s.blocked_issues}")
    click.echo(f"  Deferred:    {s.deferred_issues}")
    click.echo(f"  Ready:       {s.ready_issues}")
    click.echo(f"  Closed:      {s.closed_issues}")

    if s.pinned_issues:
        click.echo(f"  Pinned:      {s.pinned_issues}")
    if s.tombstone_issues:
        click.echo(f"  Tombstones:  {s.tombstone_issues}")

    if s.by_type:
        click.echo(f"\nBy Type:")
        for t, count in sorted(s.by_type.items()):
            click.echo(f"  {t:<12} {count}")

    if s.by_priority:
        click.echo(f"\nBy Priority:")
        for p in sorted(s.by_priority.keys()):
            click.echo(f"  {format_priority(p):<12} {s.by_priority[p]}")
