"""bd close - close one or more issues."""

from __future__ import annotations

import click

from beads.cli import BeadsContext, pass_ctx


@click.command("close")
@click.argument("issue_ids", nargs=-1, required=True)
@click.option("--reason", "-r", default="", help="Close reason")
@click.option("--suggest-next", is_flag=True, help="Suggest next issue to work on")
@pass_ctx
def close(ctx: BeadsContext, issue_ids: tuple[str, ...], reason: str,
          suggest_next: bool) -> None:
    """Close one or more issues."""
    ctx.ensure_initialized()
    assert ctx.store is not None

    closed_ids = []
    for partial_id in issue_ids:
        full_id = ctx.resolve_issue_id(partial_id)
        issue = ctx.store.get_issue(full_id)
        if issue is None:
            click.echo(f"Warning: issue not found: {partial_id}", err=True)
            continue
        if issue.status == "closed":
            click.echo(f"Already closed: {full_id}", err=True)
            continue

        ctx.store.close_issue(full_id, reason, ctx.actor)
        closed_ids.append(full_id)

        if not ctx.quiet:
            click.echo(f"Closed {full_id}: {issue.title}")

    ctx.auto_flush()

    if ctx.json_output:
        ctx.output({"closed": closed_ids})

    if suggest_next and closed_ids:
        ready = ctx.store.get_ready_work({"limit": 3})
        if ready:
            click.echo("\nSuggested next:")
            for r in ready:
                click.echo(f"  {r.id} P{r.priority} {r.title}")
