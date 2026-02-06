"""bd comments / bd comment add - manage comments."""

from __future__ import annotations

import sys

import click

from beads.cli import BeadsContext, pass_ctx
from beads.utils import format_time_ago


@click.command("comments")
@click.argument("issue_id")
@pass_ctx
def comments(ctx: BeadsContext, issue_id: str) -> None:
    """List comments for an issue."""
    ctx.ensure_initialized()
    assert ctx.store is not None

    full_id = ctx.resolve_issue_id(issue_id)
    comment_list = ctx.store.get_comments(full_id)

    if ctx.json_output:
        ctx.output([c.to_dict() for c in comment_list])
        return

    if not comment_list:
        click.echo(f"No comments on {full_id}")
        return

    for c in comment_list:
        click.echo(f"  [{format_time_ago(c.created_at)}] {c.author}: {c.text}")


@click.group("comment")
def comment_add() -> None:
    """Manage comments."""


@comment_add.command("add")
@click.argument("issue_id")
@click.argument("text")
@pass_ctx
def _comment_add(ctx: BeadsContext, issue_id: str, text: str) -> None:
    """Add a comment to an issue."""
    ctx.ensure_initialized()
    assert ctx.store is not None

    full_id = ctx.resolve_issue_id(issue_id)
    comment_id = ctx.store.add_comment(full_id, ctx.actor, text)
    ctx.auto_flush()

    if ctx.json_output:
        ctx.output({"id": comment_id, "issue_id": full_id})
    elif not ctx.quiet:
        click.echo(f"Added comment to {full_id}")
