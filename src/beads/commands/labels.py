"""bd label - manage labels."""

from __future__ import annotations

import sys

import click

from beads.cli import BeadsContext, pass_ctx


@click.group("label")
def label() -> None:
    """Manage issue labels."""


@label.command("add")
@click.argument("issue_id")
@click.argument("label_name")
@pass_ctx
def label_add(ctx: BeadsContext, issue_id: str, label_name: str) -> None:
    """Add a label to an issue."""
    ctx.ensure_initialized()
    assert ctx.store is not None

    full_id = ctx.resolve_issue_id(issue_id)
    ctx.store.add_label(full_id, label_name, ctx.actor)
    ctx.auto_flush()

    if not ctx.quiet:
        click.echo(f"Added label '{label_name}' to {full_id}")


@label.command("remove")
@click.argument("issue_id")
@click.argument("label_name")
@pass_ctx
def label_remove(ctx: BeadsContext, issue_id: str, label_name: str) -> None:
    """Remove a label from an issue."""
    ctx.ensure_initialized()
    assert ctx.store is not None

    full_id = ctx.resolve_issue_id(issue_id)
    ctx.store.remove_label(full_id, label_name, ctx.actor)
    ctx.auto_flush()

    if not ctx.quiet:
        click.echo(f"Removed label '{label_name}' from {full_id}")


@label.command("list")
@click.argument("issue_id")
@pass_ctx
def label_list(ctx: BeadsContext, issue_id: str) -> None:
    """List labels for an issue."""
    ctx.ensure_initialized()
    assert ctx.store is not None

    full_id = ctx.resolve_issue_id(issue_id)
    labels = ctx.store.get_labels(full_id)

    if ctx.json_output:
        ctx.output(labels)
        return

    if not labels:
        click.echo(f"No labels on {full_id}")
        return

    for lbl in labels:
        click.echo(f"  {lbl}")
