"""bd update - update an issue."""

from __future__ import annotations

import sys

import click

from beads.cli import BeadsContext, pass_ctx
from beads.models import Status, parse_timestamp


@click.command("update")
@click.argument("issue_id")
@click.option("--status", "-s", default=None, help="New status")
@click.option("--priority", "-p", type=int, default=None, help="New priority (0-4)")
@click.option("--title", "-t", default=None, help="New title")
@click.option("--type", "issue_type", default=None, help="New issue type")
@click.option("--assignee", "-a", default=None, help="New assignee (empty to clear)")
@click.option("--description", "-d", default=None, help="New description")
@click.option("--design", default=None, help="New design notes")
@click.option("--notes", "-n", default=None, help="New notes")
@click.option("--append-notes", default=None, help="Append to notes")
@click.option("--add-label", multiple=True, help="Add label")
@click.option("--remove-label", multiple=True, help="Remove label")
@click.option("--due", default=None, help="Due date (RFC3339, empty to clear)")
@click.option("--defer", "defer_until", default=None, help="Defer until (RFC3339, empty to clear)")
@click.option("--claim", is_flag=True, help="Claim issue (set assignee to actor, status to in_progress)")
@pass_ctx
def update(ctx: BeadsContext, issue_id: str, status: str | None,
           priority: int | None, title: str | None, issue_type: str | None,
           assignee: str | None, description: str | None, design: str | None,
           notes: str | None, append_notes: str | None,
           add_label: tuple[str, ...], remove_label: tuple[str, ...],
           due: str | None, defer_until: str | None, claim: bool) -> None:
    """Update an existing issue."""
    ctx.ensure_initialized()
    assert ctx.store is not None

    full_id = ctx.resolve_issue_id(issue_id)
    issue = ctx.store.get_issue(full_id)
    if issue is None:
        click.echo(f"Error: issue not found: {issue_id}", err=True)
        sys.exit(1)

    updates: dict = {}

    if claim:
        updates["assignee"] = ctx.actor
        updates["status"] = Status.IN_PROGRESS

    if status is not None:
        updates["status"] = status
    if priority is not None:
        if priority < 0 or priority > 4:
            click.echo("Error: priority must be 0-4", err=True)
            sys.exit(1)
        updates["priority"] = priority
    if title is not None:
        updates["title"] = title
    if issue_type is not None:
        updates["issue_type"] = issue_type
    if assignee is not None:
        updates["assignee"] = assignee if assignee else None
    if description is not None:
        updates["description"] = description
    if design is not None:
        updates["design"] = design
    if notes is not None:
        updates["notes"] = notes
    if append_notes is not None:
        current_notes = issue.notes or ""
        if current_notes:
            updates["notes"] = current_notes + "\n" + append_notes
        else:
            updates["notes"] = append_notes

    if due is not None:
        if due == "":
            updates["due_at"] = None
        else:
            updates["due_at"] = parse_timestamp(due)
    if defer_until is not None:
        if defer_until == "":
            updates["defer_until"] = None
        else:
            updates["defer_until"] = parse_timestamp(defer_until)

    # Handle status=closed requiring closed_at
    if updates.get("status") == Status.CLOSED and "closed_at" not in updates:
        from beads.models import now_utc
        updates["closed_at"] = now_utc()

    if not updates and not add_label and not remove_label:
        click.echo("No updates specified.", err=True)
        sys.exit(1)

    if updates:
        ctx.store.update_issue(full_id, updates, ctx.actor)

    for lbl in add_label:
        ctx.store.add_label(full_id, lbl, ctx.actor)
    for lbl in remove_label:
        ctx.store.remove_label(full_id, lbl, ctx.actor)

    ctx.auto_flush()

    if ctx.json_output:
        updated = ctx.store.get_issue(full_id)
        if updated:
            ctx.output(updated.to_dict())
    elif not ctx.quiet:
        click.echo(f"Updated {full_id}")
