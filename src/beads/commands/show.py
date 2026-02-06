"""bd show - display issue details."""

from __future__ import annotations

import sys

import click

from beads.cli import BeadsContext, pass_ctx
from beads.models import DepType
from beads.utils import format_priority, format_time_ago, priority_label


@click.command("show")
@click.argument("issue_id")
@pass_ctx
def show(ctx: BeadsContext, issue_id: str) -> None:
    """Show detailed view of an issue."""
    ctx.ensure_initialized()
    assert ctx.store is not None

    full_id = ctx.resolve_issue_id(issue_id)
    issue = ctx.store.get_issue(full_id)
    if issue is None:
        click.echo(f"Error: issue not found: {issue_id}", err=True)
        sys.exit(1)

    if ctx.json_output:
        # Full JSON output with deps, labels, comments
        data = issue.to_dict()
        deps = ctx.store.get_dependencies(full_id)
        dependents = ctx.store.get_dependents(full_id)
        data["_dependencies"] = [d.to_dict() for d in deps]
        data["_dependents"] = [d.to_dict() for d in dependents]
        ctx.output(data)
        return

    # Header
    click.echo(f"{'─' * 60}")
    click.echo(f"  {issue.id}")
    click.echo(f"{'─' * 60}")
    click.echo(f"  Title:    {issue.title}")
    click.echo(f"  Status:   {issue.status}")
    click.echo(f"  Priority: {format_priority(issue.priority)} ({priority_label(issue.priority)})")
    click.echo(f"  Type:     {issue.issue_type}")

    if issue.assignee:
        click.echo(f"  Assignee: {issue.assignee}")

    click.echo(f"  Created:  {format_time_ago(issue.created_at)}")
    if issue.created_by:
        click.echo(f"  By:       {issue.created_by}")
    click.echo(f"  Updated:  {format_time_ago(issue.updated_at)}")

    if issue.closed_at:
        click.echo(f"  Closed:   {format_time_ago(issue.closed_at)}")
    if issue.close_reason:
        click.echo(f"  Reason:   {issue.close_reason}")
    if issue.due_at:
        click.echo(f"  Due:      {issue.due_at.isoformat()}")
    if issue.defer_until:
        click.echo(f"  Deferred: until {issue.defer_until.isoformat()}")

    # Labels
    labels = ctx.store.get_labels(full_id)
    if labels:
        click.echo(f"  Labels:   {', '.join(labels)}")

    # Description sections
    if issue.description:
        click.echo(f"\n  Description:")
        for line in issue.description.split("\n"):
            click.echo(f"    {line}")

    if issue.design:
        click.echo(f"\n  Design:")
        for line in issue.design.split("\n"):
            click.echo(f"    {line}")

    if issue.acceptance_criteria:
        click.echo(f"\n  Acceptance Criteria:")
        for line in issue.acceptance_criteria.split("\n"):
            click.echo(f"    {line}")

    if issue.notes:
        click.echo(f"\n  Notes:")
        for line in issue.notes.split("\n"):
            click.echo(f"    {line}")

    # Dependencies
    dep_records = ctx.store.get_dependency_records(full_id)
    if dep_records:
        click.echo(f"\n  Dependencies:")
        for dep in dep_records:
            dep_issue = ctx.store.get_issue(dep.depends_on_id)
            dep_title = dep_issue.title if dep_issue else "(unknown)"
            dep_status = dep_issue.status if dep_issue else "?"
            click.echo(f"    → {dep.depends_on_id} [{dep.type}] ({dep_status}) {dep_title}")

    # Dependents
    dependents = ctx.store.get_dependents(full_id)
    if dependents:
        click.echo(f"\n  Blocked by this:")
        for dep in dependents:
            click.echo(f"    ← {dep.id} ({dep.status}) {dep.title}")

    # Comments
    comments = ctx.store.get_comments(full_id)
    if comments:
        click.echo(f"\n  Comments ({len(comments)}):")
        for c in comments:
            click.echo(f"    [{format_time_ago(c.created_at)}] {c.author}: {c.text}")

    click.echo()
