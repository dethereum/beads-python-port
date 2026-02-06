"""bd create - create a new issue."""

from __future__ import annotations

import json
import sys

import click

from beads.cli import BeadsContext, pass_ctx
from beads.config import get_jsonl_path
from beads.id_gen import generate_hash_id, make_issue_id, generate_child_id, check_hierarchy_depth
from beads.models import (
    Dependency, DepType, Issue, IssueType, Status, now_utc, parse_timestamp,
)


@click.command("create")
@click.option("--title", "-t", required=True, help="Issue title")
@click.option("--type", "issue_type", default="task",
              type=click.Choice(["bug", "feature", "task", "epic", "chore"]),
              help="Issue type")
@click.option("--priority", "-p", default=2, type=click.IntRange(0, 4),
              help="Priority (0=critical, 2=medium, 4=backlog)")
@click.option("--description", "-d", default="", help="Issue description")
@click.option("--design", default="", help="Design notes")
@click.option("--notes", "-n", default="", help="Additional notes")
@click.option("--assignee", "-a", default="", help="Assignee")
@click.option("--labels", "-l", multiple=True, help="Labels (repeatable)")
@click.option("--parent", default="", help="Parent issue ID")
@click.option("--deps", multiple=True, help="Dependencies (issue IDs that block this)")
@click.option("--due", default="", help="Due date (RFC3339)")
@click.option("--defer", "defer_until", default="", help="Defer until date (RFC3339)")
@click.option("--id", "custom_id", default="", help="Custom issue ID")
@click.option("--silent", is_flag=True, help="Only output the issue ID")
@click.option("--dry-run", is_flag=True, help="Preview without creating")
@pass_ctx
def create(ctx: BeadsContext, title: str, issue_type: str, priority: int,
           description: str, design: str, notes: str, assignee: str,
           labels: tuple[str, ...], parent: str, deps: tuple[str, ...],
           due: str, defer_until: str, custom_id: str, silent: bool,
           dry_run: bool) -> None:
    """Create a new issue."""
    ctx.ensure_initialized()
    assert ctx.store is not None and ctx.beads_dir is not None

    now = now_utc()
    prefix = ctx.store.get_config("issue_prefix") or "bd"

    # Generate ID
    if custom_id:
        issue_id = custom_id
    elif parent:
        # Resolve parent ID
        parent_id = ctx.resolve_issue_id(parent)
        # Check depth
        err = check_hierarchy_depth(parent_id)
        if err:
            click.echo(f"Error: {err}", err=True)
            sys.exit(1)
        child_num = ctx.store.next_child_number(parent_id)
        issue_id = generate_child_id(parent_id, child_num)
    else:
        workspace_id = prefix
        full_hash = generate_hash_id(prefix, title, description, now, workspace_id)
        issue_id = make_issue_id(prefix, full_hash, length=6)

        # Progressive collision handling
        for length in range(6, 13):
            existing = ctx.store.get_issue(issue_id)
            if existing is None:
                break
            issue_id = make_issue_id(prefix, full_hash, length=length + 1)

    issue = Issue(
        id=issue_id,
        title=title,
        description=description,
        design=design,
        notes=notes,
        status=Status.OPEN,
        priority=priority,
        issue_type=issue_type,
        assignee=assignee,
        created_at=now,
        created_by=ctx.actor,
        updated_at=now,
        labels=list(labels),
    )

    if due:
        issue.due_at = parse_timestamp(due)
    if defer_until:
        issue.defer_until = parse_timestamp(defer_until)

    if dry_run:
        if ctx.json_output:
            ctx.output(issue.to_dict())
        else:
            click.echo(f"Would create: {issue_id}")
            click.echo(f"  Title: {title}")
            click.echo(f"  Type: {issue_type}, Priority: P{priority}")
        return

    ctx.store.create_issue(issue, ctx.actor)

    # Add parent-child dependency
    if parent:
        parent_id = ctx.resolve_issue_id(parent)
        dep = Dependency(
            issue_id=issue_id,
            depends_on_id=parent_id,
            type=DepType.PARENT_CHILD,
            created_at=now,
            created_by=ctx.actor,
        )
        try:
            ctx.store.add_dependency(dep, ctx.actor)
        except ValueError as e:
            click.echo(f"Warning: could not add parent dependency: {e}", err=True)

    # Add blocking dependencies
    for dep_id in deps:
        resolved = ctx.resolve_issue_id(dep_id)
        dep = Dependency(
            issue_id=issue_id,
            depends_on_id=resolved,
            type=DepType.BLOCKS,
            created_at=now,
            created_by=ctx.actor,
        )
        try:
            ctx.store.add_dependency(dep, ctx.actor)
        except ValueError as e:
            click.echo(f"Warning: could not add dependency on {dep_id}: {e}", err=True)

    # Auto-flush
    ctx.auto_flush()

    if ctx.json_output:
        ctx.output({"id": issue_id})
    elif silent:
        click.echo(issue_id)
    else:
        click.echo(f"Created {issue_type} {issue_id}: {title}")
