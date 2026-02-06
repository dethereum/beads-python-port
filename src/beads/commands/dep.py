"""bd dep - manage dependencies."""

from __future__ import annotations

import sys

import click

from beads.cli import BeadsContext, pass_ctx
from beads.models import Dependency, DepType, now_utc
from beads.utils import truncate


@click.group("dep")
def dep() -> None:
    """Manage issue dependencies."""


@dep.command("add")
@click.argument("issue_id")
@click.argument("depends_on_id")
@click.option("--type", "dep_type", default="blocks", help="Dependency type")
@pass_ctx
def dep_add(ctx: BeadsContext, issue_id: str, depends_on_id: str,
            dep_type: str) -> None:
    """Add a dependency: ISSUE_ID depends on DEPENDS_ON_ID."""
    ctx.ensure_initialized()
    assert ctx.store is not None

    full_issue = ctx.resolve_issue_id(issue_id)
    full_depends = ctx.resolve_issue_id(depends_on_id)

    dependency = Dependency(
        issue_id=full_issue,
        depends_on_id=full_depends,
        type=dep_type,
        created_at=now_utc(),
        created_by=ctx.actor,
    )

    try:
        ctx.store.add_dependency(dependency, ctx.actor)
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    ctx.auto_flush()

    if not ctx.quiet:
        click.echo(f"Added dependency: {full_issue} depends on {full_depends} ({dep_type})")


@dep.command("remove")
@click.argument("issue_id")
@click.argument("depends_on_id")
@pass_ctx
def dep_remove(ctx: BeadsContext, issue_id: str, depends_on_id: str) -> None:
    """Remove a dependency."""
    ctx.ensure_initialized()
    assert ctx.store is not None

    full_issue = ctx.resolve_issue_id(issue_id)
    full_depends = ctx.resolve_issue_id(depends_on_id)

    ctx.store.remove_dependency(full_issue, full_depends, ctx.actor)
    ctx.auto_flush()

    if not ctx.quiet:
        click.echo(f"Removed dependency: {full_issue} → {full_depends}")


@dep.command("list")
@click.argument("issue_id")
@pass_ctx
def dep_list(ctx: BeadsContext, issue_id: str) -> None:
    """List dependencies for an issue."""
    ctx.ensure_initialized()
    assert ctx.store is not None

    full_id = ctx.resolve_issue_id(issue_id)
    deps = ctx.store.get_dependency_records(full_id)
    dependents = ctx.store.get_dependents(full_id)

    if ctx.json_output:
        ctx.output({
            "dependencies": [d.to_dict() for d in deps],
            "dependents": [d.to_dict() for d in dependents],
        })
        return

    if deps:
        click.echo(f"Dependencies of {full_id}:")
        for d in deps:
            dep_issue = ctx.store.get_issue(d.depends_on_id)
            title = dep_issue.title if dep_issue else "(unknown)"
            status = dep_issue.status if dep_issue else "?"
            click.echo(f"  → {d.depends_on_id} [{d.type}] ({status}) {truncate(title)}")
    else:
        click.echo(f"No dependencies for {full_id}")

    if dependents:
        click.echo(f"\nDepended on by:")
        for d in dependents:
            click.echo(f"  ← {d.id} ({d.status}) {truncate(d.title)}")
