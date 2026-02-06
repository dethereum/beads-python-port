"""bd doctor - health checks."""

from __future__ import annotations

import os

import click

from beads.cli import BeadsContext, pass_ctx
from beads.config import get_db_path, get_jsonl_path


@click.command("doctor")
@pass_ctx
def doctor(ctx: BeadsContext) -> None:
    """Run health checks on the beads project."""
    ctx.ensure_initialized()
    assert ctx.store is not None and ctx.beads_dir is not None

    issues_found = 0

    click.echo("Beads Doctor")
    click.echo("─" * 40)

    # Check .beads/ directory
    click.echo(f"  .beads/ directory: {ctx.beads_dir}")
    if os.path.isdir(ctx.beads_dir):
        click.echo("    [OK] exists")
    else:
        click.echo("    [ERROR] not found")
        issues_found += 1

    # Check database
    db_path = get_db_path(ctx.beads_dir, ctx.config)
    if os.path.exists(db_path):
        click.echo(f"  Database: {db_path}")
        click.echo("    [OK] exists")
        # Check schema version
        version = ctx.store.get_metadata("schema_version")
        click.echo(f"    Schema version: {version or 'unknown'}")
    else:
        click.echo(f"  Database: {db_path}")
        click.echo("    [WARN] not found (will be created on first use)")

    # Check JSONL file
    jsonl_path = get_jsonl_path(ctx.beads_dir)
    if os.path.exists(jsonl_path):
        size = os.path.getsize(jsonl_path)
        click.echo(f"  JSONL: {jsonl_path}")
        click.echo(f"    [OK] exists ({size} bytes)")
    else:
        click.echo(f"  JSONL: {jsonl_path}")
        click.echo("    [INFO] not found (no issues exported yet)")

    # Check config
    prefix = ctx.store.get_config("issue_prefix")
    click.echo(f"  Issue prefix: {prefix or '(not set)'}")

    # Check dirty issues
    dirty = ctx.store.get_dirty_issues()
    if dirty:
        click.echo(f"  Dirty issues: {len(dirty)} (need JSONL export)")
    else:
        click.echo("  Dirty issues: 0 (in sync)")

    # Check for cycles in dependency graph
    click.echo("\n  Checking for dependency cycles...")
    # Simple check: for each dependency, verify no cycle
    from beads.models import IssueFilter
    all_issues = ctx.store.search_issues("", IssueFilter(include_tombstones=False))
    cycle_found = False
    for issue in all_issues:
        deps = ctx.store.get_dependency_records(issue.id)
        for dep in deps:
            if ctx.store.has_cycle(dep.depends_on_id, dep.issue_id):
                if not cycle_found:
                    click.echo("    [WARN] Circular dependencies detected:")
                click.echo(f"      {dep.issue_id} <-> {dep.depends_on_id}")
                cycle_found = True
                issues_found += 1
    if not cycle_found:
        click.echo("    [OK] no cycles")

    # Summary
    click.echo()
    if issues_found:
        click.echo(f"Found {issues_found} issue(s)")
    else:
        click.echo("All checks passed!")
