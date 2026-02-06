"""bd config - manage configuration."""

from __future__ import annotations

import sys

import click

from beads.cli import BeadsContext, pass_ctx


@click.group("config")
def config_cmd() -> None:
    """Manage beads configuration."""


@config_cmd.command("get")
@click.argument("key")
@pass_ctx
def config_get(ctx: BeadsContext, key: str) -> None:
    """Get a config value."""
    ctx.ensure_initialized()
    assert ctx.store is not None

    value = ctx.store.get_config(key)
    if value is None:
        click.echo(f"Config key not found: {key}", err=True)
        sys.exit(1)

    if ctx.json_output:
        ctx.output({key: value})
    else:
        click.echo(value)


@config_cmd.command("set")
@click.argument("key")
@click.argument("value")
@pass_ctx
def config_set(ctx: BeadsContext, key: str, value: str) -> None:
    """Set a config value."""
    ctx.ensure_initialized()
    assert ctx.store is not None

    ctx.store.set_config(key, value)

    if not ctx.quiet:
        click.echo(f"Set {key} = {value}")


@config_cmd.command("list")
@pass_ctx
def config_list(ctx: BeadsContext) -> None:
    """List all config values."""
    ctx.ensure_initialized()
    assert ctx.store is not None

    configs = ctx.store.list_config()

    if ctx.json_output:
        ctx.output(configs)
        return

    if not configs:
        click.echo("No config values set.")
        return

    for key, value in sorted(configs.items()):
        click.echo(f"  {key} = {value}")
