#!/usr/bin/env python3

import click


@click.group(name="depsets")
@click.pass_context
def cli(ctx):
    """Manage Python dependency sets."""
    pass

if __name__ == "__main__":
    cli()
