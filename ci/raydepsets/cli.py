import click


@click.group(name="depsets")
@click.pass_context
def cli(ctx):
    """Manage Python dependency sets."""
    pass
