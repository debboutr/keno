import click
from click_default_group import DefaultGroup

from app.utils import retreive


@click.group(cls=DefaultGroup, default="foo", default_if_no_args=True)
def cli():
    pass


@cli.command()
def foo():
    # retreive()
    click.echo("foo")


@cli.command()
def bar():
    click.echo("bar")
