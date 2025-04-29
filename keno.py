import click

from app.utils import retreive


@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx):
    if ctx.invoked_subcommand is None:
        click.echo("I was invoked without subcommand")
        # ctx.invoke(a)
    else:
        click.echo("I am about to invoke %s" % ctx.invoked_subcommand)


@cli.command()
@click.option("--test")
def a(test):
    # click.echo(test)
    click.echo("they should have never made it thru.")


@cli.command()
def sync():
    click.echo("The subcommand")


cli()
