"""Command-line entry point: `uv run toeval ...`."""

import typer

from toeval import __version__

app = typer.Typer(help="Toeval data platform.", no_args_is_help=True)


@app.callback()
def main() -> None:
    """Toeval data platform."""


@app.command()
def version() -> None:
    """Print the installed version."""
    typer.echo(__version__)
