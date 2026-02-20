"""
Click CLI entry point for the FAOSTAT pipeline.

Commands:
  faostat-pipeline fetch        Fetch and export data for one or more domains
  faostat-pipeline list-groups  List all FAOSTAT data groups
  faostat-pipeline list-domains List all domains in a group
  faostat-pipeline metadata     Show metadata for a domain
  faostat-pipeline dimensions   Show dimensions/filters for a domain
  faostat-pipeline datasize     Estimate rows for a query
  faostat-pipeline ping         Check API health
"""

import asyncio
import json
import logging
from typing import Any

import click
from rich.console import Console
from rich.table import Table

from .client import FAOSTATClient, FAOSTATAuthError
from .endpoints import (
    ping,
    get_groups,
    get_groups_and_domains,
    get_domains,
    get_dimensions,
    get_metadata,
    get_datasize,
)
from .pipeline import run_pipeline, run_pipeline_batch
from .spinner import spin_while

console = Console()


def _extract_list(
    response: Any,
    fallback_keys: tuple[str, ...] = ("data", "Data", "items", "results"),
) -> list:
    """Extract a list of records from an API response, trying common envelope keys."""
    if isinstance(response, list):
        return response
    if isinstance(response, dict):
        for key in fallback_keys:
            if key in response and isinstance(response[key], list):
                return response[key]
    console.print(
        f"[yellow]Warning:[/yellow] Unexpected response structure. "
        f"Keys: {list(response.keys()) if isinstance(response, dict) else type(response).__name__}"
    )
    console.print(f"[dim]Raw (first 500 chars): {str(response)[:500]}[/dim]")
    return []


def run(coro):
    """Helper to run async functions from sync Click commands."""
    return asyncio.run(coro)


def _handle_errors(func):
    """Decorator to catch common errors and show clean messages."""
    import functools

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FAOSTATAuthError as e:
            console.print(f"[red]Auth error:[/red] {e}")
            raise click.Abort()
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            raise click.Abort()

    return wrapper


@click.group()
@click.option("-v", "--verbose", is_flag=True, default=False, help="Enable debug logging")
def cli(verbose: bool):
    """FAOSTAT Data Pipeline — download and clean FAO agricultural statistics."""
    if verbose:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(name)s %(levelname)s: %(message)s",
        )


@cli.command("ping")
@_handle_errors
def ping_cmd():
    """Check the FAOSTAT API health status."""
    async def _run():
        async with FAOSTATClient() as client:
            result = await spin_while(ping(client), label="Pinging API", console=console)
            console.print_json(json.dumps(result))
    run(_run())


@cli.command("list-groups")
@click.option("--lang", default="en", show_default=True, help="Language code")
@_handle_errors
def list_groups(lang: str):
    """List all top-level FAOSTAT data groups."""
    async def _run():
        async with FAOSTATClient() as client:
            groups = await spin_while(get_groups(client, lang=lang), label="Fetching groups", console=console)
        table = Table(title="FAOSTAT Data Groups", show_lines=True)
        table.add_column("Code", style="cyan", no_wrap=True)
        table.add_column("Label", style="white")
        for g in _extract_list(groups):
            table.add_row(str(g.get("GroupCode", g.get("code", ""))),
                          str(g.get("GroupLabel", g.get("label", g.get("name", "")))))
        console.print(table)
    run(_run())


@cli.command("list-domains")
@click.argument("group_code")
@click.option("--lang", default="en", show_default=True, help="Language code")
@_handle_errors
def list_domains(group_code: str, lang: str):
    """List all domains in GROUP_CODE (e.g. 'Q' for Production)."""
    async def _run():
        async with FAOSTATClient() as client:
            domains = await spin_while(get_domains(client, group_code=group_code, lang=lang), label="Fetching domains", console=console)
        table = Table(title=f"Domains in group '{group_code}'", show_lines=True)
        table.add_column("Code", style="cyan", no_wrap=True)
        table.add_column("Label", style="white")
        for d in _extract_list(domains):
            table.add_row(str(d.get("DomainCode", d.get("code", ""))),
                          str(d.get("DomainLabel", d.get("label", d.get("name", "")))))
        console.print(table)
    run(_run())


@cli.command("dimensions")
@click.argument("domain_code")
@click.option("--lang", default="en", show_default=True, help="Language code")
@_handle_errors
def dimensions(domain_code: str, lang: str):
    """Show available dimensions/filters for DOMAIN_CODE."""
    async def _run():
        async with FAOSTATClient() as client:
            result = await spin_while(get_dimensions(client, domain_code=domain_code, lang=lang), label="Fetching dimensions", console=console)
        console.print_json(json.dumps(result))
    run(_run())


@cli.command("metadata")
@click.argument("domain_code")
@click.option("--lang", default="en", show_default=True, help="Language code")
@_handle_errors
def metadata(domain_code: str, lang: str):
    """Show metadata for DOMAIN_CODE."""
    async def _run():
        async with FAOSTATClient() as client:
            result = await spin_while(get_metadata(client, domain_code=domain_code, lang=lang), label="Fetching metadata", console=console)
        console.print_json(json.dumps(result))
    run(_run())


@cli.command("datasize")
@click.argument("domain_code")
@click.option("--lang", default="en", show_default=True)
@click.option("--area", default=None, help="Area/country codes (comma-separated)")
@click.option("--element", default=None, help="Element codes (comma-separated)")
@click.option("--item", default=None, help="Item codes (comma-separated)")
@click.option("--year", default=None, help="Year codes (comma-separated)")
@_handle_errors
def datasize(domain_code: str, lang: str, area, element, item, year):
    """Estimate the number of rows for a DOMAIN_CODE query."""
    async def _run():
        payload: dict = {"domain_code": domain_code}
        for k, v in [("area", area), ("element", element), ("item", item), ("year", year)]:
            if v:
                payload[k] = v
        async with FAOSTATClient() as client:
            result = await spin_while(get_datasize(client, payload, lang=lang), label="Estimating data size", console=console)
        console.print_json(json.dumps(result))
    run(_run())


@cli.command("fetch")
@click.argument("domain_codes", nargs=-1, required=True)
@click.option("--lang", default="en", show_default=True, help="Language code")
@click.option("--output", "-o", default="./output", show_default=True, help="Output directory")
@click.option("--no-csv", is_flag=True, default=False, help="Skip CSV output")
@click.option("--no-parquet", is_flag=True, default=False, help="Skip Parquet output")
@click.option("--compression", default="snappy", show_default=True,
              type=click.Choice(["snappy", "gzip", "brotli", "none"]),
              help="Parquet compression codec")
@click.option("--area", default=None, help="Filter by area codes (comma-separated)")
@click.option("--element", default=None, help="Filter by element codes (comma-separated)")
@click.option("--item", default=None, help="Filter by item codes (comma-separated)")
@click.option("--year", default=None, help="Filter by year codes (comma-separated)")
@_handle_errors
def fetch(domain_codes, lang, output, no_csv, no_parquet, compression, area, element, item, year):
    """
    Fetch, clean, and export data for one or more DOMAIN_CODES.

    Examples:

    \b
      faostat-pipeline fetch QCL
      faostat-pipeline fetch QCL TM FS --output ./data
      faostat-pipeline fetch QCL --area 231 --year 2020,2021,2022
      faostat-pipeline fetch QCL --no-csv
    """
    filters = {}
    for k, v in [("area", area), ("element", element), ("item", item), ("year", year)]:
        if v:
            filters[k] = v

    to_csv = not no_csv
    to_parquet = not no_parquet

    if not to_csv and not to_parquet:
        console.print("[red]Error:[/red] At least one of CSV or Parquet output must be enabled.")
        raise click.Abort()

    console.print(f"\n[bold blue]FAOSTAT Pipeline[/bold blue]")
    console.print(f"Domains  : {', '.join(domain_codes)}")
    console.print(f"Output   : {output}")
    console.print(f"Formats  : {'CSV ' if to_csv else ''}{'Parquet' if to_parquet else ''}\n")

    async def _run():
        if len(domain_codes) == 1:
            written = await run_pipeline(
                domain_code=domain_codes[0],
                lang=lang,
                output_dir=output,
                to_csv=to_csv,
                to_parquet=to_parquet,
                parquet_compression=compression,
                filters=filters,
            )
            for fmt, path in written.items():
                console.print(f"[green]✓[/green] {fmt.upper()}: {path}")
        else:
            await run_pipeline_batch(
                domain_codes=list(domain_codes),
                lang=lang,
                output_dir=output,
                to_csv=to_csv,
                to_parquet=to_parquet,
                parquet_compression=compression,
                filters=filters,
            )
        console.print("\n[bold green]Done![/bold green]")

    run(_run())
