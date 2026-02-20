"""
Pipeline orchestration: fetch → clean → export.
"""

import logging
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from .client import FAOSTATClient
from .cleaner import clean_data
from .endpoints import get_data, get_datasize
from .exporter import export
from .spinner import spin_while

logger = logging.getLogger("faostat_pipeline")
console = Console()


def _response_has_data(raw: Any) -> bool:
    """Check whether the API response actually contains data records."""
    if raw is None:
        return False
    if isinstance(raw, list):
        return len(raw) > 0
    if isinstance(raw, dict):
        for key in ("data", "Data", "items", "results"):
            if key in raw and isinstance(raw[key], list):
                return len(raw[key]) > 0
        # If none of the known keys exist, the response is likely an error or empty envelope
        return False
    return False


async def run_pipeline(
    domain_code: str,
    lang: str = "en",
    output_dir: str | Path = "./output",
    to_csv: bool = True,
    to_parquet: bool = True,
    parquet_compression: str = "snappy",
    filters: dict[str, Any] | None = None,
    check_size_first: bool = True,
) -> dict[str, Path]:
    """
    Fetch, clean, and export data for a single FAOSTAT domain.

    Args:
        domain_code: FAOSTAT domain code (e.g. "QCL", "TM", "FS")
        lang: Language code (default: "en")
        output_dir: Directory to write output files
        to_csv: Write CSV output
        to_parquet: Write Parquet output
        parquet_compression: Parquet compression codec
        filters: Optional dict of query filters (area, element, item, year, etc.)
        check_size_first: If True, log estimated row count before fetching

    Returns:
        Dict of written file paths: {"csv": Path, "parquet": Path}
    """
    filters = filters or {}

    async with FAOSTATClient() as client:
        if check_size_first:
            try:
                size_payload = {"domain_code": domain_code, **filters}
                size_info = await get_datasize(client, size_payload, lang=lang)
                count = size_info.get("count", size_info.get("size", "unknown"))
                console.print(f"  [dim]Estimated rows: {count}[/dim]")
            except Exception:
                pass  # datasize is best-effort

        raw = await spin_while(
            get_data(client, domain_code, lang=lang, **filters),
            label=f"Fetching [bold]{domain_code}[/bold]",
            console=console,
        )

        # Log response structure for debugging
        if isinstance(raw, dict):
            logger.debug(
                "Response for %s: type=dict, keys=%s, first_500=%s",
                domain_code, list(raw.keys()), str(raw)[:500],
            )
        elif isinstance(raw, list):
            logger.debug(
                "Response for %s: type=list, length=%d, first_item=%s",
                domain_code, len(raw), str(raw[0])[:300] if raw else "(empty)",
            )
        else:
            logger.debug("Response for %s: type=%s, value=%s", domain_code, type(raw).__name__, str(raw)[:500])

        # Validate the API response contains data
        if not _response_has_data(raw):
            console.print(
                f"  [yellow]⚠ API returned no data for domain '{domain_code}'.[/yellow]"
            )
            if isinstance(raw, dict):
                # Show all available info from the response
                console.print(f"  [dim]Response keys: {list(raw.keys())}[/dim]")
                status = raw.get("status_code") or raw.get("status", "")
                message = raw.get("message") or raw.get("description", "") or raw.get("text", "")
                if status:
                    console.print(f"  [dim]Status : {status}[/dim]")
                if message:
                    console.print(f"  [dim]Message: {str(message)[:300]}[/dim]")
            raise ValueError(
                f"No records returned for domain '{domain_code}'. "
                "The domain may be empty, or your filters may be too restrictive. "
                "Use -v flag for full debug output."
            )

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
            transient=True,
        ) as progress:
            task = progress.add_task(f"Cleaning [bold]{domain_code}[/bold]...", total=None)
            df = clean_data(raw)

            if df.empty:
                progress.stop()
                console.print(f"  [yellow]⚠ Data cleaned to empty DataFrame for '{domain_code}'.[/yellow]")
                if isinstance(raw, dict):
                    console.print(f"  [dim]Response keys: {list(raw.keys())}[/dim]")
                    console.print(f"  [dim]Raw preview: {str(raw)[:500]}[/dim]")
                raise ValueError(
                    f"No usable records after cleaning for domain '{domain_code}'. "
                    "The API response format may not match expected structure. "
                    "Use -v flag for full debug output."
                )

            progress.update(task, description=f"Exporting [bold]{domain_code}[/bold]...")
            written = export(
                df,
                domain_code=domain_code,
                output_dir=output_dir,
                to_csv=to_csv,
                to_parquet=to_parquet,
                parquet_compression=parquet_compression,
            )

    return written


async def run_pipeline_batch(
    domain_codes: list[str],
    lang: str = "en",
    output_dir: str | Path = "./output",
    to_csv: bool = True,
    to_parquet: bool = True,
    parquet_compression: str = "snappy",
    filters: dict[str, Any] | None = None,
) -> dict[str, dict[str, Path]]:
    """
    Sequentially run the pipeline for multiple domains.
    (Sequential to respect the 2 req/s rate limit.)

    Returns:
        Dict of {domain_code: {format: path}} for all successful domains.
    """
    results: dict[str, dict[str, Path]] = {}
    for code in domain_codes:
        console.print(f"\n[cyan]→ Processing domain:[/cyan] [bold]{code}[/bold]")
        try:
            written = await run_pipeline(
                domain_code=code,
                lang=lang,
                output_dir=output_dir,
                to_csv=to_csv,
                to_parquet=to_parquet,
                parquet_compression=parquet_compression,
                filters=filters,
            )
            results[code] = written
            for fmt, path in written.items():
                console.print(f"  [green]✓[/green] {fmt.upper()}: {path}")
        except Exception as e:
            console.print(f"  [red]✗ Failed:[/red] {e}")

    return results
