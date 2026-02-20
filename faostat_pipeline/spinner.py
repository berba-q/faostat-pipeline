"""
Fun rotating spinner for CLI feedback while waiting on API calls.
"""

import asyncio
import contextlib
import random
from typing import Any, Awaitable, TypeVar

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

T = TypeVar("T")

_MESSAGES: tuple[str, ...] = (
    "Ploughing through the data fields...",
    "Waiting for the crops to come in...",
    "Herding the bytes back to the barn...",
    "Milking the API for all it's worth...",
    "Sowing the query seeds...",
    "Checking if the data is ripe yet...",
    "Separating the wheat from the chaff...",
    "Irrigating the data pipeline...",
    "Harvesting rows from the server farm...",
    "Feeding the chickens while we wait...",
    "Tilling the database soil...",
    "The data is still marinating...",
    "Counting sheep... and cattle... and goats...",
    "Consulting the Farmers' Almanac...",
    "Fertilizing the query parameters...",
    "Rotating the crops, rotating the messages...",
    "Putting the data out to pasture...",
    "Shearing the response payload...",
)


async def _rotate_status(
    progress: Progress,
    task_id: Any,
    label: str,
    interval: float,
) -> None:
    """Cycle through fun status messages in the background."""
    messages = list(_MESSAGES)
    random.shuffle(messages)
    idx = 0
    try:
        while True:
            msg = messages[idx % len(messages)]
            progress.update(
                task_id,
                description=f"{label} \u2014 [dim]{msg}[/dim]",
            )
            idx += 1
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        return


async def spin_while(
    coro: Awaitable[T],
    label: str = "Working",
    console: Console | None = None,
    interval: float = 2.0,
) -> T:
    """
    Run an awaitable with a fun rotating spinner.

    Args:
        coro: The awaitable (e.g. an API call) to execute.
        label: Static prefix shown before the rotating message.
        console: Rich Console instance (creates one if not provided).
        interval: Seconds between message rotations.

    Returns:
        The result of the awaitable.
    """
    console = console or Console()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task(f"{label}...", total=None)
        rotator = asyncio.create_task(
            _rotate_status(progress, task, label, interval)
        )
        try:
            result = await coro
        finally:
            rotator.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await rotator

    return result
