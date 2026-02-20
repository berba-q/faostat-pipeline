"""
Typed wrappers for every FAOSTAT API endpoint.
All functions accept a FAOSTATClient and return parsed JSON (dict or list).
"""

from typing import Any

from .client import FAOSTATClient


async def ping(client: FAOSTATClient) -> dict[str, Any]:
    """GET /ping — Check API health."""
    return await client.get("/ping")


async def get_groups(client: FAOSTATClient, lang: str = "en") -> list[dict]:
    """GET /{lang}/groups/ — List all top-level data groups."""
    return await client.get(f"/{lang}/groups/")


async def get_groups_and_domains(client: FAOSTATClient, lang: str = "en") -> dict[str, Any]:
    """GET /{lang}/groupsanddomains — Full hierarchical tree of groups and domains."""
    return await client.get(f"/{lang}/groupsanddomains")


async def get_domains(client: FAOSTATClient, group_code: str, lang: str = "en") -> list[dict]:
    """GET /{lang}/domains/{group_code}/ — List domains within a group."""
    return await client.get(f"/{lang}/domains/{group_code}/")


async def get_dimensions(client: FAOSTATClient, domain_code: str, lang: str = "en") -> dict[str, Any]:
    """GET /{lang}/dimensions/{domain_code}/ — Domain structure (dimensions/filters available)."""
    return await client.get(f"/{lang}/dimensions/{domain_code}/")


async def get_codes(
    client: FAOSTATClient,
    dimension_id: str,
    domain_code: str,
    lang: str = "en",
) -> list[dict]:
    """GET /{lang}/codes/{dimension_id}/{domain_code} — Available codes for a dimension."""
    return await client.get(f"/{lang}/codes/{dimension_id}/{domain_code}")


async def get_data(
    client: FAOSTATClient,
    domain_code: str,
    lang: str = "en",
    area: str | None = None,
    element: str | None = None,
    item: str | None = None,
    year: str | None = None,
    area_cs: str | None = None,
    element_cs: str | None = None,
    item_cs: str | None = None,
    year_cs: str | None = None,
    show_codes: bool = True,
    show_unit: bool = True,
    show_flags: bool = True,
    null_values: bool = False,
    output_type: str = "objects",
) -> dict[str, Any]:
    """
    GET /{lang}/data/{domain_code} — Fetch actual statistical data.

    Filter parameters (comma-separated codes for multiple values):
      area       : Country/area codes (e.g. "231" for USA, "79" for Ethiopia)
      element    : Element codes (e.g. "5510" for Production)
      item       : Item codes (e.g. "56" for Maize)
      year       : Year codes (e.g. "2020" or "2018,2019,2020")
      *_cs       : Use code sets instead of individual codes (area_cs, element_cs, etc.)
    """
    params: dict[str, Any] = {
        "show_codes": show_codes,
        "show_unit": show_unit,
        "show_flags": show_flags,
        "null_values": null_values,
        "output_type": output_type,
    }
    for key, val in [("area", area), ("element", element), ("item", item), ("year", year),
                     ("area_cs", area_cs), ("element_cs", element_cs),
                     ("item_cs", item_cs), ("year_cs", year_cs)]:
        if val is not None:
            params[key] = val

    return await client.get(f"/{lang}/data/{domain_code}", params=params)


async def get_datasize(
    client: FAOSTATClient,
    payload: dict[str, Any],
    lang: str = "en",
) -> dict[str, Any]:
    """POST /{lang}/datasize/ — Estimate the number of rows a query will return."""
    return await client.post(f"/{lang}/datasize/", json=payload)


async def get_definitions(
    client: FAOSTATClient,
    domain_code: str,
    lang: str = "en",
) -> dict[str, Any]:
    """GET /{lang}/definitions/domain/{domain_code} — Definitions for a domain."""
    return await client.get(f"/{lang}/definitions/domain/{domain_code}")


async def get_definitions_by_type(
    client: FAOSTATClient,
    domain_code: str,
    definition_type: str,
    lang: str = "en",
) -> dict[str, Any]:
    """GET /{lang}/definitions/domain/{domain_code}/{type} — Definitions filtered by type."""
    return await client.get(f"/{lang}/definitions/domain/{domain_code}/{definition_type}")


async def get_definition_types(client: FAOSTATClient, lang: str = "en") -> list[dict]:
    """GET /{lang}/definitions/types — All available definition types."""
    return await client.get(f"/{lang}/definitions/types")


async def get_metadata(
    client: FAOSTATClient,
    domain_code: str,
    lang: str = "en",
) -> dict[str, Any]:
    """GET /{lang}/metadata/{domain_code} — Full metadata for a domain."""
    return await client.get(f"/{lang}/metadata/{domain_code}")


async def get_metadata_print(
    client: FAOSTATClient,
    domain_code: str,
    lang: str = "en",
) -> dict[str, Any]:
    """GET /{lang}/metadata_print/{domain_code} — Printable metadata for a domain."""
    return await client.get(f"/{lang}/metadata_print/{domain_code}")


async def get_bulk_downloads(
    client: FAOSTATClient,
    domain_code: str,
    lang: str = "en",
) -> list[dict]:
    """GET /{lang}/bulkdownloads/{domain_code}/ — List available bulk download files."""
    return await client.get(f"/{lang}/bulkdownloads/{domain_code}/")


async def get_documents(
    client: FAOSTATClient,
    domain_code: str,
    lang: str = "en",
) -> list[dict]:
    """GET /{lang}/documents/{domain_code}/ — List related documents for a domain."""
    return await client.get(f"/{lang}/documents/{domain_code}/")


async def get_rankings(
    client: FAOSTATClient,
    payload: dict[str, Any],
    lang: str = "en",
) -> dict[str, Any]:
    """POST /{lang}/rankings/ — Get rankings data."""
    return await client.post(f"/{lang}/rankings/", json=payload)


async def get_report_data(
    client: FAOSTATClient,
    payload: dict[str, Any],
    lang: str = "en",
) -> dict[str, Any]:
    """POST /{lang}/report/data/ — Get report data."""
    return await client.post(f"/{lang}/report/data/", json=payload)


async def get_report_headers(
    client: FAOSTATClient,
    payload: dict[str, Any],
    lang: str = "en",
) -> dict[str, Any]:
    """POST /{lang}/report/headers/ — Get report headers."""
    return await client.post(f"/{lang}/report/headers/", json=payload)
