from __future__ import annotations

from typing import Iterable, List, Tuple

from .avanza_client import get_avanza_client, get_avanza_constants, load_avanza_config


def _normalize_hit(hit: dict) -> dict:
    instrument_id = (
        hit.get("orderBookId")
        or hit.get("orderbookId")
        or hit.get("id")
        or hit.get("instrumentId")
        or hit.get("isin")
        or hit.get("symbol")
        or hit.get("tickerSymbol")
    )
    return {
        "instrument_id": str(instrument_id) if instrument_id is not None else "",
        "name": hit.get("name") or hit.get("instrumentName") or hit.get("shortName") or "",
        "isin": hit.get("isin") or "",
        "ticker": hit.get("tickerSymbol") or hit.get("symbol") or "",
        "instrument_type": hit.get("instrumentType") or hit.get("type") or "",
        "provider": "avanza",
        "raw": hit,
    }


def search_avanza(
    query: str,
    types: Iterable[str] | None = None,
    limit: int | None = None,
) -> Tuple[List[dict], str | None]:
    cfg = load_avanza_config()
    client, reason = get_avanza_client()
    if client is None:
        return [], reason

    constants = get_avanza_constants()
    if constants is None:
        return [], "constants-missing"
    TimePeriod, Resolution, InstrumentType = constants

    if limit is None:
        limit = cfg.search_limit

    type_map = {
        "stock": InstrumentType.STOCK,
        "fund": InstrumentType.FUND,
        "bond": InstrumentType.BOND,
        "certificate": InstrumentType.CERTIFICATE,
        "warrant": InstrumentType.WARRANT,
        "etf": InstrumentType.EXCHANGE_TRADED_FUND,
    }

    instrument_types = [type_map["stock"], type_map["fund"]]
    if types:
        instrument_types = []
        for entry in types:
            key = str(entry).lower()
            mapped = type_map.get(key)
            if mapped is not None:
                instrument_types.append(mapped)

    hits: List[dict] = []
    for instrument_type in instrument_types:
        try:
            results = client.search_for_instrument(instrument_type, query, limit)
            if isinstance(results, list):
                hits.extend(results)
        except Exception:
            continue

    normalized = [_normalize_hit(hit) for hit in hits if isinstance(hit, dict)]
    return normalized, None
