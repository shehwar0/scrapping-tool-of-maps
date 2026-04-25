import re
from typing import List

GLOBAL_AREA_PATTERNS = [
    "Downtown {city}",
    "{city} City Center",
    "Central {city}",
    "{city} Old Town",
    "{city} Business District",
    "{city} Financial District",
    "{city} Midtown",
    "{city} Uptown",
    "{city} North",
    "{city} South",
    "{city} East",
    "{city} West",
    "{city} Suburbs",
    "{city} Metropolitan Area",
    "Greater {city}",
    "{city} Nearby",
    "{city} Surrounding Areas",
    "{city} Suburban Area",
    "{city} Outer Area",
    "{city} Industrial Area",
    "{city} Market Area",
    "{city} Residential Area",
    "{city} Local Area",
    "{city} District Area",
]

GLOBAL_DISCOVERY_MODIFIERS = [
    "best",
    "top rated",
    "local",
    "nearby",
    "popular",
    "trusted",
    "open now",
    "nearest",
]

RADIUS_HINTS_KM = [3, 5, 8, 12, 20, 30, 45, 60, 80, 120]

COUNTRY_ALIASES = {
    "usa": "United States",
    "us": "United States",
    "uk": "United Kingdom",
    "uae": "United Arab Emirates",
    "ksa": "Saudi Arabia",
    "sa": "Saudi Arabia",
}

SMALL_TOWN_HINTS = [
    "surrounding areas",
    "nearby villages",
    "adjacent towns",
    "nearby neighborhoods",
    "local market area",
]


def _normalize_text(text: str) -> str:
    lowered = (text or "").lower()
    lowered = re.sub(r"[^a-z0-9\s]+", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def _append_query(queries: List[str], seen_keys: set, query: str, max_queries: int) -> None:
    if len(queries) >= max_queries:
        return
    cleaned = re.sub(r"\s+", " ", query).strip()
    if not cleaned:
        return
    key = _normalize_text(cleaned)
    if key and key not in seen_keys:
        seen_keys.add(key)
        queries.append(cleaned)


def _split_location_parts(location: str) -> List[str]:
    parts = [re.sub(r"\s+", " ", p).strip() for p in (location or "").split(",")]
    return [p for p in parts if p]


def _normalize_country_aliases(location: str) -> str:
    cleaned = re.sub(r"\s+", " ", (location or "").strip())
    if not cleaned:
        return ""

    parts = _split_location_parts(cleaned)
    if parts:
        normalized_parts = []
        for part in parts:
            lower = part.lower()
            normalized_parts.append(COUNTRY_ALIASES.get(lower, part))
        return ", ".join(normalized_parts)

    normalized = cleaned
    for alias, full in COUNTRY_ALIASES.items():
        normalized = re.sub(rf"\b{re.escape(alias)}\b", full, normalized, flags=re.I)
    return normalized


def _extract_city_anchor(location: str) -> str:
    parts = _split_location_parts(location)
    city = parts[0] if parts else re.sub(r"\s+", " ", (location or "").strip())
    city = re.sub(r"\b(city|town|district|province|state|region|county|municipality)\b", "", city, flags=re.I)
    city = re.sub(r"\s+", " ", city).strip(" ,")
    return city


def _looks_like_small_town(location: str) -> bool:
    normalized = _normalize_text(location)
    if not normalized:
        return False

    small_terms = {
        "town",
        "village",
        "tehsil",
        "taluka",
        "suburb",
        "locality",
        "district",
        "municipality",
    }

    if any(term in normalized.split() for term in small_terms):
        return True

    first_part = _split_location_parts(location)
    if first_part and len(first_part[0].split()) <= 2:
        return True

    return False


def _build_location_variants(cleaned_location: str) -> List[str]:
    variants: List[str] = []
    seen = set()

    def _add(value: str) -> None:
        key = _normalize_text(value)
        if value and key and key not in seen:
            seen.add(key)
            variants.append(value)

    _add(cleaned_location)

    parts = _split_location_parts(cleaned_location)
    city_anchor = _extract_city_anchor(cleaned_location)

    if city_anchor:
        _add(city_anchor)

    if len(parts) >= 2:
        region = parts[1]
        country = parts[-1]
        if city_anchor:
            _add(f"{city_anchor}, {region}")
            _add(f"{city_anchor}, {country}")
            _add(f"{city_anchor} {country}")

    if len(parts) >= 3 and city_anchor:
        _add(f"{city_anchor}, {parts[-2]}, {parts[-1]}")

    return variants


def _build_area_hints(cleaned_location: str) -> List[str]:
    area_hints: List[str] = []
    city_anchor = _extract_city_anchor(cleaned_location)

    if not city_anchor:
        return area_hints

    for pattern in GLOBAL_AREA_PATTERNS:
        area_hints.append(pattern.format(city=city_anchor))
    return area_hints


def build_citywide_queries(keyword: str, location: str, max_queries: int = 14) -> List[str]:
    """Build multiple Maps search queries with global-first city coverage."""
    cleaned_keyword = re.sub(r"\s+", " ", (keyword or "").strip())
    cleaned_location = _normalize_country_aliases(location)

    if not cleaned_keyword or not cleaned_location:
        return []

    max_queries = max(1, min(max_queries, 72))
    queries: List[str] = []
    seen_keys = set()

    location_variants = _build_location_variants(cleaned_location)
    area_hints = _build_area_hints(cleaned_location)
    city_anchor = _extract_city_anchor(cleaned_location)
    is_small_town = _looks_like_small_town(cleaned_location)

    if not location_variants:
        return []

    for loc in location_variants[:8]:
        _append_query(queries, seen_keys, f"{cleaned_keyword} in {loc}", max_queries)

    for loc in location_variants[:5]:
        _append_query(queries, seen_keys, f"{cleaned_keyword} near {loc}", max_queries)
        _append_query(queries, seen_keys, f"{cleaned_keyword} around {loc}", max_queries)
        _append_query(queries, seen_keys, f"{cleaned_keyword} close to {loc}", max_queries)
        _append_query(queries, seen_keys, f"{cleaned_keyword} nearby {loc}", max_queries)

        for radius in RADIUS_HINTS_KM:
            _append_query(queries, seen_keys, f"{cleaned_keyword} within {radius} km of {loc}", max_queries)

    for area in area_hints:
        _append_query(queries, seen_keys, f"{cleaned_keyword} in {area}", max_queries)
        _append_query(queries, seen_keys, f"{cleaned_keyword} near {area}", max_queries)

    if is_small_town and city_anchor:
        for hint in SMALL_TOWN_HINTS:
            _append_query(queries, seen_keys, f"{cleaned_keyword} in {city_anchor} {hint}", max_queries)
            _append_query(queries, seen_keys, f"{cleaned_keyword} near {city_anchor} {hint}", max_queries)

    if len(queries) < max_queries:
        _append_query(queries, seen_keys, f"{cleaned_keyword} {cleaned_location}", max_queries)

    primary_loc = location_variants[0]
    for modifier in GLOBAL_DISCOVERY_MODIFIERS:
        _append_query(queries, seen_keys, f"{modifier} {cleaned_keyword} in {primary_loc}", max_queries)
        _append_query(queries, seen_keys, f"{modifier} {cleaned_keyword} near {primary_loc}", max_queries)

    if city_anchor:
        _append_query(queries, seen_keys, f"{cleaned_keyword} around greater {city_anchor}", max_queries)
        _append_query(queries, seen_keys, f"{cleaned_keyword} in {city_anchor} metropolitan area", max_queries)
        _append_query(queries, seen_keys, f"{cleaned_keyword} in and around {city_anchor}", max_queries)
        _append_query(queries, seen_keys, f"{cleaned_keyword} near {city_anchor} neighboring towns", max_queries)

    return queries
