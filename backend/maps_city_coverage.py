import re
from typing import List

CITY_AREA_HINTS = {
    "karachi": [
        "Saddar Karachi",
        "Clifton Karachi",
        "DHA Karachi",
        "Gulshan e Iqbal Karachi",
        "Gulistan e Johar Karachi",
        "North Nazimabad Karachi",
        "Korangi Karachi",
        "Malir Karachi",
        "PECHS Karachi",
    ],
    "lahore": [
        "Gulberg Lahore",
        "DHA Lahore",
        "Johar Town Lahore",
        "Model Town Lahore",
        "Bahria Town Lahore",
        "Cantt Lahore",
    ],
    "islamabad": [
        "Blue Area Islamabad",
        "F 6 Islamabad",
        "F 7 Islamabad",
        "F 10 Islamabad",
        "G 9 Islamabad",
        "G 11 Islamabad",
        "Bahria Town Islamabad",
    ],
    "rawalpindi": [
        "Saddar Rawalpindi",
        "Bahria Town Rawalpindi",
        "DHA Rawalpindi",
        "Satellite Town Rawalpindi",
        "Chaklala Rawalpindi",
    ],
    "faisalabad": [
        "D Ground Faisalabad",
        "Peoples Colony Faisalabad",
        "Madina Town Faisalabad",
    ],
    "peshawar": [
        "University Town Peshawar",
        "Hayatabad Peshawar",
        "Saddar Peshawar",
    ],
    "quetta": [
        "Jinnah Road Quetta",
        "Satellite Town Quetta",
        "Samungli Road Quetta",
    ],
    "multan": [
        "Cantt Multan",
        "Gulgasht Colony Multan",
        "Bosan Road Multan",
    ],
}

GENERIC_DIRECTIONAL_HINTS = [
    "city center",
    "north side",
    "south side",
    "east side",
    "west side",
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


def build_citywide_queries(keyword: str, location: str, max_queries: int = 7) -> List[str]:
    """Build multiple Maps search queries to improve full-city coverage."""
    cleaned_keyword = re.sub(r"\s+", " ", (keyword or "").strip())
    cleaned_location = re.sub(r"\s+", " ", (location or "").strip())

    if not cleaned_keyword or not cleaned_location:
        return []

    max_queries = max(1, min(max_queries, 12))
    queries: List[str] = []
    seen_keys = set()

    base_query = f"{cleaned_keyword} in {cleaned_location}".strip()
    _append_query(queries, seen_keys, base_query, max_queries)

    normalized_location = _normalize_text(cleaned_location)
    area_hints: List[str] = []

    for city_name, hints in CITY_AREA_HINTS.items():
        if city_name in normalized_location:
            area_hints.extend(hints)

    if not area_hints:
        area_hints.extend([f"{hint} {cleaned_location}" for hint in GENERIC_DIRECTIONAL_HINTS])

    for area in area_hints:
        query = f"{cleaned_keyword} in {area}".strip()
        _append_query(queries, seen_keys, query, max_queries)

    if len(queries) < max_queries:
        _append_query(queries, seen_keys, f"{cleaned_keyword} near {cleaned_location}", max_queries)

    return queries
