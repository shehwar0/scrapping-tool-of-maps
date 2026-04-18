from urllib.parse import urlparse

# Domains that are social/contact platforms, not a business website.
NON_BUSINESS_DOMAINS = (
    "instagram.com",
    "facebook.com",
    "fb.com",
    "twitter.com",
    "x.com",
    "linkedin.com",
    "tiktok.com",
    "youtube.com",
    "youtu.be",
    "wa.me",
    "whatsapp.com",
    "api.whatsapp.com",
    "telegram.me",
    "t.me",
    "linktr.ee",
    "beacons.ai",
    "bio.site",
)


def _normalize_host(url: str) -> str:
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower().split(":", 1)[0].strip()
    if host.startswith("www."):
        host = host[4:]
    return host


def is_business_website(url: str) -> bool:
    if not url:
        return False

    cleaned = (url or "").strip()
    if not cleaned.startswith(("http://", "https://")):
        return False

    host = _normalize_host(cleaned)
    if not host:
        return False

    for domain in NON_BUSINESS_DOMAINS:
        if host == domain or host.endswith(f".{domain}"):
            return False

    return True


def normalize_business_website(url: str) -> str:
    cleaned = (url or "").strip()
    if not cleaned:
        return ""
    return cleaned if is_business_website(cleaned) else ""
