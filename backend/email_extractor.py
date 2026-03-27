import re
from dataclasses import dataclass
from html import unescape
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import httpx
import requests
from bs4 import BeautifulSoup, FeatureNotFound

# Reduce noise when scraping sites with broken TLS (we intentionally allow verify=False)
try:  # pragma: no cover
    import warnings
    from urllib3.exceptions import InsecureRequestWarning

    warnings.filterwarnings("ignore", category=InsecureRequestWarning)
except Exception:
    pass

try:
    from selectolax.parser import HTMLParser
except Exception:  # pragma: no cover
    HTMLParser = None

try:
    import orjson as _json

    def _loads_json(s: str):
        return _json.loads(s)
except Exception:  # pragma: no cover
    import json as _json

    def _loads_json(s: str):
        return _json.loads(s)


EMAIL_PATTERN = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-z]{2,}", re.IGNORECASE)
# Common obfuscations: name (at) domain (dot) tld
OBFUSCATED_EMAIL_PATTERN = re.compile(
    r"([a-zA-Z0-9._%+-]{1,64})\s*(?:\(|\[)?\s*(?:at|\@)\s*(?:\)|\])?\s*"
    r"([a-zA-Z0-9.-]{1,253})\s*(?:\(|\[)?\s*(?:dot|\.)\s*(?:\)|\])?\s*"
    r"([a-zA-Z]{2,24})",
    re.IGNORECASE,
)

WHATSAPP_LINK_PATTERN = re.compile(r"(?:wa\.me/|phone=)(\+?\d{6,15})", re.IGNORECASE)
WHATSAPP_REF_PATTERN = re.compile(
    r"(?:https?:\\?/\\?/(?:wa\.me|api\.whatsapp\.com|chat\.whatsapp\.com)[^\"'\s<]*)|(?:whatsapp:\\\?/\\?/send\?[^\"'\s<]*)",
    re.IGNORECASE,
)
GENERIC_PHONE_PATTERN = re.compile(r"\+?\d[\d\s().-]{6,}\d")
DIGIT_PATTERN = re.compile(r"\d+")

# Prioritized internal pages and keywords for deep enrichment
DEFAULT_PATHS = [
    "",
    "/contact",
    "/contact-us",
    "/contactus",
    "/about",
    "/about-us",
    "/aboutus",
    "/team",
    "/support",
    "/help",
    "/privacy",
    "/terms",
    "/legal",
    "/impressum",
]

PRIORITY_LINK_KEYWORDS = (
    "contact",
    "about",
    "team",
    "support",
    "help",
    "privacy",
    "terms",
    "legal",
    "impressum",
    "whatsapp",
)


@dataclass
class CrawledPage:
    url: str
    html: str


class WebsiteExtractor:
    """Fast website enrichment with bounded deep crawling.

    Backwards compatible: keep enrich() output keys stable.
    """

    def __init__(self, timeout: int = 12) -> None:
        self.timeout = timeout

        self._requests_session = requests.Session()
        self._requests_session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            }
        )

        self._httpx = httpx.Client(
            http2=True,
            follow_redirects=True,
            timeout=httpx.Timeout(self.timeout, connect=min(8, self.timeout)),
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            verify=False,
        )

        self._html_cache: Dict[str, str] = {}
        self._cache_fifo: List[str] = []
        self._max_cache_entries = 64

    def enrich(self, website_url: str, fallback_phone: str = "") -> Dict[str, str]:
        """Return {email, whatsapp} by crawling a small set of internal pages."""
        if not website_url:
            return {"email": "", "whatsapp": self._normalize_phone(fallback_phone)}

        normalized = self._normalize_url(website_url)
        if not normalized:
            return {"email": "", "whatsapp": self._normalize_phone(fallback_phone)}

        pages = self.crawl_pages(normalized, max_pages=8)
        if not pages:
            return {"email": "", "whatsapp": self._normalize_phone(fallback_phone)}

        emails: List[str] = []
        whatsapp_numbers: List[str] = []

        for page in pages:
            page_emails = self._extract_emails(page.html)
            page_whatsapp = self._extract_whatsapp_numbers(page.html)

            for email in page_emails:
                if email and email not in emails:
                    emails.append(email)
            for number in page_whatsapp:
                if number and number not in whatsapp_numbers:
                    whatsapp_numbers.append(number)

            # If we already have both, stop early.
            if emails and whatsapp_numbers:
                break

        whatsapp = whatsapp_numbers[0] if whatsapp_numbers else self._normalize_phone(fallback_phone)
        email = emails[0] if emails else ""
        return {"email": email, "whatsapp": whatsapp}

    def crawl_pages(self, website_url: str, max_pages: int = 8, max_bytes_per_page: int = 1_500_000) -> List[CrawledPage]:
        """Crawl a small, bounded set of internal pages and return HTML corpus."""
        base = self._normalize_url(website_url)
        if not base:
            return []

        to_visit: List[str] = []
        seen: Set[str] = set()

        # Seed with common paths
        for p in DEFAULT_PATHS:
            to_visit.append(urljoin(base + "/", p.lstrip("/")))

        # Also try robots/sitemap for hints (best-effort)
        sitemap_hint_urls: List[str] = []
        robots = self._safe_get_text(urljoin(base + "/", "robots.txt"), max_bytes=200_000)
        if robots:
            for line in robots.splitlines():
                if line.lower().startswith("sitemap:"):
                    u = line.split(":", 1)[1].strip()
                    if u.startswith("http"):
                        sitemap_hint_urls.append(u)
        sitemap_hint_urls.append(urljoin(base + "/", "sitemap.xml"))

        for sm in sitemap_hint_urls[:2]:
            sm_xml = self._safe_get_text(sm, max_bytes=600_000)
            if not sm_xml:
                continue
            # Cheap extraction: find URLs containing high-value keywords.
            for m in re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", sm_xml, flags=re.I):
                ml = m.lower()
                if any(k in ml for k in PRIORITY_LINK_KEYWORDS):
                    to_visit.append(m.strip())

        crawled: List[CrawledPage] = []

        while to_visit and len(crawled) < max_pages:
            url = (to_visit.pop(0) or "").strip()
            if not url:
                continue
            norm_url = self._normalize_full_url(url)
            if not norm_url or norm_url in seen:
                continue
            if not self._is_same_site(base, norm_url):
                continue

            seen.add(norm_url)
            html = self._safe_get_html(norm_url, max_bytes=max_bytes_per_page)
            if not html:
                continue

            crawled.append(CrawledPage(url=norm_url, html=html))

            # Discover more internal candidate links from the first 1-2 pages
            if len(crawled) <= 2 and len(crawled) < max_pages:
                for link in self._discover_priority_links(html, base):
                    if link not in seen and link not in to_visit:
                        to_visit.append(link)

        return crawled

    def _normalize_url(self, url: str) -> str:
        url = (url or "").strip()
        if not url:
            return ""
        if not url.startswith(("http://", "https://")):
            url = f"https://{url}"
        parsed = urlparse(url)
        if not parsed.netloc:
            return ""
        return f"{parsed.scheme}://{parsed.netloc}"

    def _normalize_full_url(self, url: str) -> str:
        u = (url or "").strip()
        if not u:
            return ""
        if u.startswith("//"):
            u = "https:" + u
        if not u.startswith(("http://", "https://")):
            u = "https://" + u.lstrip("/")
        parsed = urlparse(u)
        if not parsed.scheme or not parsed.netloc:
            return ""
        # Remove fragments; keep query.
        parsed = parsed._replace(fragment="")
        return parsed.geturl()

    def _is_same_site(self, base: str, url: str) -> bool:
        try:
            b = urlparse(base)
            u = urlparse(url)
            return (b.netloc or "").lower() == (u.netloc or "").lower()
        except Exception:
            return False

    def _cache_put(self, url: str, html: str) -> None:
        if url in self._html_cache:
            return
        self._html_cache[url] = html
        self._cache_fifo.append(url)
        if len(self._cache_fifo) > self._max_cache_entries:
            old = self._cache_fifo.pop(0)
            self._html_cache.pop(old, None)

    def _safe_get_text(self, url: str, max_bytes: int = 600_000) -> str:
        return self._safe_get_html(url, max_bytes=max_bytes)

    def _safe_get_html(self, url: str, max_bytes: int = 1_500_000) -> str:
        url = (url or "").strip()
        if not url:
            return ""
        if url in self._html_cache:
            return self._html_cache[url]

        # httpx first (faster, HTTP/2), then requests fallback
        text = ""
        try:
            r = self._httpx.get(url)
            if r.status_code < 400:
                content = r.content or b""
                if max_bytes and len(content) > max_bytes:
                    content = content[:max_bytes]
                text = content.decode(r.encoding or "utf-8", errors="ignore")
        except Exception:
            text = ""

        if not text:
            try:
                r2 = self._requests_session.get(url, timeout=self.timeout, verify=False, allow_redirects=True)
                if r2.status_code < 400:
                    raw = (r2.content or b"")
                    if max_bytes and len(raw) > max_bytes:
                        raw = raw[:max_bytes]
                    text = raw.decode(r2.encoding or "utf-8", errors="ignore")
            except requests.RequestException:
                text = ""

        if text:
            self._cache_put(url, text)
        return text

    def _discover_priority_links(self, html: str, base: str) -> List[str]:
        candidates: List[str] = []
        if not html:
            return candidates

        def consider(href: str, anchor_text: str = "") -> None:
            href = (href or "").strip()
            if not href or href.startswith(("mailto:", "tel:", "javascript:")):
                return
            href_l = href.lower()
            text_l = (anchor_text or "").lower()
            if any(k in href_l for k in PRIORITY_LINK_KEYWORDS) or any(k in text_l for k in PRIORITY_LINK_KEYWORDS):
                u = urljoin(base + "/", href)
                u = self._normalize_full_url(u)
                if u and self._is_same_site(base, u):
                    candidates.append(u)

        if HTMLParser is not None:
            try:
                tree = HTMLParser(html)
                for a in tree.css("a"):
                    consider(a.attributes.get("href", ""), a.text())
            except Exception:
                pass

        if not candidates:
            try:
                soup = BeautifulSoup(html, "lxml")
            except FeatureNotFound:
                soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                consider(a.get("href", ""), a.get_text(" ", strip=True))

        # De-dupe preserving order
        return list(dict.fromkeys(candidates))[:12]

    def _extract_emails(self, html: str) -> List[str]:
        if not html:
            return []
        raw = unescape(html)

        matches = EMAIL_PATTERN.findall(raw)
        # Also attempt to parse obfuscated emails
        for m in OBFUSCATED_EMAIL_PATTERN.findall(raw):
            try:
                local, domain, tld = m
                candidate = f"{local}@{domain}.{tld}"
                if EMAIL_PATTERN.fullmatch(candidate):
                    matches.append(candidate)
            except Exception:
                continue

        # JSON-LD can include email fields
        for email in self._extract_emails_from_jsonld(raw):
            matches.append(email)

        deduped = list(dict.fromkeys([m.lower() for m in matches if m]))
        return deduped

    def _extract_emails_from_jsonld(self, html: str) -> List[str]:
        emails: List[str] = []
        if not html:
            return emails
        try:
            # Cheaply slice scripts; avoid full DOM when possible
            for script in re.findall(
                r"<script[^>]+type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
                html,
                flags=re.I | re.S,
            ):
                s = (script or "").strip()
                if not s:
                    continue
                try:
                    data = _loads_json(s)
                except Exception:
                    continue
                for e in self._walk_json_for_key(data, "email"):
                    if isinstance(e, str):
                        for m in EMAIL_PATTERN.findall(e):
                            emails.append(m)
        except Exception:
            return emails

        return list(dict.fromkeys([e.lower() for e in emails if e]))

    def _walk_json_for_key(self, obj, key: str) -> Iterable:
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == key:
                    yield v
                yield from self._walk_json_for_key(v, key)
        elif isinstance(obj, list):
            for it in obj:
                yield from self._walk_json_for_key(it, key)

    def _extract_whatsapp_numbers(self, html: str) -> List[str]:
        try:
            soup = BeautifulSoup(html, "lxml")
        except FeatureNotFound:
            soup = BeautifulSoup(html, "html.parser")
        found: List[str] = []
        markers = ["wa.me", "api.whatsapp.com", "whatsapp://", "chat.whatsapp.com", "wa.link"]

        # Primary extraction from WhatsApp link formats including short links.
        for anchor in soup.find_all("a", href=True):
            href = anchor.get("href", "")
            if not href:
                continue
            lower_href = href.lower()
            if not any(marker in lower_href for marker in markers):
                continue

            link_numbers = self._extract_numbers_from_whatsapp_ref(href)
            for number in link_numbers:
                if number and number not in found:
                    found.append(number)

            # wa.link often redirects to wa.me or api.whatsapp.com with phone in query.
            if "wa.link" in lower_href and not link_numbers:
                resolved = self._resolve_short_whatsapp_link(href)
                if resolved:
                    for number in self._extract_numbers_from_whatsapp_ref(resolved):
                        if number and number not in found:
                            found.append(number)

        # Fallback: check raw HTML for WhatsApp markers and nearby phone candidates.
        lower_html = html.lower()
        if not found and any(marker in lower_html for marker in markers):
            for match in WHATSAPP_LINK_PATTERN.findall(html):
                number = self._normalize_phone(match)
                if number and number not in found:
                    found.append(number)

            if not found:
                for match in GENERIC_PHONE_PATTERN.findall(html):
                    number = self._normalize_phone(match)
                    if 8 <= len(number.replace("+", "")) <= 15 and number not in found:
                        found.append(number)

        # Extra fallback: many sites keep WhatsApp links in inline script strings.
        if not found:
            for script in soup.find_all("script"):
                script_text = script.get_text(" ", strip=True)
                if not script_text:
                    continue
                if not any(marker in script_text.lower() for marker in markers):
                    continue

                normalized_script = script_text.replace("\\/", "/")
                for ref in WHATSAPP_REF_PATTERN.findall(normalized_script):
                    for number in self._extract_numbers_from_whatsapp_ref(ref):
                        if number and number not in found:
                            found.append(number)

                if not found:
                    for match in WHATSAPP_LINK_PATTERN.findall(normalized_script):
                        number = self._normalize_phone(match)
                        if number and number not in found:
                            found.append(number)

        deduped: List[str] = []
        seen_canonical = set()
        for number in found:
            if not number:
                continue
            canonical = self._canonical_phone(number)
            if canonical and canonical not in seen_canonical:
                seen_canonical.add(canonical)
                deduped.append(number)
        return deduped

    def _extract_numbers_from_whatsapp_ref(self, href: str) -> List[str]:
        decoded = unquote((href or "").strip())
        if not decoded:
            return []

        numbers: List[str] = []
        seen_canonical = set()

        # Direct path style, e.g. wa.me/923001234567
        for match in WHATSAPP_LINK_PATTERN.findall(decoded):
            normalized = self._normalize_phone(match)
            canonical = self._canonical_phone(normalized)
            if normalized and canonical and canonical not in seen_canonical:
                seen_canonical.add(canonical)
                numbers.append(normalized)

        # Query parameter style, e.g. ?phone=923001234567
        try:
            parsed = urlparse(decoded)
            query_values = parse_qs(parsed.query)
            for key in ["phone", "phonenumber", "number"]:
                for value in query_values.get(key, []):
                    normalized = self._normalize_phone(value)
                    canonical = self._canonical_phone(normalized)
                    if normalized and canonical and canonical not in seen_canonical:
                        seen_canonical.add(canonical)
                        numbers.append(normalized)
        except Exception:
            pass

        # Last fallback for href containing mixed symbols.
        if not numbers:
            maybe = self._extract_digits(decoded)
            if maybe:
                numbers.append(maybe)

        return numbers

    def _resolve_short_whatsapp_link(self, href: str) -> str:
        url = (href or "").strip()
        if not url:
            return ""
        if not url.startswith(("http://", "https://")):
            url = f"https://{url.lstrip('/')}"
        try:
            response = self._requests_session.get(url, timeout=8, allow_redirects=True, verify=False)
            return response.url or ""
        except requests.RequestException:
            return ""

    def _extract_digits(self, text: str) -> str:
        digits = "".join(DIGIT_PATTERN.findall(text))
        if len(digits) < 6:
            return ""
        return digits

    def _normalize_phone(self, phone: str) -> str:
        phone = (phone or "").strip()
        if not phone:
            return ""
        cleaned = phone.replace(" ", "").replace("-", "")
        if cleaned.startswith("+"):
            return "+" + "".join(DIGIT_PATTERN.findall(cleaned))
        digits = "".join(DIGIT_PATTERN.findall(cleaned))
        return digits

    def _canonical_phone(self, phone: str) -> str:
        return "".join(DIGIT_PATTERN.findall(phone or ""))


def crawl_site_pages(website_url: str, timeout: int = 12, max_pages: int = 8) -> List[Tuple[str, str]]:
    """Convenience helper for other modules.

    Returns list of (url, html) tuples.
    """
    extractor = WebsiteExtractor(timeout=timeout)
    pages = extractor.crawl_pages(website_url, max_pages=max_pages)
    return [(p.url, p.html) for p in pages]
