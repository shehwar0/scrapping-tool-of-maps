"""
Deep Business Scraper - Multi-Source Intelligence Extraction
Extracts comprehensive business data from:
1. Google Maps (primary source)
2. Business website (contact pages, about pages, etc.)
3. Google Search (cross-verification and additional info)
4. Social media profiles (Instagram, Facebook, etc.)

Focus on ACCURACY, COMPLETENESS, and CROSS-VERIFICATION.
"""

import logging
import random
import re
import time
from dataclasses import dataclass, field
from threading import Event
from typing import Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Page, BrowserContext
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from email_extractor import WebsiteExtractor
from maps_city_coverage import build_citywide_queries
from url_filters import is_business_website, normalize_business_website


# ============================================================================
# CONFIGURATION
# ============================================================================

MAX_RESULTS_CAP = 500
RESULT_SCAN_WINDOW = 260
CITYWIDE_QUERY_LIMIT = 7
MAP_STAGNANT_ROUNDS = 16
MAP_SCROLL_DELAY_MIN = 0.28
MAP_SCROLL_DELAY_MAX = 0.55
REQUEST_TIMEOUT = 15

# Pages to check for contact info on websites
CONTACT_PAGES = [
    "",
    "/contact",
    "/contact-us",
    "/contactus",
    "/about",
    "/about-us",
    "/aboutus",
    "/team",
    "/reach-us",
    "/get-in-touch",
]

# Social media pages to check
SOCIAL_PAGES = [
    "/social",
    "/follow-us",
    "/connect",
]


# ============================================================================
# REGEX PATTERNS
# ============================================================================

PHONE_REGEX = re.compile(r"(\+?\d[\d\s()\-\.]{6,}\d)")

# WhatsApp patterns - comprehensive
WHATSAPP_PATTERNS = [
    re.compile(r"(?:https?://)?wa\.me/(\+?\d{10,15})", re.I),
    re.compile(r"(?:https?://)?api\.whatsapp\.com/send\?phone=(\+?\d{10,15})", re.I),
    re.compile(r"(?:https?://)?wa\.me/(\d{10,15})", re.I),
    re.compile(r"(?:https?://)?api\.whatsapp\.com/send\?phone=(\d{10,15})", re.I),
    re.compile(r"whatsapp://send\?phone=(\+?\d{10,15})", re.I),
    re.compile(r"href=[\"'](?:https?://)?wa\.me/(\+?\d{10,15})[\"']", re.I),
    re.compile(r"href=[\"'](?:https?://)?wa\.me/(\d{10,15})[\"']", re.I),
    re.compile(r"data-phone[=\"':]+[\"']?(\+?\d{10,15})", re.I),
    re.compile(r"data-whatsapp[=\"':]+[\"']?(\+?\d{10,15})", re.I),
    re.compile(r"whatsapp[\"']?\s*[:=]\s*[\"']?(\+?\d{10,15})", re.I),
]

# Instagram patterns - fixed to properly extract usernames
INSTAGRAM_PATTERNS = [
    re.compile(r"(?:https?://)?(?:www\.)?instagram\.com/([a-zA-Z0-9_\.]{1,30})/?(?:\?|$|#|\"|\s|<)", re.I),
    re.compile(r"href=[\"'](?:https?://)?(?:www\.)?instagram\.com/([a-zA-Z0-9_\.]{1,30})/?[\"']", re.I),
    re.compile(r"instagram\.com/([a-zA-Z0-9_\.]{1,30})(?:/|\?|$|#|\"|\s)", re.I),
]

# Facebook patterns
FACEBOOK_PATTERNS = [
    re.compile(r"(?:https?://)?(?:www\.)?facebook\.com/([a-zA-Z0-9\.]{1,50})/?(?:\?|$|#|\"|\s|<)", re.I),
    re.compile(r"(?:https?://)?(?:www\.)?fb\.com/([a-zA-Z0-9\.]{1,50})/?", re.I),
    re.compile(r"href=[\"'](?:https?://)?(?:www\.)?facebook\.com/([a-zA-Z0-9\.]{1,50})/?[\"']", re.I),
]

# Twitter/X patterns
TWITTER_PATTERNS = [
    re.compile(r"(?:https?://)?(?:www\.)?twitter\.com/([a-zA-Z0-9_]{1,15})/?(?:\?|$|#|\"|\s|<)", re.I),
    re.compile(r"(?:https?://)?(?:www\.)?x\.com/([a-zA-Z0-9_]{1,15})/?", re.I),
]

# LinkedIn patterns
LINKEDIN_PATTERNS = [
    re.compile(r"(?:https?://)?(?:www\.)?linkedin\.com/company/([a-zA-Z0-9_-]+)/?", re.I),
    re.compile(r"(?:https?://)?(?:www\.)?linkedin\.com/in/([a-zA-Z0-9_-]+)/?", re.I),
]

# TikTok patterns
TIKTOK_PATTERNS = [
    re.compile(r"(?:https?://)?(?:www\.)?tiktok\.com/@([a-zA-Z0-9_\.]+)/?", re.I),
]

# YouTube patterns
YOUTUBE_PATTERNS = [
    re.compile(r"(?:https?://)?(?:www\.)?youtube\.com/(?:@|channel/|c/|user/)?([a-zA-Z0-9_-]+)/?", re.I),
]

# Email patterns
EMAIL_PATTERNS = [
    re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-z]{2,}", re.I),
    re.compile(r"mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-z]{2,})", re.I),
]

# Invalid social handles (generic pages)
INVALID_SOCIAL_HANDLES = {
    "share", "sharer", "intent", "dialog", "login", "signup", "home",
    "p", "explore", "accounts", "oauth", "help", "settings", "search",
    "hashtag", "i", "direct", "stories", "reels", "live", "tv",
    "pages", "groups", "events", "marketplace", "gaming", "watch",
    "profile.php", "plugins", "sharer.php", "share.php", "tr",
    "photo.php", "video.php", "reel", "about", "photos", "videos",
}

# Chatbot/automation markers
CHATBOT_MARKERS = [
    "tidio", "intercom", "drift", "crisp", "livechat", "zendesk", "freshchat",
    "hubspot", "tawk.to", "olark", "smartsupp", "chatra", "jivochat",
    "whatsapp-widget", "click-to-chat", "wa-automate", "wati.io",
    "messenger.com/t/", "m.me/", "getbutton.io",
]

# Analytics markers
ANALYTICS_MARKERS = {
    "google_analytics": ["google-analytics.com", "gtag", "ga.js", "analytics.js", "G-", "UA-", "GTM-"],
    "meta_pixel": ["facebook.com/tr", "fbevents.js", "fbq(", "Meta Pixel", "connect.facebook.net"],
}

# CMS markers
CMS_MARKERS = {
    "wordpress": ["wp-content", "wp-includes", "wordpress"],
    "wix": ["wix.com", "wixstatic.com", "_wix"],
    "squarespace": ["squarespace.com", "sqsp.net"],
    "shopify": ["shopify.com", "cdn.shopify"],
    "webflow": ["webflow.com", "webflow.io"],
    "godaddy": ["godaddy.com", "secureserver.net"],
    "weebly": ["weebly.com"],
}


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class BusinessData:
    """Comprehensive business data structure."""
    # Basic Info (from Google Maps)
    name: str = ""
    phone: str = ""
    website: str = ""
    has_website: bool = False
    address: str = ""
    rating: float = 0.0
    review_count: int = 0
    business_hours: str = ""
    category: str = ""
    plus_code: str = ""
    google_maps_url: str = ""

    # Contact Info (extracted from website)
    emails: List[str] = field(default_factory=list)
    whatsapp_numbers: List[str] = field(default_factory=list)
    additional_phones: List[str] = field(default_factory=list)

    # Social Media
    instagram: str = ""
    facebook: str = ""
    twitter: str = ""
    linkedin: str = ""
    tiktok: str = ""
    youtube: str = ""

    # Marketing & Tech Intelligence
    has_chatbot: bool = False
    chatbot_type: str = ""
    has_google_analytics: bool = False
    has_meta_pixel: bool = False
    cms_platform: str = ""
    is_automated: bool = False

    # Metadata
    extraction_quality: str = "unknown"
    data_sources: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON/CSV export."""
        return {
            "name": self.name,
            "phone": self.phone,
            "email": self.emails[0] if self.emails else "",
            "all_emails": "; ".join(self.emails),
            "whatsapp": self.whatsapp_numbers[0] if self.whatsapp_numbers else "",
            "all_whatsapp": "; ".join(self.whatsapp_numbers),
            "website": self.website,
            "has_website": "Yes" if self.has_website else "No",
            "address": self.address,
            "rating": self.rating,
            "review_count": self.review_count,
            "category": self.category,
            "business_hours": self.business_hours,
            "instagram": self.instagram,
            "facebook": self.facebook,
            "twitter": self.twitter,
            "linkedin": self.linkedin,
            "tiktok": self.tiktok,
            "youtube": self.youtube,
            "has_chatbot": "Yes" if self.has_chatbot else "No",
            "chatbot_type": self.chatbot_type,
            "has_google_analytics": "Yes" if self.has_google_analytics else "No",
            "has_meta_pixel": "Yes" if self.has_meta_pixel else "No",
            "cms_platform": self.cms_platform,
            "is_automated": "Yes" if self.is_automated else "No",
            "quality_score": self.extraction_quality,
            "google_maps_url": self.google_maps_url,
        }

    def calculate_quality(self) -> str:
        """Calculate extraction quality score."""
        score = 0
        if self.name:
            score += 1
        if self.phone:
            score += 1
        if self.emails:
            score += 2
        if self.whatsapp_numbers:
            score += 2
        if self.website:
            score += 1
        if self.address:
            score += 1
        if self.instagram or self.facebook:
            score += 1
        if self.has_chatbot or self.has_google_analytics:
            score += 1

        if score >= 8:
            return "high"
        elif score >= 5:
            return "medium"
        else:
            return "low"


# ============================================================================
# EXTRACTION UTILITIES
# ============================================================================

def normalize_phone(phone: str, default_country: str = "92") -> str:
    """Normalize phone number to international format."""
    if not phone:
        return ""
    
    # Remove all non-digit characters except +
    cleaned = re.sub(r"[^\d+]", "", phone)
    
    # Remove + for processing
    has_plus = cleaned.startswith("+")
    digits = cleaned.replace("+", "")
    
    if len(digits) < 8:
        return ""
    
    # Pakistan specific handling
    if digits.startswith("03") and len(digits) == 11:
        # Convert 03XX to 923XX
        digits = default_country + digits[1:]
    elif digits.startswith("3") and len(digits) == 10:
        # Convert 3XX to 923XX
        digits = default_country + digits
    elif digits.startswith("0") and len(digits) == 11:
        # Generic: remove leading 0 and add country code
        digits = default_country + digits[1:]
    
    # Return with + prefix for international format
    if has_plus or len(digits) > 10:
        return "+" + digits
    return digits


def extract_emails(html: str) -> List[str]:
    """Extract unique valid emails from HTML."""
    emails = []
    seen = set()
    
    for pattern in EMAIL_PATTERNS:
        for match in pattern.finditer(html):
            email = match.group(1) if match.groups() else match.group(0)
            email = email.lower().strip()
            
            if is_valid_email(email) and email not in seen:
                seen.add(email)
                emails.append(email)
    
    return emails[:10]  # Limit to 10 emails


def is_valid_email(email: str) -> bool:
    """Check if email is valid and not generic."""
    if not email or "@" not in email:
        return False
    
    # Check format
    parts = email.split("@")
    if len(parts) != 2:
        return False
    
    local, domain = parts
    if not local or not domain or "." not in domain:
        return False
    
    # Filter fake/invalid domains
    invalid_domains = [
        "example.com", "test.com", "email.com", "domain.com",
        "yoursite.com", "website.com", "company.com", "business.com",
        "sentry.io", "wixpress.com", "sentry-next.wixpress.com",
    ]
    if domain in invalid_domains:
        return False
    
    # Filter image/asset "emails"
    if any(ext in email for ext in [".png", ".jpg", ".gif", ".svg", ".webp", ".css", ".js"]):
        return False
    
    return True


def extract_whatsapp(html: str) -> List[str]:
    """Extract WhatsApp numbers from HTML."""
    numbers = []
    seen = set()
    
    for pattern in WHATSAPP_PATTERNS:
        for match in pattern.finditer(html):
            raw_num = match.group(1) if match.groups() else match.group(0)
            normalized = normalize_phone(raw_num)
            
            if normalized and normalized not in seen:
                seen.add(normalized)
                numbers.append(normalized)
    
    # Also check for WhatsApp widget markers with phone numbers nearby
    html_lower = html.lower()
    wa_markers = ["wa.me", "api.whatsapp.com", "wa.link", "whatsapp://", "whatsapp-widget"]
    
    if any(m in html_lower for m in wa_markers) and not numbers:
        # Find phone numbers near WhatsApp mentions
        for match in PHONE_REGEX.finditer(html):
            normalized = normalize_phone(match.group(1))
            if normalized and len(normalized.replace("+", "")) >= 10 and normalized not in seen:
                seen.add(normalized)
                numbers.append(normalized)
                if len(numbers) >= 3:
                    break
    
    return numbers[:5]


def extract_social_handle(html: str, patterns: List[re.Pattern], platform: str) -> str:
    """Extract social media handle/URL from HTML."""
    for pattern in patterns:
        for match in pattern.finditer(html):
            handle = match.group(1) if match.groups() else match.group(0)
            handle = handle.strip().rstrip("/")
            
            if handle and handle.lower() not in INVALID_SOCIAL_HANDLES:
                # Return full URL
                if platform == "instagram":
                    return f"https://www.instagram.com/{handle}"
                elif platform == "facebook":
                    return f"https://www.facebook.com/{handle}"
                elif platform == "twitter":
                    return f"https://twitter.com/{handle}"
                elif platform == "linkedin":
                    return f"https://www.linkedin.com/company/{handle}"
                elif platform == "tiktok":
                    return f"https://www.tiktok.com/@{handle}"
                elif platform == "youtube":
                    return f"https://www.youtube.com/{handle}"
    return ""


def detect_chatbot(html: str) -> Tuple[bool, str]:
    """Detect chatbot/automation on website."""
    html_lower = html.lower()
    for marker in CHATBOT_MARKERS:
        if marker in html_lower:
            return True, marker
    return False, ""


def detect_analytics(html: str) -> Dict[str, bool]:
    """Detect analytics tools."""
    html_lower = html.lower()
    result = {"google_analytics": False, "meta_pixel": False}
    
    for tool, markers in ANALYTICS_MARKERS.items():
        for marker in markers:
            if marker.lower() in html_lower:
                result[tool] = True
                break
    
    return result


def detect_cms(html: str) -> str:
    """Detect CMS platform."""
    html_lower = html.lower()
    for cms, markers in CMS_MARKERS.items():
        for marker in markers:
            if marker.lower() in html_lower:
                return cms
    return ""


def clean_address(address: str) -> str:
    """Clean and format address string."""
    if not address:
        return ""
    
    # Remove excessive whitespace and newlines
    cleaned = re.sub(r"\s+", " ", address)
    cleaned = cleaned.strip()
    
    # Remove leading/trailing punctuation
    cleaned = cleaned.strip(",;.")
    
    return cleaned


# ============================================================================
# CAPTCHA ERROR
# ============================================================================

class CaptchaDetectedError(RuntimeError):
    pass


# ============================================================================
# MAIN SCRAPER CLASS
# ============================================================================

class DeepBusinessScraper:
    """
    Multi-source business intelligence scraper.
    
    Extraction flow:
    1. Search Google Maps for businesses
    2. For each business:
       a. Extract data from Google Maps listing
       b. Visit business website (if available)
       c. Search Google for additional info
       d. Cross-verify and consolidate data
    """

    def __init__(
        self,
        max_results: int = 50,
        headless: bool = False,
        min_delay: float = 0.7,
        max_delay: float = 1.6,
        website_filter: str = "all",
        deep_search: bool = True,
        skip_duplicates: bool = True,  # NEW: Skip previously scraped businesses
        logger: Optional[logging.Logger] = None,
        progress_callback: Optional[Callable[[Dict[str, str]], None]] = None,
    ) -> None:
        self.max_results = max(1, min(max_results, MAX_RESULTS_CAP))
        self.headless = headless
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.website_filter = website_filter if website_filter in {"all", "with", "without"} else "all"
        self.deep_search = deep_search
        self.skip_duplicates = skip_duplicates
        self.log = logger or logging.getLogger(__name__)
        self.progress_callback = progress_callback
        self._website_cache: Dict[str, Dict] = {}
        self._google_cache: Dict[str, Optional[Dict]] = {}
        
        # Initialize history manager for deduplication
        try:
            from scrape_history import get_history
            self.history = get_history(logger)
        except ImportError:
            self.history = None
            self.skip_duplicates = False
        
        # HTTP session for additional requests
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        })

    def scrape(
        self,
        keyword: str,
        location: str,
        stop_event: Optional[Event] = None,
    ) -> List[Dict[str, str]]:
        """Main scrape method with deduplication."""
        stop_event = stop_event or Event()
        search_queries = build_citywide_queries(keyword, location, max_queries=CITYWIDE_QUERY_LIMIT)

        if not search_queries:
            return []

        duplicate_buffer = 0
        if self.skip_duplicates:
            duplicate_buffer = min(12, max(2, self.max_results // 12))
        target_urls = self.max_results + duplicate_buffer
        
        # Log history stats
        if self.skip_duplicates and self.history:
            stats = self.history.get_stats(keyword, location)
            self.log.info(f"📊 History: {stats.get('search_total', 0)} previously scraped for this search")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1400, "height": 1000},
            )
            
            try:
                # Main page for Maps navigation
                page = context.new_page()

                if len(search_queries) > 1:
                    self.log.info("Using %d map zones for broader city coverage", len(search_queries))

                discovered: List[str] = []
                seen: Set[str] = set()
                per_query_target = max(10, (target_urls + len(search_queries) - 1) // len(search_queries))

                for query in search_queries:
                    if stop_event.is_set() or len(discovered) >= target_urls:
                        break

                    remaining = target_urls - len(discovered)
                    query_target = min(per_query_target, remaining)
                    self._open_and_search(page, query)
                    place_urls = self._collect_place_urls(page, stop_event, target_count=query_target)

                    for place_url in place_urls:
                        if place_url and place_url not in seen:
                            seen.add(place_url)
                            discovered.append(place_url)
                            if len(discovered) >= target_urls:
                                break

                if len(discovered) < target_urls and not stop_event.is_set():
                    remaining = target_urls - len(discovered)
                    self._open_and_search(page, search_queries[0])
                    fallback_urls = self._collect_place_urls(page, stop_event, target_count=remaining)
                    for place_url in fallback_urls:
                        if place_url and place_url not in seen:
                            seen.add(place_url)
                            discovered.append(place_url)
                            if len(discovered) >= target_urls:
                                break

                leads = self._collect_lead_details(context, discovered[:target_urls], keyword, location, stop_event)
                
                # Convert to dicts
                results = [lead.to_dict() for lead in leads]
                
                # Add new results to history
                if self.skip_duplicates and self.history and results:
                    self.history.add_batch_to_history(results, keyword, location)
                    self.log.info(f"💾 Saved {len(results)} new businesses to history")
                
                return results
            finally:
                context.close()
                browser.close()

    def _open_and_search(self, page: Page, query: str) -> None:
        """Navigate to Google Maps and search."""
        encoded_query = quote_plus(query)
        page.goto(f"https://www.google.com/maps/search/{encoded_query}", timeout=90000)
        page.wait_for_timeout(1500)
        self._maybe_accept_consent(page)
        self._raise_if_captcha(page)

        if self._wait_for_any(page, ["div[role='feed']", "a.hfpxzc"], timeout_ms=45000):
            self._human_delay()
            return

        search_input = self._find_search_input(page)
        if search_input:
            search_input.fill(query)
            self._human_delay()
            search_input.press("Enter")

        if not self._wait_for_any(page, ["div[role='feed']", "a.hfpxzc", "h1.DUwDvf"], timeout_ms=45000):
            raise RuntimeError(
                "Google Maps results did not load. Check network or try again."
            )

        self._human_delay()
        self._raise_if_captcha(page)

    def _find_search_input(self, page: Page):
        """Find the search input element."""
        selectors = [
            "input#searchboxinput",
            "input[aria-label='Search Google Maps']",
            "input[aria-label*='Search']",
            "input[name='q']",
        ]
        for selector in selectors:
            try:
                locator = page.locator(selector).first
                if locator.count() > 0:
                    locator.wait_for(state="visible", timeout=6000)
                    return locator
            except Exception:
                continue
        return None

    def _wait_for_any(self, page: Page, selectors: List[str], timeout_ms: int) -> bool:
        """Wait for any selector to appear."""
        deadline = time.time() + (timeout_ms / 1000)
        while time.time() < deadline:
            for selector in selectors:
                try:
                    if page.locator(selector).first.count() > 0:
                        return True
                except Exception:
                    continue
            page.wait_for_timeout(400)
        return False

    def _maybe_accept_consent(self, page: Page) -> None:
        """Accept cookie consent if present."""
        selectors = [
            "button:has-text('Accept all')",
            "button:has-text('I agree')",
            "button:has-text('Accept')",
            "button[aria-label='Accept all']",
        ]
        for selector in selectors:
            try:
                button = page.locator(selector).first
                if button.count() > 0 and button.is_visible():
                    button.click(timeout=3000)
                    page.wait_for_timeout(1200)
                    return
            except Exception:
                continue

    def _collect_place_urls(self, page: Page, stop_event: Event, target_count: Optional[int] = None) -> List[str]:
        """Collect place URLs from Maps results - scrolls deeper when deduplication is enabled."""
        discovered: List[str] = []
        seen: Set[str] = set()
        stagnant_rounds = 0
        max_stagnant_rounds = MAP_STAGNANT_ROUNDS + (4 if self.skip_duplicates else 0)
        
        if target_count is None:
            duplicate_buffer = 0
            if self.skip_duplicates:
                duplicate_buffer = min(12, max(2, self.max_results // 12))
            target_urls = self.max_results + duplicate_buffer
        else:
            target_urls = max(1, target_count)

        current_url = page.url or ""
        if "/maps/place/" in current_url:
            return [current_url]

        while len(discovered) < target_urls and stagnant_rounds < max_stagnant_rounds and not stop_event.is_set():
            before = len(discovered)
            
            try:
                hrefs = page.eval_on_selector_all(
                    "a.hfpxzc",
                    "els => els.map(el => el.getAttribute('href') || el.href || '').filter(Boolean)",
                )
            except Exception:
                hrefs = []

            if hrefs:
                tail_start = max(0, len(hrefs) - RESULT_SCAN_WINDOW)
                for href in hrefs[tail_start:]:
                    if stop_event.is_set() or len(discovered) >= target_urls:
                        break
                    if href and href not in seen:
                        seen.add(href)
                        discovered.append(href)
            else:
                links = page.locator("a.hfpxzc")
                count = links.count()
                start_idx = max(0, count - RESULT_SCAN_WINDOW)

                for idx in range(start_idx, count):
                    if stop_event.is_set() or len(discovered) >= target_urls:
                        break
                    try:
                        href = links.nth(idx).get_attribute("href") or ""
                        if href and href not in seen:
                            seen.add(href)
                            discovered.append(href)
                    except PlaywrightTimeoutError:
                        continue

            if len(discovered) == before:
                stagnant_rounds += 1
            else:
                stagnant_rounds = 0

            # Scroll
            feed = page.locator("div[role='feed']").first
            try:
                feed.evaluate("el => el.scrollBy(0, el.scrollHeight)")
            except Exception:
                page.mouse.wheel(0, 4000)

            self._human_delay(MAP_SCROLL_DELAY_MIN, MAP_SCROLL_DELAY_MAX)
            self._raise_if_captcha(page)

        self.log.info("📍 Discovered %d place URLs (target %d)", len(discovered), self.max_results)
        return discovered

    def _collect_lead_details(
        self,
        context: BrowserContext,
        place_urls: List[str],
        keyword: str,
        location: str,
        stop_event: Event,
    ) -> List[BusinessData]:
        """Extract detailed data from each listing with deduplication."""
        leads: List[BusinessData] = []
        skipped_duplicates = 0
        processed = 0

        for index, place_url in enumerate(place_urls, start=1):
            if stop_event.is_set():
                self.log.info("Stop requested. Ending early.")
                break
            
            # Stop if we have enough NEW results
            if len(leads) >= self.max_results:
                break

            processed += 1
            self.log.info(
                "🔍 Processing candidate %d (Collected: %d/%d, Skipped: %d duplicates)",
                processed,
                len(leads),
                self.max_results,
                skipped_duplicates,
            )

            page = context.new_page()
            try:
                lead = self._extract_full_listing(page, place_url, keyword, location)
                if lead:
                    # Check for duplicates BEFORE applying other filters
                    if self.skip_duplicates and self.history:
                        lead_dict = lead.to_dict()
                        if self.history.is_duplicate(lead_dict, keyword, location):
                            skipped_duplicates += 1
                            self.log.info("⏭️ Skipping duplicate: %s", lead.name)
                            continue
                    
                    if self._passes_website_filter(lead.website):
                        lead.extraction_quality = lead.calculate_quality()
                        leads.append(lead)
                        self.log.info("✓ NEW: %s (Quality: %s)", lead.name, lead.extraction_quality)
                        if self.progress_callback:
                            try:
                                self.progress_callback(lead.to_dict())
                            except Exception:
                                # Progress callbacks should never interrupt scraping.
                                pass
            except CaptchaDetectedError:
                raise
            except Exception as e:
                self.log.error("Failed to extract %s: %s", place_url, e)
            finally:
                page.close()

            self._human_delay(0.2, 0.5)
        
        self.log.info(f"📊 Summary: {len(leads)} new businesses, {skipped_duplicates} duplicates skipped")
        return leads

    def _passes_website_filter(self, website: str) -> bool:
        """Check if listing passes website filter."""
        has_website = is_business_website(website)
        if self.website_filter == "with":
            return has_website
        if self.website_filter == "without":
            return not has_website
        return True

    def _extract_full_listing(
        self,
        page: Page,
        place_url: str,
        keyword: str,
        location: str,
    ) -> Optional[BusinessData]:
        """Extract comprehensive data from a single listing."""
        for attempt in range(2):
            try:
                page.goto(place_url, timeout=60000)
                page.wait_for_timeout(1800)
                self._raise_if_captcha(page)

                data = BusinessData()
                data.google_maps_url = place_url
                data.data_sources.append("google_maps")

                # ===== STEP 1: Extract from Google Maps =====
                data.name = self._safe_text(page, "h1.DUwDvf", fallback_selector="h1")
                data.phone = self._extract_phone(page)
                data.website = self._extract_website(page)
                data.has_website = bool(data.website)
                data.address = self._extract_address(page)
                data.rating, data.review_count = self._extract_rating(page)
                data.category = self._extract_category(page)
                data.business_hours = self._extract_hours(page)
                
                # Social from Maps
                gmaps_socials = self._extract_social_from_gmaps(page)
                data.instagram = gmaps_socials.get("instagram", "")
                data.facebook = gmaps_socials.get("facebook", "")
                data.twitter = gmaps_socials.get("twitter", "")

                # ===== STEP 2: Analyze website if available =====
                if data.website:
                    cache_key = self._website_cache_key(data.website)
                    website_data = self._website_cache.get(cache_key)
                    if website_data is None:
                        website_data = self._deep_analyze_website(page, data.website)
                        self._website_cache[cache_key] = dict(website_data)
                    data.data_sources.append("website")
                    
                    # Merge website data
                    data.emails = website_data.get("emails", [])
                    data.whatsapp_numbers = website_data.get("whatsapp_numbers", [])
                    
                    socials = website_data.get("socials", {})
                    if not data.instagram and socials.get("instagram"):
                        data.instagram = socials["instagram"]
                    if not data.facebook and socials.get("facebook"):
                        data.facebook = socials["facebook"]
                    if not data.twitter and socials.get("twitter"):
                        data.twitter = socials["twitter"]
                    if socials.get("linkedin"):
                        data.linkedin = socials["linkedin"]
                    if socials.get("tiktok"):
                        data.tiktok = socials["tiktok"]
                    if socials.get("youtube"):
                        data.youtube = socials["youtube"]
                    
                    data.has_chatbot = website_data.get("has_chatbot", False)
                    data.chatbot_type = website_data.get("chatbot_type", "")
                    data.has_google_analytics = website_data.get("has_google_analytics", False)
                    data.has_meta_pixel = website_data.get("has_meta_pixel", False)
                    data.cms_platform = website_data.get("cms_platform", "")
                    data.is_automated = data.has_chatbot

                # ===== STEP 3: Google search for additional info =====
                needs_google_lookup = (
                    self.deep_search
                    and data.name
                    and (
                        not data.instagram
                        or not data.facebook
                        or not data.whatsapp_numbers
                        or not data.emails
                    )
                )
                if needs_google_lookup:
                    google_key = f"{data.name.strip().lower()}|{location.strip().lower()}"
                    if google_key not in self._google_cache:
                        self._google_cache[google_key] = self._search_google_for_business(page, data.name, location)
                    google_data = self._google_cache.get(google_key)
                    if google_data:
                        data.data_sources.append("google_search")
                        
                        # Fill in missing data
                        if not data.instagram and google_data.get("instagram"):
                            data.instagram = google_data["instagram"]
                        if not data.facebook and google_data.get("facebook"):
                            data.facebook = google_data["facebook"]
                        if not data.whatsapp_numbers and google_data.get("whatsapp"):
                            data.whatsapp_numbers = [google_data["whatsapp"]]
                        if not data.emails and google_data.get("email"):
                            data.emails = [google_data["email"]]

                # ===== STEP 4: Use phone as WhatsApp fallback =====
                if not data.whatsapp_numbers and data.phone:
                    normalized = normalize_phone(data.phone)
                    if normalized:
                        data.whatsapp_numbers = [normalized]

                return data

            except CaptchaDetectedError:
                raise
            except Exception as exc:
                self.log.warning("Attempt %d failed for %s: %s", attempt + 1, place_url, exc)
                self._human_delay(1.5, 2.5)

        return None

    def _safe_text(self, page: Page, selector: str, fallback_selector: str = "") -> str:
        """Safely extract text from element."""
        try:
            locator = page.locator(selector).first
            if locator.count() > 0:
                value = locator.inner_text(timeout=4000).strip()
                if value:
                    return value
        except Exception:
            pass

        if fallback_selector:
            try:
                locator = page.locator(fallback_selector).first
                if locator.count() > 0:
                    value = locator.inner_text(timeout=3000).strip()
                    if value:
                        return value
            except Exception:
                pass

        return ""

    def _extract_phone(self, page: Page) -> str:
        """Extract phone number from Google Maps."""
        selectors = [
            "button[data-item-id^='phone:tel:']",
            "a[data-item-id^='phone:tel:']",
            "button[aria-label*='Phone']",
            "button[aria-label*='phone']",
        ]

        for selector in selectors:
            try:
                locator = page.locator(selector).first
                if locator.count() == 0:
                    continue
                text = locator.inner_text(timeout=3500).strip()
                match = PHONE_REGEX.search(text)
                if match:
                    return match.group(1).strip()
            except Exception:
                continue

        return ""

    def _extract_website(self, page: Page) -> str:
        """Extract website URL from Google Maps."""
        selectors = [
            "a[data-item-id='authority']",
            "a[aria-label*='Website']",
            "a[aria-label*='website']",
        ]

        for selector in selectors:
            try:
                anchor = page.locator(selector).first
                if anchor.count() == 0:
                    continue
                href = anchor.get_attribute("href") or ""
                if href and href.startswith("http"):
                    return normalize_business_website(href)
            except Exception:
                continue

        return ""

    def _extract_address(self, page: Page) -> str:
        """Extract and clean business address."""
        selectors = [
            "button[data-item-id='address']",
            "button[aria-label*='Address']",
            "button[aria-label*='address']",
            "div[data-item-id='address']",
        ]

        for selector in selectors:
            try:
                locator = page.locator(selector).first
                if locator.count() > 0:
                    text = locator.inner_text(timeout=3000)
                    cleaned = clean_address(text)
                    if cleaned:
                        return cleaned
            except Exception:
                continue

        return ""

    def _extract_rating(self, page: Page) -> Tuple[float, int]:
        """Extract rating and review count."""
        rating = 0.0
        review_count = 0

        try:
            # Rating
            rating_el = page.locator("span.ceNzKf, div.F7nice span[aria-hidden='true']").first
            if rating_el.count() > 0:
                rating_text = rating_el.inner_text(timeout=3000)
                match = re.search(r"[\d.]+", rating_text)
                if match:
                    rating = float(match.group())
        except Exception:
            pass

        try:
            # Review count - try multiple selectors
            review_selectors = [
                "span.UY7F9",
                "button[jsaction*='review'] span",
                "span[aria-label*='review']",
            ]
            for sel in review_selectors:
                review_el = page.locator(sel).first
                if review_el.count() > 0:
                    review_text = review_el.inner_text(timeout=3000)
                    # Extract number, handle commas
                    review_match = re.search(r"([\d,]+)", review_text)
                    if review_match:
                        review_count = int(review_match.group(1).replace(",", ""))
                        break
        except Exception:
            pass

        return rating, review_count

    def _extract_category(self, page: Page) -> str:
        """Extract business category."""
        try:
            category_el = page.locator("button.DkEaL, span.DkEaL").first
            if category_el.count() > 0:
                return category_el.inner_text(timeout=3000).strip()
        except Exception:
            pass
        return ""

    def _extract_hours(self, page: Page) -> str:
        """Extract business hours."""
        try:
            hours_button = page.locator("button[data-item-id*='oh'], button[aria-label*='hour']").first
            if hours_button.count() > 0:
                text = hours_button.inner_text(timeout=3000).strip()
                # Clean up the text
                text = re.sub(r"\s+", " ", text)
                return text
        except Exception:
            pass
        return ""

    def _extract_social_from_gmaps(self, page: Page) -> Dict[str, str]:
        """Extract social links shown on Google Maps."""
        socials = {}

        try:
            links = page.eval_on_selector_all(
                "a[href*='instagram.com'], a[href*='facebook.com'], a[href*='twitter.com'], a[href*='x.com']",
                "els => els.map(el => el.href)"
            )

            for link in links:
                link_lower = link.lower()
                if "instagram.com" in link_lower:
                    socials["instagram"] = link
                elif "facebook.com" in link_lower:
                    socials["facebook"] = link
                elif "twitter.com" in link_lower or "x.com" in link_lower:
                    socials["twitter"] = link
        except Exception:
            pass

        return socials

    def _deep_analyze_website(self, page: Page, website_url: str) -> Dict:
        """Deep website analysis.

        Prefer fast HTTP crawling (email_extractor WebsiteExtractor) and only fall back
        to Playwright navigation when HTTP yields nothing.
        """
        combined_data = {
            "emails": [],
            "whatsapp_numbers": [],
            "socials": {},
            "has_chatbot": False,
            "chatbot_type": "",
            "has_google_analytics": False,
            "has_meta_pixel": False,
            "cms_platform": "",
        }

        # Normalize URL
        if not website_url.startswith(("http://", "https://")):
            website_url = f"https://{website_url}"

        base_url = website_url.rstrip("/")

        # ---- Phase 1: HTTP crawl (fast) ----
        try:
            crawler = WebsiteExtractor(timeout=min(12, REQUEST_TIMEOUT))
            pages = crawler.crawl_pages(base_url, max_pages=8)
            if pages:
                corpus = "\n\n".join(p.html for p in pages if p.html)

                combined_data["emails"] = list(dict.fromkeys(extract_emails(corpus)))
                combined_data["whatsapp_numbers"] = list(dict.fromkeys(extract_whatsapp(corpus)))

                socials = {}
                for social, patterns, label in [
                    ("instagram", INSTAGRAM_PATTERNS, "instagram"),
                    ("facebook", FACEBOOK_PATTERNS, "facebook"),
                    ("twitter", TWITTER_PATTERNS, "twitter"),
                    ("linkedin", LINKEDIN_PATTERNS, "linkedin"),
                    ("tiktok", TIKTOK_PATTERNS, "tiktok"),
                    ("youtube", YOUTUBE_PATTERNS, "youtube"),
                ]:
                    val = extract_social_handle(corpus, patterns, label)
                    if val:
                        socials[social] = val
                combined_data["socials"] = socials

                has_bot, bot_type = detect_chatbot(corpus)
                combined_data["has_chatbot"] = bool(has_bot)
                combined_data["chatbot_type"] = bot_type or ""

                analytics = detect_analytics(corpus)
                combined_data["has_google_analytics"] = bool(analytics.get("google_analytics"))
                combined_data["has_meta_pixel"] = bool(analytics.get("meta_pixel"))
                combined_data["cms_platform"] = detect_cms(corpus) or ""

                return combined_data
        except Exception as e:
            self.log.debug("HTTP crawl failed: %s", e)

        # ---- Phase 2: Playwright fallback (JS-heavy) ----
        pages_to_check = CONTACT_PAGES + SOCIAL_PAGES

        for path in pages_to_check[:8]:
            try:
                url = f"{base_url}{path}"
                response = page.goto(url, timeout=15000, wait_until="domcontentloaded")

                if not response or response.status >= 400:
                    continue

                page.wait_for_timeout(1200)
                html = page.content()

                page_emails = extract_emails(html)
                page_whatsapp = extract_whatsapp(html)

                for email in page_emails:
                    if email not in combined_data["emails"]:
                        combined_data["emails"].append(email)

                for wa in page_whatsapp:
                    if wa not in combined_data["whatsapp_numbers"]:
                        combined_data["whatsapp_numbers"].append(wa)

                if not combined_data["socials"].get("instagram"):
                    ig = extract_social_handle(html, INSTAGRAM_PATTERNS, "instagram")
                    if ig:
                        combined_data["socials"]["instagram"] = ig

                if not combined_data["socials"].get("facebook"):
                    fb = extract_social_handle(html, FACEBOOK_PATTERNS, "facebook")
                    if fb:
                        combined_data["socials"]["facebook"] = fb

                if not combined_data["socials"].get("twitter"):
                    tw = extract_social_handle(html, TWITTER_PATTERNS, "twitter")
                    if tw:
                        combined_data["socials"]["twitter"] = tw

                if not combined_data["socials"].get("linkedin"):
                    li = extract_social_handle(html, LINKEDIN_PATTERNS, "linkedin")
                    if li:
                        combined_data["socials"]["linkedin"] = li

                if not combined_data["socials"].get("tiktok"):
                    tt = extract_social_handle(html, TIKTOK_PATTERNS, "tiktok")
                    if tt:
                        combined_data["socials"]["tiktok"] = tt

                if not combined_data["socials"].get("youtube"):
                    yt = extract_social_handle(html, YOUTUBE_PATTERNS, "youtube")
                    if yt:
                        combined_data["socials"]["youtube"] = yt

                if not combined_data["has_chatbot"]:
                    has_bot, bot_type = detect_chatbot(html)
                    if has_bot:
                        combined_data["has_chatbot"] = True
                        combined_data["chatbot_type"] = bot_type

                analytics = detect_analytics(html)
                if analytics["google_analytics"]:
                    combined_data["has_google_analytics"] = True
                if analytics["meta_pixel"]:
                    combined_data["has_meta_pixel"] = True

                if not combined_data["cms_platform"]:
                    combined_data["cms_platform"] = detect_cms(html)

                if combined_data["emails"] and combined_data["whatsapp_numbers"] and combined_data["socials"].get("instagram"):
                    break

            except Exception as e:
                self.log.debug("Error analyzing %s: %s", url, e)
                continue

        return combined_data

    def _website_cache_key(self, website_url: str) -> str:
        if not website_url:
            return ""
        normalized = website_url if website_url.startswith(("http://", "https://")) else f"https://{website_url}"
        parsed = urlparse(normalized)
        host = (parsed.netloc or parsed.path).lower().strip()
        if host.startswith("www."):
            host = host[4:]
        return host

    def _search_google_for_business(self, page: Page, business_name: str, location: str) -> Optional[Dict]:
        """
        Search Google for additional business information.
        Cross-verifies and finds missing social media links.
        """
        try:
            # Create search query
            search_query = f"{business_name} {location} instagram contact"
            encoded_query = quote_plus(search_query)
            
            page.goto(f"https://www.google.com/search?q={encoded_query}", timeout=20000)
            page.wait_for_timeout(1500)

            html = page.content()
            result = {}

            # Look for Instagram
            ig = extract_social_handle(html, INSTAGRAM_PATTERNS, "instagram")
            if ig:
                result["instagram"] = ig

            # Look for Facebook
            fb = extract_social_handle(html, FACEBOOK_PATTERNS, "facebook")
            if fb:
                result["facebook"] = fb

            # Look for WhatsApp
            wa_list = extract_whatsapp(html)
            if wa_list:
                result["whatsapp"] = wa_list[0]

            # Look for email
            emails = extract_emails(html)
            if emails:
                result["email"] = emails[0]

            return result if result else None

        except Exception as e:
            self.log.debug("Google search failed: %s", e)
            return None

    def _raise_if_captcha(self, page: Page) -> None:
        """Check for CAPTCHA."""
        try:
            content = page.content().lower()
        except Exception:
            return

        if "unusual traffic" in content or "detected unusual" in content or "recaptcha" in content:
            raise CaptchaDetectedError("Captcha or anti-bot challenge detected")

    def _human_delay(self, minimum: Optional[float] = None, maximum: Optional[float] = None) -> None:
        """Add human-like delay."""
        min_d = self.min_delay if minimum is None else minimum
        max_d = self.max_delay if maximum is None else maximum
        time.sleep(random.uniform(min_d, max_d))


# Alias for backwards compatibility
GoogleMapsScraper = DeepBusinessScraper
