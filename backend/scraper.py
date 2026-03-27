import logging
import random
import re
import time
from threading import Event
from typing import Dict, List, Optional, Set
from urllib.parse import quote_plus

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from email_extractor import WebsiteExtractor

PHONE_REGEX = re.compile(r"(\+?\d[\d\s()\-]{6,}\d)")
MAX_RESULTS_CAP = 500
RESULT_SCAN_WINDOW = 140


class CaptchaDetectedError(RuntimeError):
    pass


class GoogleMapsScraper:
    def __init__(
        self,
        max_results: int = 50,
        headless: bool = False,
        min_delay: float = 0.7,
        max_delay: float = 1.6,
        website_filter: str = "all",
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.max_results = max(1, min(max_results, MAX_RESULTS_CAP))
        self.headless = headless
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.website_filter = website_filter if website_filter in {"all", "with", "without"} else "all"
        self.log = logger or logging.getLogger(__name__)
        self.website_extractor = WebsiteExtractor()

    def scrape(
        self,
        keyword: str,
        location: str,
        stop_event: Optional[Event] = None,
    ) -> List[Dict[str, str]]:
        query = f"{keyword} in {location}".strip()
        stop_event = stop_event or Event()

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
            page = context.new_page()

            try:
                self._open_and_search(page, query)
                place_urls = self._collect_place_urls(page, stop_event)
                leads = self._collect_lead_details(page, place_urls, stop_event)
                return leads
            finally:
                context.close()
                browser.close()

    def _open_and_search(self, page, query: str) -> None:
        # Directly loading the maps search URL is more stable than relying only on the homepage search box.
        encoded_query = quote_plus(query)
        page.goto(f"https://www.google.com/maps/search/{encoded_query}", timeout=90000)
        page.wait_for_timeout(1200)
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
                "Google Maps results did not load. This can happen due to consent/CAPTCHA, network issues, or UI changes."
            )

        self._human_delay()
        self._raise_if_captcha(page)

    def _find_search_input(self, page):
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

    def _wait_for_any(self, page, selectors: List[str], timeout_ms: int) -> bool:
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

    def _maybe_accept_consent(self, page) -> None:
        selectors = [
            "button:has-text('Accept all')",
            "button:has-text('I agree')",
            "button:has-text('Accept')",
            "button[aria-label='Accept all']",
            "form button[type='submit']",
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

    def _collect_place_urls(self, page, stop_event: Event) -> List[str]:
        discovered: List[str] = []
        seen: Set[str] = set()
        stagnant_rounds = 0
        max_stagnant_rounds = 14 if self.max_results > 100 else 8
        scroll_delay_min, scroll_delay_max = (0.45, 0.9) if self.max_results > 100 else (self.min_delay, self.max_delay)

        if "/maps/place/" in (page.url or ""):
            return [page.url]

        while len(discovered) < self.max_results and stagnant_rounds < max_stagnant_rounds and not stop_event.is_set():
            before = len(discovered)
            hrefs: List[str] = []

            try:
                hrefs = page.eval_on_selector_all(
                    "a.hfpxzc",
                    "els => els.map(el => el.getAttribute('href') || el.href || '').filter(Boolean)",
                )
            except Exception:
                hrefs = []

            if hrefs:
                # New URLs usually appear near the tail of the results feed; scanning a window keeps this loop fast.
                tail_start = max(0, len(hrefs) - RESULT_SCAN_WINDOW)
                for href in hrefs[tail_start:]:
                    if stop_event.is_set() or len(discovered) >= self.max_results:
                        break
                    if href and href not in seen:
                        seen.add(href)
                        discovered.append(href)
            else:
                links = page.locator("a.hfpxzc")
                count = links.count()
                start_idx = max(0, count - RESULT_SCAN_WINDOW)

                for idx in range(start_idx, count):
                    if stop_event.is_set() or len(discovered) >= self.max_results:
                        break
                    href = ""
                    for _ in range(2):
                        try:
                            href = links.nth(idx).get_attribute("href") or ""
                            break
                        except PlaywrightTimeoutError:
                            self._human_delay(0.6, 1.2)
                    if href and href not in seen:
                        seen.add(href)
                        discovered.append(href)

            if len(discovered) == before:
                stagnant_rounds += 1
            else:
                stagnant_rounds = 0

            feed = page.locator("div[role='feed']").first
            try:
                feed.evaluate("el => el.scrollBy(0, el.scrollHeight)")
            except Exception:
                page.mouse.wheel(0, 4000)

            self._human_delay(scroll_delay_min, scroll_delay_max)
            self._raise_if_captcha(page)

        self.log.info("Discovered %s place urls", len(discovered))
        return discovered[: self.max_results]

    def _collect_lead_details(self, page, place_urls: List[str], stop_event: Event) -> List[Dict[str, str]]:
        leads: List[Dict[str, str]] = []

        for index, place_url in enumerate(place_urls, start=1):
            if stop_event.is_set():
                self.log.info("Stop requested. Ending scrape early.")
                break

            self.log.info("Processing %s/%s", index, len(place_urls))
            lead = self._extract_single_listing(page, place_url)
            if not lead:
                continue

            if not self._passes_website_filter(lead.get("website", "")):
                continue

            leads.append(lead)
            self._human_delay()

        return leads

    def _passes_website_filter(self, website: str) -> bool:
        has_website = bool((website or "").strip())
        if self.website_filter == "with":
            return has_website
        if self.website_filter == "without":
            return not has_website
        return True

    def _extract_single_listing(self, page, place_url: str) -> Optional[Dict[str, str]]:
        for attempt in range(2):
            try:
                page.goto(place_url, timeout=60000)
                page.wait_for_timeout(1200)
                self._raise_if_captcha(page)

                name = self._safe_text(page, "h1.DUwDvf", fallback_selector="h1")
                phone = self._extract_phone(page)
                website = self._extract_website(page)

                enrichment = self.website_extractor.enrich(website, fallback_phone=phone) if website else {
                    "email": "",
                    "whatsapp": self.website_extractor._normalize_phone(phone),
                }

                return {
                    "name": name,
                    "phone": phone,
                    "email": enrichment.get("email", ""),
                    "website": website,
                    "whatsapp": enrichment.get("whatsapp", ""),
                    "google_maps_url": place_url,
                    "has_website": "Yes" if bool((website or "").strip()) else "No",
                }
            except CaptchaDetectedError:
                raise
            except Exception as exc:
                self.log.warning("Failed listing attempt %s for %s: %s", attempt + 1, place_url, exc)
                self._human_delay(1.2, 2.2)

        return None

    def _safe_text(self, page, selector: str, fallback_selector: str = "") -> str:
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

    def _extract_phone(self, page) -> str:
        selectors = [
            "button[data-item-id^='phone:tel:']",
            "button[aria-label*='Phone']",
            "button[aria-label*='phone']",
        ]

        for selector in selectors:
            try:
                locator = page.locator(selector).first
                if locator.count() == 0:
                    continue
                text = self._clean_phone_text(locator.inner_text(timeout=3500))
                if text:
                    return text
            except Exception:
                continue

        return ""

    def _clean_phone_text(self, value: str) -> str:
        raw = (value or "").strip()
        if not raw:
            return ""
        match = PHONE_REGEX.search(raw)
        if match:
            return match.group(1).strip()
        # Remove obvious non-phone symbols while preserving useful separators.
        cleaned = re.sub(r"[^0-9+()\-\s.]", "", raw)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned

    def _extract_website(self, page) -> str:
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
                    return href
            except Exception:
                continue

        return ""

    def _raise_if_captcha(self, page) -> None:
        try:
            content = page.content().lower()
        except Exception:
            return

        if "unusual traffic" in content or "detected unusual" in content or "recaptcha" in content:
            raise CaptchaDetectedError("Captcha or anti-bot challenge detected on Google Maps")

    def _human_delay(self, minimum: Optional[float] = None, maximum: Optional[float] = None) -> None:
        min_d = self.min_delay if minimum is None else minimum
        max_d = self.max_delay if maximum is None else maximum
        time.sleep(random.uniform(min_d, max_d))
