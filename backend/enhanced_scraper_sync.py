"""
Enhanced Google Maps Scraper (Sync Version)
Extracts comprehensive business details with focus on ACCURACY and COMPLETENESS.
Uses sync Playwright API for Flask compatibility.
"""

import logging
import random
import re
import time
from threading import Event
from typing import Dict, List, Optional, Set
from urllib.parse import quote_plus

from playwright.sync_api import sync_playwright, Page, BrowserContext
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from business_extractor import BusinessData, analyze_website
from email_extractor import WebsiteExtractor

PHONE_REGEX = re.compile(r"(\+?\d[\d\s()\-]{6,}\d)")
MAX_RESULTS_CAP = 500
RESULT_SCAN_WINDOW = 140

# Pages to check for contact info (in order of priority)
CONTACT_PAGES = ["", "/contact", "/contact-us", "/about", "/about-us", "/team"]


class CaptchaDetectedError(RuntimeError):
    pass


class GoogleMapsScraper:
    """Enhanced scraper that extracts comprehensive business intelligence."""
    
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
    
    def scrape(
        self,
        keyword: str,
        location: str,
        stop_event: Optional[Event] = None,
    ) -> List[Dict[str, str]]:
        """Main scrape method."""
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
                leads = self._collect_lead_details(context, place_urls, stop_event)
                return [lead.to_dict() for lead in leads]
            finally:
                context.close()
                browser.close()
    
    def _open_and_search(self, page: Page, query: str) -> None:
        """Navigate to Google Maps and search."""
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
        """Wait for any of the given selectors to appear."""
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
    
    def _collect_place_urls(self, page: Page, stop_event: Event) -> List[str]:
        """Collect all place URLs from search results."""
        discovered: List[str] = []
        seen: Set[str] = set()
        stagnant_rounds = 0
        max_stagnant_rounds = 14 if self.max_results > 100 else 8
        scroll_delay_min, scroll_delay_max = (0.45, 0.9) if self.max_results > 100 else (self.min_delay, self.max_delay)
        
        current_url = page.url or ""
        if "/maps/place/" in current_url:
            return [current_url]
        
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
    
    def _collect_lead_details(
        self, 
        context: BrowserContext, 
        place_urls: List[str], 
        stop_event: Event
    ) -> List[BusinessData]:
        """Extract detailed business data from each listing."""
        leads: List[BusinessData] = []
        
        for index, place_url in enumerate(place_urls, start=1):
            if stop_event.is_set():
                self.log.info("Stop requested. Ending scrape early.")
                break
            
            self.log.info("Processing %s/%s", index, len(place_urls))
            
            # Create a new page for each listing to avoid state issues
            page = context.new_page()
            try:
                lead = self._extract_full_listing(page, place_url)
                if lead:
                    if self._passes_website_filter(lead.website):
                        lead.extraction_quality = lead.calculate_quality()
                        leads.append(lead)
                        self.log.info("Successfully extracted: %s (Total: %d)", lead.name, len(leads))
            except Exception as e:
                self.log.error("Failed to extract %s: %s", place_url, e)
            finally:
                page.close()
            
            self._human_delay()
        
        return leads
    
    def _passes_website_filter(self, website: str) -> bool:
        """Check if listing passes website filter."""
        has_website = bool((website or "").strip())
        if self.website_filter == "with":
            return has_website
        if self.website_filter == "without":
            return not has_website
        return True
    
    def _extract_full_listing(self, page: Page, place_url: str) -> Optional[BusinessData]:
        """Extract comprehensive data from a single Google Maps listing."""
        for attempt in range(2):
            try:
                page.goto(place_url, timeout=60000)
                page.wait_for_timeout(1500)
                self._raise_if_captcha(page)
                
                # Create business data object
                data = BusinessData()
                data.google_maps_url = place_url
                
                # Extract all Google Maps data
                data.name = self._safe_text(page, "h1.DUwDvf", fallback_selector="h1")
                data.phone = self._extract_phone(page)
                data.website = self._extract_website(page)
                data.has_website = bool(data.website)
                data.address = self._extract_address(page)
                data.rating, data.review_count = self._extract_rating(page)
                data.category = self._extract_category(page)
                data.business_hours = self._extract_hours(page)
                data.plus_code = self._extract_plus_code(page)
                
                # Extract social links from Google Maps (if shown)
                gmaps_socials = self._extract_social_from_gmaps(page)
                data.instagram = gmaps_socials.get("instagram", "")
                data.facebook = gmaps_socials.get("facebook", "")
                data.twitter = gmaps_socials.get("twitter", "")
                
                # If has website, extract additional data
                if data.website:
                    website_data = self._analyze_website(page, data.website)
                    
                    # Merge website data
                    data.emails = website_data.get("emails", [])
                    data.whatsapp_numbers = website_data.get("whatsapp_numbers", [])
                    
                    socials = website_data.get("socials", {})
                    if not data.instagram:
                        data.instagram = socials.get("instagram", "")
                    if not data.facebook:
                        data.facebook = socials.get("facebook", "")
                    if not data.twitter:
                        data.twitter = socials.get("twitter", "")
                    data.linkedin = socials.get("linkedin", "")
                    data.tiktok = socials.get("tiktok", "")
                    data.youtube = socials.get("youtube", "")
                    
                    data.has_chatbot = website_data.get("has_chatbot", False)
                    data.chatbot_type = website_data.get("chatbot_type", "")
                    data.has_google_analytics = website_data.get("has_google_analytics", False)
                    data.has_meta_pixel = website_data.get("has_meta_pixel", False)
                    data.has_other_analytics = website_data.get("other_analytics", [])
                    data.cms_platform = website_data.get("cms_platform", "")
                    data.is_automated = data.has_chatbot
                else:
                    # Use phone as WhatsApp fallback
                    if data.phone:
                        normalized = self._normalize_phone(data.phone)
                        if normalized:
                            data.whatsapp_numbers = [normalized]
                
                return data
                
            except CaptchaDetectedError:
                raise
            except Exception as exc:
                self.log.warning("Failed listing attempt %s for %s: %s", attempt + 1, place_url, exc)
                self._human_delay(1.2, 2.2)
        
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
        """Extract phone number from Google Maps listing."""
        selectors = [
            "button[data-item-id^='phone:tel:']",
            "button[aria-label*='Phone']",
            "button[aria-label*='phone']",
            "a[data-item-id^='phone:tel:']",
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
        """Clean and normalize phone text."""
        raw = (value or "").strip()
        if not raw:
            return ""
        match = PHONE_REGEX.search(raw)
        if match:
            return match.group(1).strip()
        cleaned = re.sub(r"[^0-9+()\-\s.]", "", raw)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned
    
    def _normalize_phone(self, phone: str) -> str:
        """Normalize phone to digits only."""
        if not phone:
            return ""
        digits = re.sub(r"[^\d+]", "", phone)
        return digits if len(digits) >= 8 else ""
    
    def _extract_website(self, page: Page) -> str:
        """Extract website URL from Google Maps listing."""
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
    
    def _extract_address(self, page: Page) -> str:
        """Extract business address from Google Maps."""
        selectors = [
            "button[data-item-id='address']",
            "button[aria-label*='Address']",
            "button[aria-label*='address']",
        ]
        
        for selector in selectors:
            try:
                locator = page.locator(selector).first
                if locator.count() > 0:
                    text = locator.inner_text(timeout=3000).strip()
                    if text:
                        return text
            except Exception:
                continue
        
        return ""
    
    def _extract_rating(self, page: Page) -> tuple:
        """Extract rating and review count."""
        rating = 0.0
        review_count = 0
        
        try:
            # Try to find rating span
            rating_el = page.locator("span.ceNzKf, div.F7nice span[aria-hidden='true']").first
            if rating_el.count() > 0:
                rating_text = rating_el.inner_text(timeout=3000)
                match = re.search(r"[\d.]+", rating_text)
                if match:
                    rating = float(match.group())
        except Exception:
            pass
        
        try:
            # Try to find review count
            review_el = page.locator("span.UY7F9, button[jsaction*='review'] span").first
            if review_el.count() > 0:
                review_text = review_el.inner_text(timeout=3000)
                review_match = re.search(r"[\d,]+", review_text.replace(",", ""))
                if review_match:
                    review_count = int(review_match.group())
        except Exception:
            pass
        
        return rating, review_count
    
    def _extract_category(self, page: Page) -> str:
        """Extract business category."""
        try:
            # Category is often shown as a button below the name
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
                return hours_button.inner_text(timeout=3000).strip()
        except Exception:
            pass
        return ""
    
    def _extract_plus_code(self, page: Page) -> str:
        """Extract Plus Code (location code)."""
        try:
            plus_el = page.locator("button[data-item-id='oloc']").first
            if plus_el.count() > 0:
                return plus_el.inner_text(timeout=3000).strip()
        except Exception:
            pass
        return ""
    
    def _extract_social_from_gmaps(self, page: Page) -> Dict[str, str]:
        """Extract social links shown directly on Google Maps."""
        socials = {}
        
        try:
            # Look for social media links in the info section
            links = page.eval_on_selector_all(
                "a[href*='instagram.com'], a[href*='facebook.com'], a[href*='twitter.com'], a[href*='x.com']",
                "els => els.map(el => el.href)"
            )
            
            for link in links:
                if "instagram.com" in link:
                    socials["instagram"] = link
                elif "facebook.com" in link:
                    socials["facebook"] = link
                elif "twitter.com" in link or "x.com" in link:
                    socials["twitter"] = link
        except Exception:
            pass
        
        return socials
    
    def _analyze_website(self, page: Page, website_url: str) -> Dict:
        """Analyze a business website for contact info + tech stack.

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
            "other_analytics": [],
            "cms_platform": "",
        }

        if not website_url.startswith(("http://", "https://")):
            website_url = f"https://{website_url}"

        base_url = website_url.rstrip("/")

        # ---- Phase 1: HTTP crawl (fast) ----
        try:
            crawler = WebsiteExtractor(timeout=12)
            pages = crawler.crawl_pages(base_url, max_pages=8)
            if pages:
                corpus = "\n\n".join(p.html for p in pages if p.html)
                page_data = analyze_website(corpus, base_url)

                combined_data["emails"] = list(dict.fromkeys(page_data.get("emails", [])))
                combined_data["whatsapp_numbers"] = list(dict.fromkeys(page_data.get("whatsapp_numbers", [])))
                combined_data["socials"] = page_data.get("socials", {}) or {}
                combined_data["has_chatbot"] = bool(page_data.get("has_chatbot"))
                combined_data["chatbot_type"] = page_data.get("chatbot_type", "") or ""
                combined_data["has_google_analytics"] = bool(page_data.get("has_google_analytics"))
                combined_data["has_meta_pixel"] = bool(page_data.get("has_meta_pixel"))
                combined_data["other_analytics"] = list(dict.fromkeys(page_data.get("other_analytics", []) or []))
                combined_data["cms_platform"] = page_data.get("cms_platform", "") or ""
                return combined_data
        except Exception as e:
            self.log.debug("HTTP crawl failed: %s", e)

        # ---- Phase 2: Playwright fallback ----
        for path in CONTACT_PAGES:
            try:
                url = f"{base_url}{path}"
                response = page.goto(url, timeout=15000, wait_until="domcontentloaded")

                if not response or response.status >= 400:
                    continue

                page.wait_for_timeout(1000)
                html = page.content()
                page_data = analyze_website(html, url)

                for email in page_data.get("emails", []):
                    if email not in combined_data["emails"]:
                        combined_data["emails"].append(email)

                for wa in page_data.get("whatsapp_numbers", []):
                    if wa not in combined_data["whatsapp_numbers"]:
                        combined_data["whatsapp_numbers"].append(wa)

                for platform, link in page_data.get("socials", {}).items():
                    if platform not in combined_data["socials"]:
                        combined_data["socials"][platform] = link

                if page_data.get("has_chatbot"):
                    combined_data["has_chatbot"] = True
                    combined_data["chatbot_type"] = page_data.get("chatbot_type", "")

                if page_data.get("has_google_analytics"):
                    combined_data["has_google_analytics"] = True

                if page_data.get("has_meta_pixel"):
                    combined_data["has_meta_pixel"] = True

                for tool in page_data.get("other_analytics", []):
                    if tool not in combined_data["other_analytics"]:
                        combined_data["other_analytics"].append(tool)

                if page_data.get("cms_platform") and not combined_data["cms_platform"]:
                    combined_data["cms_platform"] = page_data["cms_platform"]

                if combined_data["emails"] and combined_data["whatsapp_numbers"]:
                    break

            except Exception as e:
                self.log.debug("Error analyzing %s: %s", url, e)
                continue

        return combined_data
    
    def _raise_if_captcha(self, page: Page) -> None:
        """Check for CAPTCHA and raise error if detected."""
        try:
            content = page.content().lower()
        except Exception:
            return
        
        if "unusual traffic" in content or "detected unusual" in content or "recaptcha" in content:
            raise CaptchaDetectedError("Captcha or anti-bot challenge detected on Google Maps")
    
    def _human_delay(self, minimum: Optional[float] = None, maximum: Optional[float] = None) -> None:
        """Add human-like delay."""
        min_d = self.min_delay if minimum is None else minimum
        max_d = self.max_delay if maximum is None else maximum
        time.sleep(random.uniform(min_d, max_d))
