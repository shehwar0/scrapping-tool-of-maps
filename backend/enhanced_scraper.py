"""
Enhanced Google Maps Scraper
Extracts comprehensive business details with focus on ACCURACY and COMPLETENESS.
"""

import logging
import random
import re
import time
from threading import Event
from typing import Callable, Dict, List, Optional, Set
from urllib.parse import quote_plus, urlparse

import asyncio

from playwright.async_api import async_playwright, Page, BrowserContext
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from business_extractor import BusinessData, analyze_website

PHONE_REGEX = re.compile(r"(\+?\d[\d\s()\-]{6,}\d)")
MAX_RESULTS_CAP = 500
RESULT_SCAN_WINDOW = 140

# Pages to check for contact info (in order of priority)
CONTACT_PAGES = ["", "/contact", "/contact-us", "/about", "/about-us", "/team"]


class CaptchaDetectedError(RuntimeError):
    pass


class EnhancedGoogleMapsScraper:
    """Enhanced scraper that extracts comprehensive business intelligence."""
    
    def __init__(
        self,
        max_results: int = 50,
        headless: bool = False,
        min_delay: float = 0.7,
        max_delay: float = 1.6,
        website_filter: str = "all",
        logger: Optional[logging.Logger] = None,
        concurrent_extractions: int = 3,
        progress_callback: Optional[Callable[[Dict[str, str]], None]] = None,
    ) -> None:
        self.max_results = max(1, min(max_results, MAX_RESULTS_CAP))
        self.headless = headless
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.website_filter = website_filter if website_filter in {"all", "with", "without"} else "all"
        self.log = logger or logging.getLogger(__name__)
        self.concurrent_extractions = concurrent_extractions
        self.progress_callback = progress_callback
        self._website_cache: Dict[str, Dict] = {}
    
    async def scrape(
        self,
        keyword: str,
        location: str,
        stop_event: Optional[Event] = None,
    ) -> List[Dict[str, str]]:
        """Main scrape method - async for better performance."""
        query = f"{keyword} in {location}".strip()
        stop_event = stop_event or Event()
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1400, "height": 1000},
            )
            page = await context.new_page()
            
            try:
                await self._open_and_search(page, query)
                place_urls = await self._collect_place_urls(page, stop_event)
                leads = await self._collect_lead_details(context, place_urls, stop_event)
                return [lead.to_dict() for lead in leads]
            finally:
                await context.close()
                await browser.close()
    
    def scrape_sync(
        self,
        keyword: str,
        location: str,
        stop_event: Optional[Event] = None,
    ) -> List[Dict[str, str]]:
        """Synchronous wrapper for backwards compatibility."""
        try:
            # Try to get existing event loop
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If loop is already running, create a new one in a thread
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, self.scrape(keyword, location, stop_event))
                    return future.result()
            else:
                return loop.run_until_complete(self.scrape(keyword, location, stop_event))
        except RuntimeError:
            # No event loop exists, create one
            return asyncio.run(self.scrape(keyword, location, stop_event))
    
    async def _open_and_search(self, page: Page, query: str) -> None:
        """Navigate to Google Maps and search."""
        encoded_query = quote_plus(query)
        await page.goto(f"https://www.google.com/maps/search/{encoded_query}", timeout=90000)
        await page.wait_for_timeout(1200)
        await self._maybe_accept_consent(page)
        await self._raise_if_captcha(page)
        
        if await self._wait_for_any(page, ["div[role='feed']", "a.hfpxzc"], timeout_ms=45000):
            await self._human_delay()
            return
        
        search_input = await self._find_search_input(page)
        if search_input:
            await search_input.fill(query)
            await self._human_delay()
            await search_input.press("Enter")
        
        if not await self._wait_for_any(page, ["div[role='feed']", "a.hfpxzc", "h1.DUwDvf"], timeout_ms=45000):
            raise RuntimeError(
                "Google Maps results did not load. This can happen due to consent/CAPTCHA, network issues, or UI changes."
            )
        
        await self._human_delay()
        await self._raise_if_captcha(page)
    
    async def _find_search_input(self, page: Page):
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
                if await locator.count() > 0:
                    await locator.wait_for(state="visible", timeout=6000)
                    return locator
            except Exception:
                continue
        return None
    
    async def _wait_for_any(self, page: Page, selectors: List[str], timeout_ms: int) -> bool:
        """Wait for any of the given selectors to appear."""
        import time
        deadline = time.time() + (timeout_ms / 1000)
        while time.time() < deadline:
            for selector in selectors:
                try:
                    if await page.locator(selector).first.count() > 0:
                        return True
                except Exception:
                    continue
            await page.wait_for_timeout(400)
        return False
    
    async def _maybe_accept_consent(self, page: Page) -> None:
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
                if await button.count() > 0 and await button.is_visible():
                    await button.click(timeout=3000)
                    await page.wait_for_timeout(1200)
                    return
            except Exception:
                continue
    
    async def _collect_place_urls(self, page: Page, stop_event: Event) -> List[str]:
        """Collect all place URLs from search results."""
        discovered: List[str] = []
        seen: Set[str] = set()
        stagnant_rounds = 0
        max_stagnant_rounds = 14 if self.max_results > 100 else 8
        scroll_delay_min, scroll_delay_max = (0.25, 0.5) if self.max_results > 100 else (0.3, 0.6)
        
        current_url = page.url or ""
        if "/maps/place/" in current_url:
            return [current_url]
        
        while len(discovered) < self.max_results and stagnant_rounds < max_stagnant_rounds and not stop_event.is_set():
            before = len(discovered)
            hrefs: List[str] = []
            
            try:
                hrefs = await page.eval_on_selector_all(
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
                count = await links.count()
                start_idx = max(0, count - RESULT_SCAN_WINDOW)
                
                for idx in range(start_idx, count):
                    if stop_event.is_set() or len(discovered) >= self.max_results:
                        break
                    href = ""
                    for _ in range(2):
                        try:
                            href = await links.nth(idx).get_attribute("href") or ""
                            break
                        except PlaywrightTimeoutError:
                            await self._human_delay(0.6, 1.2)
                    if href and href not in seen:
                        seen.add(href)
                        discovered.append(href)
            
            if len(discovered) == before:
                stagnant_rounds += 1
            else:
                stagnant_rounds = 0
            
            feed = page.locator("div[role='feed']").first
            try:
                await feed.evaluate("el => el.scrollBy(0, el.scrollHeight)")
            except Exception:
                await page.mouse.wheel(0, 4000)
            
            await self._human_delay(scroll_delay_min, scroll_delay_max)
            await self._raise_if_captcha(page)
        
        self.log.info("Discovered %s place urls", len(discovered))
        return discovered[: self.max_results]
    
    async def _collect_lead_details(
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
            page = await context.new_page()
            try:
                lead = await self._extract_full_listing(page, place_url)
                if lead:
                    if self._passes_website_filter(lead.website):
                        lead.extraction_quality = lead.calculate_quality()
                        leads.append(lead)
                        if self.progress_callback:
                            try:
                                self.progress_callback(lead.to_dict())
                            except Exception:
                                # Progress callbacks should never interrupt scraping.
                                pass
            finally:
                await page.close()
            
            await self._human_delay(0.2, 0.45)
        
        return leads
    
    def _passes_website_filter(self, website: str) -> bool:
        """Check if listing passes website filter."""
        has_website = bool((website or "").strip())
        if self.website_filter == "with":
            return has_website
        if self.website_filter == "without":
            return not has_website
        return True
    
    async def _extract_full_listing(self, page: Page, place_url: str) -> Optional[BusinessData]:
        """Extract comprehensive data from a single Google Maps listing."""
        for attempt in range(2):
            try:
                await page.goto(place_url, timeout=60000)
                await page.wait_for_timeout(1500)
                await self._raise_if_captcha(page)
                
                # Create business data object
                data = BusinessData()
                data.google_maps_url = place_url
                
                # Extract all Google Maps data
                data.name = await self._safe_text(page, "h1.DUwDvf", fallback_selector="h1")
                data.phone = await self._extract_phone(page)
                data.website = await self._extract_website(page)
                data.has_website = bool(data.website)
                data.address = await self._extract_address(page)
                data.rating, data.review_count = await self._extract_rating(page)
                data.category = await self._extract_category(page)
                data.business_hours = await self._extract_hours(page)
                data.plus_code = await self._extract_plus_code(page)
                
                # Extract social links from Google Maps (if shown)
                gmaps_socials = await self._extract_social_from_gmaps(page)
                data.instagram = gmaps_socials.get("instagram", "")
                data.facebook = gmaps_socials.get("facebook", "")
                data.twitter = gmaps_socials.get("twitter", "")
                
                # If has website, extract additional data
                if data.website:
                    cache_key = self._website_cache_key(data.website)
                    website_data = self._website_cache.get(cache_key)
                    if website_data is None:
                        website_data = await self._analyze_website(page, data.website)
                        self._website_cache[cache_key] = dict(website_data)
                    
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
                await self._human_delay(1.2, 2.2)
        
        return None
    
    async def _safe_text(self, page: Page, selector: str, fallback_selector: str = "") -> str:
        """Safely extract text from element."""
        try:
            locator = page.locator(selector).first
            if await locator.count() > 0:
                value = (await locator.inner_text(timeout=4000)).strip()
                if value:
                    return value
        except Exception:
            pass
        
        if fallback_selector:
            try:
                locator = page.locator(fallback_selector).first
                if await locator.count() > 0:
                    value = (await locator.inner_text(timeout=3000)).strip()
                    if value:
                        return value
            except Exception:
                pass
        
        return ""
    
    async def _extract_phone(self, page: Page) -> str:
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
                if await locator.count() == 0:
                    continue
                text = self._clean_phone_text(await locator.inner_text(timeout=3500))
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
    
    async def _extract_website(self, page: Page) -> str:
        """Extract website URL from Google Maps listing."""
        selectors = [
            "a[data-item-id='authority']",
            "a[aria-label*='Website']",
            "a[aria-label*='website']",
        ]
        
        for selector in selectors:
            try:
                anchor = page.locator(selector).first
                if await anchor.count() == 0:
                    continue
                href = await anchor.get_attribute("href") or ""
                if href and href.startswith("http"):
                    return href
            except Exception:
                continue
        
        return ""
    
    async def _extract_address(self, page: Page) -> str:
        """Extract business address from Google Maps."""
        selectors = [
            "button[data-item-id='address']",
            "button[aria-label*='Address']",
            "button[aria-label*='address']",
        ]
        
        for selector in selectors:
            try:
                locator = page.locator(selector).first
                if await locator.count() > 0:
                    text = (await locator.inner_text(timeout=3000)).strip()
                    if text:
                        return text
            except Exception:
                continue
        
        return ""
    
    async def _extract_rating(self, page: Page) -> tuple[float, int]:
        """Extract rating and review count."""
        try:
            # Try to find rating span
            rating_el = page.locator("span.ceNzKf, div.F7nice span[aria-hidden='true']").first
            if await rating_el.count() > 0:
                rating_text = await rating_el.inner_text(timeout=3000)
                rating = float(re.search(r"[\d.]+", rating_text).group())
            else:
                rating = 0.0
            
            # Try to find review count
            review_el = page.locator("span.UY7F9, button[jsaction*='review'] span").first
            if await review_el.count() > 0:
                review_text = await review_el.inner_text(timeout=3000)
                review_match = re.search(r"[\d,]+", review_text.replace(",", ""))
                review_count = int(review_match.group()) if review_match else 0
            else:
                review_count = 0
            
            return rating, review_count
        except Exception:
            return 0.0, 0
    
    async def _extract_category(self, page: Page) -> str:
        """Extract business category."""
        try:
            # Category is often shown as a button below the name
            category_el = page.locator("button.DkEaL, span.DkEaL").first
            if await category_el.count() > 0:
                return (await category_el.inner_text(timeout=3000)).strip()
        except Exception:
            pass
        return ""
    
    async def _extract_hours(self, page: Page) -> str:
        """Extract business hours."""
        try:
            hours_button = page.locator("button[data-item-id*='oh'], button[aria-label*='hour']").first
            if await hours_button.count() > 0:
                return (await hours_button.inner_text(timeout=3000)).strip()
        except Exception:
            pass
        return ""
    
    async def _extract_plus_code(self, page: Page) -> str:
        """Extract Plus Code (location code)."""
        try:
            plus_el = page.locator("button[data-item-id='oloc']").first
            if await plus_el.count() > 0:
                return (await plus_el.inner_text(timeout=3000)).strip()
        except Exception:
            pass
        return ""
    
    async def _extract_social_from_gmaps(self, page: Page) -> Dict[str, str]:
        """Extract social links shown directly on Google Maps."""
        socials = {}
        
        try:
            # Look for social media links in the info section
            links = await page.eval_on_selector_all(
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

    def _website_cache_key(self, website_url: str) -> str:
        if not website_url:
            return ""
        normalized = website_url if website_url.startswith(("http://", "https://")) else f"https://{website_url}"
        parsed = urlparse(normalized)
        host = (parsed.netloc or parsed.path).lower().strip()
        if host.startswith("www."):
            host = host[4:]
        return host
    
    async def _analyze_website(self, page: Page, website_url: str) -> Dict:
        """Navigate to website and analyze for contact info and tech stack."""
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
        
        # Normalize URL
        if not website_url.startswith(("http://", "https://")):
            website_url = f"https://{website_url}"
        
        base_url = website_url.rstrip("/")
        
        # Check multiple pages for comprehensive extraction
        for path in CONTACT_PAGES:
            try:
                url = f"{base_url}{path}"
                response = await page.goto(url, timeout=15000, wait_until="domcontentloaded")
                
                if not response or response.status >= 400:
                    continue
                
                await page.wait_for_timeout(1000)
                
                # Get page content
                html = await page.content()
                
                # Analyze this page
                page_data = analyze_website(html, url)
                
                # Merge results
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
                
                # If we found enough data, stop early
                if combined_data["emails"] and combined_data["whatsapp_numbers"]:
                    break
                    
            except Exception as e:
                self.log.debug("Error analyzing %s: %s", url, e)
                continue
        
        return combined_data
    
    async def _raise_if_captcha(self, page: Page) -> None:
        """Check for CAPTCHA and raise error if detected."""
        try:
            content = (await page.content()).lower()
        except Exception:
            return
        
        if "unusual traffic" in content or "detected unusual" in content or "recaptcha" in content:
            raise CaptchaDetectedError("Captcha or anti-bot challenge detected on Google Maps")
    
    async def _human_delay(self, minimum: Optional[float] = None, maximum: Optional[float] = None) -> None:
        """Add human-like delay."""
        min_d = self.min_delay if minimum is None else minimum
        max_d = self.max_delay if maximum is None else maximum
        await asyncio.sleep(random.uniform(min_d, max_d))


# Backwards compatible sync wrapper
class GoogleMapsScraper(EnhancedGoogleMapsScraper):
    """Backwards compatible wrapper that uses sync interface."""
    
    def scrape(
        self,
        keyword: str,
        location: str,
        stop_event: Optional[Event] = None,
    ) -> List[Dict[str, str]]:
        """Synchronous scrape method for backwards compatibility."""
        return self.scrape_sync(keyword, location, stop_event)
