"""
Business Intelligence Extractor
Extracts comprehensive business data with focus on ACCURACY and COMPLETENESS.
Enhanced version with better WhatsApp, Instagram, and social media detection.
"""

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse

# Enhanced WhatsApp Patterns - more comprehensive
WHATSAPP_PATTERNS = [
    # Direct wa.me links
    re.compile(r"(?:https?://)?wa\.me/(\+?\d{6,15})", re.I),
    re.compile(r"(?:https?://)?api\.whatsapp\.com/send\?phone=(\+?\d{6,15})", re.I),
    re.compile(r"(?:https?://)?chat\.whatsapp\.com/\w+", re.I),
    re.compile(r"(?:https?://)?wa\.link/\w+", re.I),
    # WhatsApp protocol
    re.compile(r"whatsapp://send\?phone=(\+?\d{6,15})", re.I),
    # JavaScript/data attributes with phone
    re.compile(r"(?:phone|whatsapp|wa)[\"']?\s*[:=]\s*[\"']?(\+?\d{6,15})", re.I),
    # Common widget patterns
    re.compile(r"data-phone[=\"':]+(\+?\d{6,15})", re.I),
    re.compile(r"data-whatsapp[=\"':]+(\+?\d{6,15})", re.I),
    # href patterns
    re.compile(r"href=[\"'](?:https?://)?wa\.me/(\+?\d{6,15})", re.I),
    re.compile(r"href=[\"'](?:https?://)?api\.whatsapp\.com/send\?phone=(\+?\d{6,15})", re.I),
]

# Enhanced Social Media Patterns - more comprehensive
SOCIAL_PATTERNS = {
    "instagram": [
        re.compile(r"(?:https?://)?(?:www\.)?instagram\.com/([a-zA-Z0-9_\.]{1,30})/?(?:\?|$|#|\")", re.I),
        re.compile(r"href=[\"'](?:https?://)?(?:www\.)?instagram\.com/([a-zA-Z0-9_\.]{1,30})/?[\"']", re.I),
        re.compile(r"instagram\.com/([a-zA-Z0-9_\.]{1,30})", re.I),
    ],
    "facebook": [
        re.compile(r"(?:https?://)?(?:www\.)?facebook\.com/([a-zA-Z0-9\.]{1,50})/?(?:\?|$|#|\")", re.I),
        re.compile(r"(?:https?://)?(?:www\.)?fb\.com/([a-zA-Z0-9\.]{1,50})/?", re.I),
        re.compile(r"href=[\"'](?:https?://)?(?:www\.)?facebook\.com/([a-zA-Z0-9\.]{1,50})/?[\"']", re.I),
    ],
    "twitter": [
        re.compile(r"(?:https?://)?(?:www\.)?twitter\.com/([a-zA-Z0-9_]{1,15})/?(?:\?|$|#|\")", re.I),
        re.compile(r"(?:https?://)?(?:www\.)?x\.com/([a-zA-Z0-9_]{1,15})/?", re.I),
    ],
    "linkedin": [
        re.compile(r"(?:https?://)?(?:www\.)?linkedin\.com/company/([a-zA-Z0-9_-]+)/?", re.I),
        re.compile(r"(?:https?://)?(?:www\.)?linkedin\.com/in/([a-zA-Z0-9_-]+)/?", re.I),
    ],
    "tiktok": [
        re.compile(r"(?:https?://)?(?:www\.)?tiktok\.com/@([a-zA-Z0-9_\.]+)/?", re.I),
    ],
    "youtube": [
        re.compile(r"(?:https?://)?(?:www\.)?youtube\.com/(?:@|channel/|c/|user/)?([a-zA-Z0-9_-]+)/?", re.I),
    ],
}

EMAIL_PATTERNS = [
    re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-z]{2,}", re.I),
    re.compile(r"[a-zA-Z0-9._%+-]+\s*\[\s*at\s*\]\s*[a-zA-Z0-9.-]+\s*\[\s*dot\s*\]\s*[a-z]{2,}", re.I),
    re.compile(r"[a-zA-Z0-9._%+-]+\s*\(\s*at\s*\)\s*[a-zA-Z0-9.-]+\s*\(\s*dot\s*\)\s*[a-z]{2,}", re.I),
]

PHONE_PATTERN = re.compile(r"(\+?\d[\d\s().\-]{6,}\d)")

# Generic social handle exclusions
INVALID_SOCIAL_HANDLES = {
    "share", "sharer", "intent", "dialog", "login", "signup", "home", 
    "p", "explore", "accounts", "oauth", "help", "settings", "search",
    "hashtag", "i", "direct", "stories", "reels", "live", "tv",
    "pages", "groups", "events", "marketplace", "gaming", "watch",
    "profile.php", "plugins", "sharer.php", "share.php", "tr",
    "photo.php", "video.php", "reel", "about", "photos", "videos",
    "privacy", "terms", "legal", "policy", "cookies", "contact",
}

CHATBOT_MARKERS = [
    "tidio", "intercom", "drift", "crisp", "livechat", "zendesk", "freshchat",
    "hubspot", "tawk.to", "olark", "smartsupp", "chatra", "jivochat",
    "whatsapp-widget", "click-to-chat", "wa-automate", "wati.io",
]

ANALYTICS_MARKERS = {
    "google_analytics": ["google-analytics.com", "gtag", "ga.js", "analytics.js", "G-", "UA-", "GTM-"],
    "meta_pixel": ["facebook.com/tr", "fbevents.js", "fbq(", "Meta Pixel"],
    "hotjar": ["hotjar.com", "hj.js"],
    "mixpanel": ["mixpanel.com", "mixpanel.init"],
    "hubspot": ["hs-scripts.com", "hs-analytics"],
}

CMS_MARKERS = {
    "wordpress": ["wp-content", "wp-includes", "wordpress"],
    "wix": ["wix.com", "wixstatic.com", "_wix"],
    "squarespace": ["squarespace.com", "sqsp.net"],
    "shopify": ["shopify.com", "cdn.shopify"],
    "webflow": ["webflow.com", "webflow.io"],
    "godaddy": ["godaddy.com", "secureserver.net"],
    "weebly": ["weebly.com"],
    "duda": ["dudaone.com"],
}


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
    has_other_analytics: List[str] = field(default_factory=list)
    cms_platform: str = ""
    is_automated: bool = False  # Has chatbot/autoresponder/booking system
    
    # Metadata
    extraction_quality: str = "unknown"  # low/medium/high based on data found
    extraction_errors: List[str] = field(default_factory=list)
    
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
        if self.name: score += 1
        if self.phone: score += 1
        if self.emails: score += 2
        if self.whatsapp_numbers: score += 2
        if self.website: score += 1
        if self.address: score += 1
        if self.instagram or self.facebook: score += 1
        if self.has_chatbot or self.has_google_analytics: score += 1
        
        if score >= 8:
            return "high"
        elif score >= 5:
            return "medium"
        else:
            return "low"


class WebsiteAnalyzer:
    """Analyzes website for comprehensive business intelligence."""
    
    def __init__(self, html: str, url: str):
        self.html = html
        self.html_lower = html.lower()
        self.url = url
        self.domain = urlparse(url).netloc if url else ""
    
    def extract_whatsapp(self) -> List[str]:
        """Deep WhatsApp number extraction."""
        numbers: List[str] = []
        seen: Set[str] = set()
        
        # Check all WhatsApp patterns
        for pattern in WHATSAPP_PATTERNS:
            for match in pattern.finditer(self.html):
                num = self._normalize_phone(match.group(1) if match.groups() else match.group(0))
                if num and num not in seen:
                    seen.add(num)
                    numbers.append(num)
        
        # Check for WhatsApp widget markers
        whatsapp_markers = [
            "wa.me", "api.whatsapp.com", "wa.link", "whatsapp://",
            "whatsapp-widget", "wa-widget", "click-to-whatsapp",
            "elfsight.com/whatsapp", "getbutton.io/whatsapp",
        ]
        
        # If WhatsApp marker found but no number, try to find nearby phone
        if any(m in self.html_lower for m in whatsapp_markers) and not numbers:
            # Look for phone numbers near WhatsApp mentions
            for match in PHONE_PATTERN.finditer(self.html):
                num = self._normalize_phone(match.group(1))
                if num and len(num) >= 10 and num not in seen:
                    seen.add(num)
                    numbers.append(num)
        
        return numbers
    
    def extract_emails(self) -> List[str]:
        """Thorough email extraction including obfuscated ones."""
        emails: List[str] = []
        seen: Set[str] = set()
        
        # Standard email pattern
        for pattern in EMAIL_PATTERNS:
            for match in pattern.finditer(self.html):
                email = match.group(0).lower()
                # Convert obfuscated formats
                email = re.sub(r"\s*\[\s*at\s*\]\s*", "@", email)
                email = re.sub(r"\s*\(\s*at\s*\)\s*", "@", email)
                email = re.sub(r"\s*\[\s*dot\s*\]\s*", ".", email)
                email = re.sub(r"\s*\(\s*dot\s*\)\s*", ".", email)
                
                if self._is_valid_email(email) and email not in seen:
                    seen.add(email)
                    emails.append(email)
        
        # Check mailto links
        mailto_pattern = re.compile(r'href=["\']mailto:([^"\'?]+)', re.I)
        for match in mailto_pattern.finditer(self.html):
            email = match.group(1).lower().strip()
            if self._is_valid_email(email) and email not in seen:
                seen.add(email)
                emails.append(email)
        
        # Filter out generic/spam emails
        filtered = [e for e in emails if not self._is_generic_email(e)]
        return filtered if filtered else emails[:3]  # Return top 3 if all generic
    
    def extract_social_media(self) -> Dict[str, str]:
        """Extract all social media profiles."""
        socials: Dict[str, str] = {}
        
        for platform, patterns in SOCIAL_PATTERNS.items():
            for pattern in patterns:
                matches = pattern.findall(self.html)
                if matches:
                    # Get the first valid username/handle
                    for match in matches:
                        if match and not self._is_generic_social(match):
                            if platform == "linkedin":
                                socials[platform] = f"https://linkedin.com/company/{match}"
                            else:
                                socials[platform] = f"https://{platform}.com/{match}"
                            break
                if platform in socials:
                    break
        
        return socials
    
    def detect_chatbot(self) -> tuple[bool, str]:
        """Detect if website has chatbot/automation."""
        for marker in CHATBOT_MARKERS:
            if marker in self.html_lower:
                return True, marker
        return False, ""
    
    def detect_analytics(self) -> Dict[str, bool]:
        """Detect analytics and marketing tools."""
        result = {
            "google_analytics": False,
            "meta_pixel": False,
            "other": [],
        }
        
        for tool, markers in ANALYTICS_MARKERS.items():
            for marker in markers:
                if marker.lower() in self.html_lower:
                    if tool == "google_analytics":
                        result["google_analytics"] = True
                    elif tool == "meta_pixel":
                        result["meta_pixel"] = True
                    else:
                        if tool not in result["other"]:
                            result["other"].append(tool)
                    break
        
        return result
    
    def detect_cms(self) -> str:
        """Detect CMS/website builder platform."""
        for cms, markers in CMS_MARKERS.items():
            for marker in markers:
                if marker.lower() in self.html_lower:
                    return cms
        return ""
    
    def _normalize_phone(self, phone: str) -> str:
        """Normalize phone number."""
        if not phone:
            return ""
        digits = re.sub(r"[^\d+]", "", phone)
        if len(digits) < 8:
            return ""
        return digits
    
    def _is_valid_email(self, email: str) -> bool:
        """Validate email format."""
        if not email or "@" not in email:
            return False
        parts = email.split("@")
        if len(parts) != 2:
            return False
        local, domain = parts
        if not local or not domain or "." not in domain:
            return False
        # Filter obvious non-emails
        invalid_domains = ["example.com", "test.com", "email.com", "domain.com", "sentry.io"]
        if domain in invalid_domains:
            return False
        return True
    
    def _is_generic_email(self, email: str) -> bool:
        """Check if email is generic/support type."""
        generic = ["noreply", "no-reply", "support@", "info@", "admin@", "webmaster@", "postmaster@"]
        return any(g in email.lower() for g in generic)
    
    def _is_generic_social(self, handle: str) -> bool:
        """Check if social handle is generic."""
        generic = ["share", "sharer", "intent", "dialog", "login", "signup", "home"]
        return handle.lower() in generic


def validate_phone(phone: str) -> tuple[bool, str]:
    """
    Validate phone number format and return (is_valid, normalized_phone).
    Supports international formats.
    """
    if not phone:
        return False, ""
    
    # Remove all non-digit characters except +
    cleaned = re.sub(r"[^\d+]", "", phone)
    
    # Must have at least 8 digits
    digits_only = cleaned.replace("+", "")
    if len(digits_only) < 8 or len(digits_only) > 15:
        return False, ""
    
    # Normalize: ensure + prefix for international
    if cleaned.startswith("+"):
        return True, cleaned
    elif len(digits_only) >= 10:
        return True, digits_only
    
    return False, ""


def validate_email(email: str) -> tuple[bool, str]:
    """
    Validate email format and return (is_valid, normalized_email).
    Filters out obvious fake/test emails.
    """
    if not email:
        return False, ""
    
    email = email.lower().strip()
    
    # Basic format check
    if "@" not in email or email.count("@") != 1:
        return False, ""
    
    local, domain = email.split("@")
    
    # Check local part
    if not local or len(local) > 64:
        return False, ""
    
    # Check domain
    if not domain or "." not in domain or len(domain) > 255:
        return False, ""
    
    # Filter obvious fake domains
    fake_domains = [
        "example.com", "test.com", "email.com", "domain.com",
        "yoursite.com", "website.com", "company.com", "business.com",
        "mail.com", "fake.com", "sample.com", "demo.com",
        "sentry.io", "wixpress.com", "sentry-next.wixpress.com"
    ]
    if domain in fake_domains:
        return False, ""
    
    # Filter image/asset emails
    if any(ext in email for ext in [".png", ".jpg", ".gif", ".svg", ".webp"]):
        return False, ""
    
    return True, email


def validate_whatsapp(number: str) -> tuple[bool, str]:
    """
    Validate WhatsApp number format.
    WhatsApp requires country code + national number.
    """
    if not number:
        return False, ""
    
    # Clean the number
    cleaned = re.sub(r"[^\d+]", "", number)
    digits_only = cleaned.replace("+", "")
    
    # WhatsApp numbers are typically 10-15 digits with country code
    if len(digits_only) < 10 or len(digits_only) > 15:
        return False, ""
    
    # Return normalized format
    if cleaned.startswith("+"):
        return True, cleaned
    else:
        return True, digits_only


def validate_url(url: str) -> tuple[bool, str]:
    """
    Validate URL format and return (is_valid, normalized_url).
    """
    if not url:
        return False, ""
    
    url = url.strip()
    
    # Add protocol if missing
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"
    
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        if parsed.netloc and "." in parsed.netloc:
            return True, url
    except Exception:
        pass
    
    return False, ""


def deduplicate_leads(leads: List[Dict]) -> List[Dict]:
    """
    Remove duplicate leads based on phone, email, or website.
    Keeps the lead with most data.
    """
    seen_phones = set()
    seen_emails = set()
    seen_websites = set()
    unique_leads = []
    
    # Sort by quality (high first) to keep best version
    def quality_score(lead):
        score = 0
        if lead.get("email"): score += 2
        if lead.get("whatsapp"): score += 2
        if lead.get("instagram") or lead.get("facebook"): score += 1
        if lead.get("has_chatbot") == "Yes": score += 1
        return score
    
    sorted_leads = sorted(leads, key=quality_score, reverse=True)
    
    for lead in sorted_leads:
        phone = lead.get("phone", "").replace(" ", "").replace("-", "")
        email = lead.get("email", "").lower()
        website = lead.get("website", "").lower().rstrip("/")
        
        # Check for duplicates
        is_duplicate = False
        if phone and phone in seen_phones:
            is_duplicate = True
        if email and email in seen_emails:
            is_duplicate = True
        if website and website in seen_websites:
            is_duplicate = True
        
        if not is_duplicate:
            unique_leads.append(lead)
            if phone:
                seen_phones.add(phone)
            if email:
                seen_emails.add(email)
            if website:
                seen_websites.add(website)
    
    return unique_leads


def analyze_website(html: str, url: str) -> Dict:
    """Analyze website and return all extracted data."""
    analyzer = WebsiteAnalyzer(html, url)
    
    socials = analyzer.extract_social_media()
    has_chatbot, chatbot_type = analyzer.detect_chatbot()
    analytics = analyzer.detect_analytics()
    
    return {
        "emails": analyzer.extract_emails(),
        "whatsapp_numbers": analyzer.extract_whatsapp(),
        "socials": socials,
        "has_chatbot": has_chatbot,
        "chatbot_type": chatbot_type,
        "has_google_analytics": analytics["google_analytics"],
        "has_meta_pixel": analytics["meta_pixel"],
        "other_analytics": analytics["other"],
        "cms_platform": analyzer.detect_cms(),
    }
