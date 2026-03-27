import csv
import logging
import os
import re
from datetime import datetime
from threading import Event
from typing import Dict, List

from flask import Flask, jsonify, request, send_file

# Import all scrapers for different modes
# Ultra Deep - uses ALL engines in parallel with cross-verification
try:
    from ultra_scraper import UltraDeepScraper
except ImportError:
    UltraDeepScraper = None

# Deep - multi-source (Maps + Website + Google Search)
try:
    from deep_scraper import DeepBusinessScraper
except ImportError:
    DeepBusinessScraper = None

# Enhanced - comprehensive extraction
try:
    from enhanced_scraper_sync import GoogleMapsScraper as EnhancedScraper
except ImportError:
    try:
        from enhanced_scraper import GoogleMapsScraper as EnhancedScraper
    except ImportError:
        EnhancedScraper = None

# Basic - fast Maps-only extraction
try:
    from scraper import GoogleMapsScraper as BasicScraper, CaptchaDetectedError
except ImportError:
    BasicScraper = None
    class CaptchaDetectedError(RuntimeError):
        pass

# Import history manager for deduplication
try:
    from scrape_history import get_history, ScrapeHistory
    scrape_history = get_history()
except ImportError:
    scrape_history = None

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FRONTEND_DIR = os.path.join(BASE_DIR, "frontend")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

app = Flask(__name__, static_folder=FRONTEND_DIR, static_url_path="")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("lead-scraper")

SCRAPE_STATE = {
    "running": False,
    "status": "idle",
    "message": "Ready",
    "results": [],
    "csv_path": "",
}
STOP_EVENT = Event()


@app.get("/")
def index():
    return app.send_static_file("index.html")


@app.get("/favicon.ico")
def favicon():
    return ("", 204)


@app.get("/modes")
def get_available_modes() -> Dict:
    """Return available extraction modes based on installed scrapers."""
    modes = []
    
    if UltraDeepScraper:
        modes.append({
            "value": "ultra",
            "label": "🚀 Ultra Deep (ALL engines + Cross-verification)",
            "description": "Uses ALL extraction engines in parallel with cross-verification. Highest accuracy, slowest speed."
        })
    
    if DeepBusinessScraper:
        modes.append({
            "value": "deep",
            "label": "🔍 Deep (Maps + Website + Google Search)",
            "description": "Multi-source extraction with Google Search cross-verification. Finds more Instagram/Facebook/WhatsApp."
        })
    
    if EnhancedScraper:
        modes.append({
            "value": "enhanced",
            "label": "⚙️ Enhanced (Maps + Website analysis)",
            "description": "Google Maps + comprehensive website analysis. Good balance of speed and data quality."
        })
    
    if BasicScraper:
        modes.append({
            "value": "basic",
            "label": "⚡ Basic (Fastest, Maps only)",
            "description": "Fast Maps-only extraction. Gets name, phone, address, rating, website from Google Maps only."
        })
    
    return jsonify({"modes": modes})


@app.get("/status")
def status() -> Dict:
    return jsonify(
        {
            "running": SCRAPE_STATE["running"],
            "status": SCRAPE_STATE["status"],
            "message": SCRAPE_STATE["message"],
            "count": len(SCRAPE_STATE["results"]),
        }
    )


@app.post("/stop")
def stop_scrape() -> Dict:
    if SCRAPE_STATE["running"]:
        STOP_EVENT.set()
        SCRAPE_STATE["status"] = "stopping"
        SCRAPE_STATE["message"] = "Stop requested. Finishing current step..."
        return jsonify({"ok": True, "message": "Stop signal sent"})
    return jsonify({"ok": False, "message": "No active scrape job"}), 400


# ============================================================================
# HISTORY MANAGEMENT ENDPOINTS
# ============================================================================

@app.get("/history/stats")
def get_history_stats() -> Dict:
    """Get history statistics for a search query."""
    if not scrape_history:
        return jsonify({"error": "History not available"}), 500
    
    keyword = request.args.get("keyword", "")
    location = request.args.get("location", "")
    
    stats = scrape_history.get_stats(keyword, location)
    return jsonify(stats)


@app.post("/history/clear")
def clear_history() -> Dict:
    """Clear history for a search query or all history."""
    if not scrape_history:
        return jsonify({"error": "History not available"}), 500
    
    payload = request.get_json(silent=True) or {}
    keyword = (payload.get("keyword") or "").strip()
    location = (payload.get("location") or "").strip()
    clear_all = payload.get("clear_all", False)
    
    if clear_all:
        count = scrape_history.clear_all_history()
        return jsonify({"ok": True, "message": f"Cleared all history ({count} businesses)", "cleared": count})
    elif keyword and location:
        count = scrape_history.clear_search_history(keyword, location)
        return jsonify({"ok": True, "message": f"Cleared history for '{keyword}' in '{location}' ({count} businesses)", "cleared": count})
    else:
        return jsonify({"error": "Provide keyword and location, or set clear_all=true"}), 400


@app.get("/history/previous")
def get_previous_scraped() -> Dict:
    """Get list of previously scraped businesses for a search."""
    if not scrape_history:
        return jsonify({"error": "History not available"}), 500
    
    keyword = request.args.get("keyword", "")
    location = request.args.get("location", "")
    limit = int(request.args.get("limit", 100))
    
    if not keyword or not location:
        return jsonify({"error": "keyword and location are required"}), 400
    
    previous = scrape_history.get_previously_scraped(keyword, location, limit)
    return jsonify({"count": len(previous), "businesses": previous})


@app.post("/scrape")
def scrape() -> Dict:
    if SCRAPE_STATE["running"]:
        return jsonify({"error": "A scrape is already running"}), 409

    payload = request.get_json(silent=True) or {}
    keyword = (payload.get("keyword") or "").strip()
    location = (payload.get("location") or "").strip()
    website_filter = (payload.get("website_filter") or "all").strip().lower()
    extraction_mode = (payload.get("extraction_mode") or "deep").strip().lower()
    
    if website_filter not in {"all", "with", "without"}:
        website_filter = "all"

    # Backward compatibility with older frontend payloads.
    if "only_with_website" in payload and bool(payload.get("only_with_website")):
        website_filter = "with"

    # Additional options
    deep_search = bool(payload.get("deep_search", True))
    verify_socials = bool(payload.get("verify_socials", True))
    skip_duplicates = bool(payload.get("skip_duplicates", True))  # NEW: Skip previously scraped

    try:
        requested_max_results = int(payload.get("max_results", 50))
    except (TypeError, ValueError):
        requested_max_results = 50
    max_results = max(1, min(requested_max_results, 500))
    headless = bool(payload.get("headless", False))

    if not keyword or not location:
        return jsonify({"error": "keyword and location are required"}), 400

    # Mode descriptions for status message
    mode_descriptions = {
        "basic": "Basic (Maps only)",
        "enhanced": "Enhanced (Maps + Website)",
        "deep": "Deep (Maps + Website + Google Search)",
        "ultra": "Ultra Deep (ALL engines + Cross-verification)",
    }
    mode_desc = mode_descriptions.get(extraction_mode, extraction_mode)
    
    # Add deduplication info
    if skip_duplicates and scrape_history:
        stats = scrape_history.get_stats(keyword, location)
        prev_count = stats.get("search_total", 0)
        if prev_count > 0:
            mode_desc += f" (skipping {prev_count} already scraped)"

    SCRAPE_STATE["running"] = True
    SCRAPE_STATE["status"] = "running"
    SCRAPE_STATE["message"] = f"🔍 {mode_desc} scraping for '{keyword}' in '{location}'"
    SCRAPE_STATE["results"] = []
    STOP_EVENT.clear()

    try:
        # Choose scraper based on extraction mode
        scraper = None
        
        if extraction_mode == "ultra":
            # Ultra Deep - uses ALL engines in parallel with cross-verification
            if UltraDeepScraper:
                log.info("Using ULTRA DEEP scraper (all engines + cross-verification)")
                scraper = UltraDeepScraper(
                    max_results=max_results,
                    headless=headless,
                    website_filter=website_filter,
                    verify_socials=verify_socials,
                    skip_duplicates=skip_duplicates,
                    logger=log,
                )
            else:
                log.warning("UltraDeepScraper not available, falling back to Deep")
                extraction_mode = "deep"
        
        if extraction_mode == "deep" and scraper is None:
            # Deep - multi-source extraction (Maps + Website + Google Search)
            if DeepBusinessScraper:
                log.info("Using DEEP scraper (Maps + Website + Google Search)")
                scraper = DeepBusinessScraper(
                    max_results=max_results,
                    headless=headless,
                    website_filter=website_filter,
                    deep_search=deep_search,
                    skip_duplicates=skip_duplicates,
                    logger=log,
                )
            else:
                log.warning("DeepBusinessScraper not available, falling back to Enhanced")
                extraction_mode = "enhanced"
        
        if extraction_mode == "enhanced" and scraper is None:
            # Enhanced - comprehensive extraction
            if EnhancedScraper:
                log.info("Using ENHANCED scraper (Maps + Website analysis)")
                scraper = EnhancedScraper(
                    max_results=max_results,
                    headless=headless,
                    website_filter=website_filter,
                    logger=log,
                )
            else:
                log.warning("EnhancedScraper not available, falling back to Basic")
                extraction_mode = "basic"
        
        if extraction_mode == "basic" or scraper is None:
            # Basic - fast Maps-only extraction
            if BasicScraper:
                log.info("Using BASIC scraper (Maps only)")
                scraper = BasicScraper(
                    max_results=max_results,
                    headless=headless,
                    website_filter=website_filter,
                    logger=log,
                )
            else:
                return jsonify({"error": "No scraper available"}), 500
        
        results = scraper.scrape(keyword=keyword, location=location, stop_event=STOP_EVENT)
        SCRAPE_STATE["results"] = results
        log.info("Scraping completed. Found %d results", len(results))

        csv_path = _write_csv(keyword, location, results)
        SCRAPE_STATE["csv_path"] = csv_path
        log.info("CSV file written to: %s", csv_path)

        if STOP_EVENT.is_set():
            SCRAPE_STATE["status"] = "stopped"
            SCRAPE_STATE["message"] = "Scrape stopped by user"
        else:
            SCRAPE_STATE["status"] = "completed"
            SCRAPE_STATE["message"] = f"Completed. {len(results)} leads collected"

        return jsonify(
            {
                "status": SCRAPE_STATE["status"],
                "message": SCRAPE_STATE["message"],
                "count": len(results),
                "results": results,
                "csv_file": os.path.basename(csv_path),
            }
        )
    except CaptchaDetectedError as exc:
        log.error("Captcha detected: %s", exc)
        SCRAPE_STATE["status"] = "captcha"
        SCRAPE_STATE["message"] = "Captcha detected. Please try again later."
        return jsonify({"error": SCRAPE_STATE["message"]}), 429
    except Exception as exc:
        log.exception("Scrape failed: %s", exc)
        SCRAPE_STATE["status"] = "error"
        SCRAPE_STATE["message"] = f"Scrape failed: {exc}"
        return jsonify({"error": SCRAPE_STATE["message"]}), 500
    finally:
        SCRAPE_STATE["running"] = False


@app.get("/download")
def download_csv():
    csv_path = SCRAPE_STATE.get("csv_path")
    if not csv_path or not os.path.exists(csv_path):
        return jsonify({"error": "No CSV file available. Run scraping first."}), 404
    return send_file(csv_path, as_attachment=True)


def _write_csv(keyword: str, location: str, leads: List[Dict[str, str]]) -> str:
    safe_keyword = _sanitize_token(keyword)
    safe_location = _sanitize_token(location)
    filename = f"leads_{safe_keyword}_{safe_location}.csv"
    path = os.path.join(OUTPUT_DIR, filename)
    
    log.info("Writing CSV to: %s", path)
    log.info("Number of leads to write: %d", len(leads))

    # Enhanced CSV with all extracted fields including verification data
    fieldnames = [
        "Name", "Phone", "Email", "All Emails", "WhatsApp", "All WhatsApp",
        "Website", "Has Website", "Address", "Rating", "Reviews",
        "Category", "Business Hours",
        "Instagram", "Facebook", "Twitter", "LinkedIn", "TikTok", "YouTube",
        "Has Chatbot", "Chatbot Type", "Has Google Analytics", "Has Meta Pixel",
        "CMS Platform", "Is Automated", "Quality Score", "Verification Score",
        "Data Sources", "Google Maps URL"
    ]

    try:
        with open(path, "w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for lead in leads:
                writer.writerow(
                    {
                        "Name": lead.get("name", ""),
                        "Phone": lead.get("phone", ""),
                        "Email": lead.get("email", ""),
                        "All Emails": lead.get("all_emails", ""),
                        "WhatsApp": lead.get("whatsapp", ""),
                        "All WhatsApp": lead.get("all_whatsapp", ""),
                        "Website": lead.get("website", ""),
                        "Has Website": lead.get("has_website", ""),
                        "Address": lead.get("address", ""),
                        "Rating": lead.get("rating", ""),
                        "Reviews": lead.get("review_count", ""),
                        "Category": lead.get("category", ""),
                        "Business Hours": lead.get("business_hours", ""),
                        "Instagram": lead.get("instagram", ""),
                        "Facebook": lead.get("facebook", ""),
                        "Twitter": lead.get("twitter", ""),
                        "LinkedIn": lead.get("linkedin", ""),
                        "TikTok": lead.get("tiktok", ""),
                        "YouTube": lead.get("youtube", ""),
                        "Has Chatbot": lead.get("has_chatbot", ""),
                        "Chatbot Type": lead.get("chatbot_type", ""),
                        "Has Google Analytics": lead.get("has_google_analytics", ""),
                        "Has Meta Pixel": lead.get("has_meta_pixel", ""),
                        "CMS Platform": lead.get("cms_platform", ""),
                        "Is Automated": lead.get("is_automated", ""),
                        "Quality Score": lead.get("quality_score", ""),
                        "Verification Score": lead.get("verification_score", ""),
                        "Data Sources": lead.get("data_sources", ""),
                        "Google Maps URL": lead.get("google_maps_url", ""),
                    }
                )
        log.info("CSV file successfully written: %s", path)
    except Exception as e:
        log.error("Failed to write CSV file: %s", e)
        raise

    return path


def _sanitize_token(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"[^a-z0-9_]+", "", value)
    return value or datetime.now().strftime("%Y%m%d%H%M%S")


if __name__ == "__main__":
    # Disable debug mode to prevent auto-reload during long scraping sessions
    app.run(host="127.0.0.1", port=5000, debug=False, threaded=True)
