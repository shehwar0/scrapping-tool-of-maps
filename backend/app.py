import csv
import logging
import os
import re
from pathlib import Path
from datetime import datetime
from threading import Event
from typing import Dict, List, Optional, Set

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
            "results": SCRAPE_STATE["results"],
        }
    )


@app.post("/stop")
def stop_scrape() -> Dict:
    if SCRAPE_STATE["running"]:
        STOP_EVENT.set()
        SCRAPE_STATE["status"] = "stopping"
        SCRAPE_STATE["message"] = "Stop requested. Finishing current step..."
        return jsonify(
            {
                "ok": True,
                "message": "Stop signal sent",
                "count": len(SCRAPE_STATE["results"]),
                "results": SCRAPE_STATE["results"],
            }
        )
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


@app.get("/history/output-files")
def get_output_history_files() -> Dict:
    """List CSV output files that can be used as selectable history sources."""
    files = _list_output_history_files()
    return jsonify({"count": len(files), "files": files})


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
    selected_history_files = _normalize_history_file_selection(payload.get("selected_history_files"))

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
    exclusion_business_ids: Set[str] = set()
    excluded_by_history = 0

    if skip_duplicates and scrape_history:
        exclusion_business_ids.update(scrape_history.get_existing_business_ids(keyword, location))

    if selected_history_files and scrape_history:
        selected_paths = [Path(os.path.join(OUTPUT_DIR, name)) for name in selected_history_files]
        imported_count, selected_ids = scrape_history.import_output_files_to_history(selected_paths)
        exclusion_business_ids.update(selected_ids)
        log.info(
            "Using %d selected output files for exclusion (%d IDs, %d newly imported)",
            len(selected_history_files),
            len(selected_ids),
            imported_count,
        )
        mode_desc += f" + selected-files({len(selected_history_files)})"
    elif selected_history_files:
        log.warning("Selected history files were provided, but history manager is unavailable")
    
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
    SCRAPE_STATE["csv_path"] = ""
    STOP_EVENT.clear()

    partial_results: List[Dict[str, str]] = []

    def report_progress(lead: Dict[str, str]) -> None:
        if not isinstance(lead, dict):
            return

        lead_copy = dict(lead)
        if exclusion_business_ids and scrape_history:
            business_id = scrape_history.get_business_id(lead_copy)
            if business_id and business_id in exclusion_business_ids:
                return

        lead_copy["whatsapp_wa_me_links"] = _build_whatsapp_wa_me_links(lead_copy)
        partial_results.append(lead_copy)
        SCRAPE_STATE["results"] = partial_results.copy()

        if STOP_EVENT.is_set():
            SCRAPE_STATE["message"] = f"Stopping... {len(partial_results)} leads collected so far"
        else:
            SCRAPE_STATE["message"] = f"Running... {len(partial_results)} leads collected"

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
                    progress_callback=report_progress,
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
                    progress_callback=report_progress,
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
                    progress_callback=report_progress,
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
                    progress_callback=report_progress,
                )
            else:
                return jsonify({"error": "No scraper available"}), 500
        
        results = scraper.scrape(keyword=keyword, location=location, stop_event=STOP_EVENT)

        if exclusion_business_ids and scrape_history:
            filtered_results = []
            for lead in results:
                if not isinstance(lead, dict):
                    filtered_results.append(lead)
                    continue
                business_id = scrape_history.get_business_id(lead)
                if business_id and business_id in exclusion_business_ids:
                    excluded_by_history += 1
                    continue
                filtered_results.append(lead)
            results = filtered_results
            if excluded_by_history > 0:
                log.info("Excluded %d leads using selected/history files", excluded_by_history)

        for lead in results:
            if isinstance(lead, dict):
                lead["whatsapp_wa_me_links"] = _build_whatsapp_wa_me_links(lead)

        if scrape_history and results:
            scrape_history.add_batch_to_history(results, keyword, location)

        SCRAPE_STATE["results"] = results
        log.info("Scraping completed. Found %d results", len(results))

        csv_path = _write_csv(keyword, location, results)
        SCRAPE_STATE["csv_path"] = csv_path
        log.info("CSV file written to: %s", csv_path)

        if STOP_EVENT.is_set():
            SCRAPE_STATE["status"] = "stopped"
            SCRAPE_STATE["message"] = f"Scrape stopped. {len(results)} leads collected"
        else:
            SCRAPE_STATE["status"] = "completed"
            if excluded_by_history > 0:
                SCRAPE_STATE["message"] = f"Completed. {len(results)} leads collected ({excluded_by_history} skipped from selected/history files)"
            else:
                SCRAPE_STATE["message"] = f"Completed. {len(results)} leads collected"

        return jsonify(
            {
                "status": SCRAPE_STATE["status"],
                "message": SCRAPE_STATE["message"],
                "count": len(results),
                "results": results,
                "csv_file": os.path.basename(csv_path),
                "history_files_used": selected_history_files,
                "history_skipped": excluded_by_history,
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
    base_filename = f"leads_{safe_keyword}_{safe_location}"
    path = _build_unique_output_path(base_filename)
    
    log.info("Writing CSV to: %s", path)
    log.info("Number of leads to write: %d", len(leads))

    # Enhanced CSV with all extracted fields including verification data
    fieldnames = [
        "Name", "Phone", "Email", "All Emails", "WhatsApp", "All WhatsApp",
        "WhatsApp wa.me Links",
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
                        "WhatsApp wa.me Links": _build_whatsapp_wa_me_links(lead),
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


def _build_unique_output_path(base_filename: str) -> str:
    filename = os.path.basename((base_filename or "").strip())
    stem, ext = os.path.splitext(filename)

    if not stem:
        stem = datetime.now().strftime("%Y%m%d%H%M%S")
    if ext.lower() != ".csv":
        ext = ".csv"

    candidate = os.path.join(OUTPUT_DIR, f"{stem}{ext}")
    if not os.path.exists(candidate):
        return candidate

    suffix = 1
    while True:
        candidate = os.path.join(OUTPUT_DIR, f"{stem}_{suffix}{ext}")
        if not os.path.exists(candidate):
            return candidate
        suffix += 1


def _list_output_history_files() -> List[Dict[str, str]]:
    files: List[Dict[str, str]] = []

    try:
        for entry in os.scandir(OUTPUT_DIR):
            if not entry.is_file() or not entry.name.lower().endswith(".csv"):
                continue

            stat = entry.stat()
            modified = datetime.fromtimestamp(stat.st_mtime)
            files.append(
                {
                    "name": entry.name,
                    "size_bytes": stat.st_size,
                    "rows": _count_csv_rows(entry.path),
                    "modified": modified.strftime("%Y-%m-%d %H:%M:%S"),
                    "modified_ts": stat.st_mtime,
                }
            )
    except Exception as exc:
        log.warning("Failed listing output history files: %s", exc)
        return []

    files.sort(key=lambda item: item.get("modified_ts", 0), reverse=True)
    for item in files:
        item.pop("modified_ts", None)
    return files


def _count_csv_rows(path: str) -> int:
    try:
        with open(path, "r", encoding="utf-8", newline="") as file:
            row_count = sum(1 for _ in csv.reader(file))
            return max(0, row_count - 1)
    except Exception:
        return 0


def _normalize_history_file_selection(raw_files: Optional[object]) -> List[str]:
    if not isinstance(raw_files, list):
        return []

    selected: List[str] = []
    seen: Set[str] = set()
    for raw in raw_files:
        if not isinstance(raw, str):
            continue

        name = os.path.basename(raw.strip())
        if not name or not name.lower().endswith(".csv"):
            continue

        full_path = os.path.join(OUTPUT_DIR, name)
        if not os.path.isfile(full_path):
            continue

        if name in seen:
            continue
        seen.add(name)
        selected.append(name)

    return selected


def _build_whatsapp_wa_me_links(lead: Dict[str, str]) -> str:
    """Build wa.me links from extracted WhatsApp numbers."""
    raw_values = [
        str(lead.get("all_whatsapp", "") or ""),
        str(lead.get("whatsapp", "") or ""),
    ]

    numbers: List[str] = []
    seen = set()

    for raw in raw_values:
        if not raw:
            continue

        candidates = re.findall(r"\+?\d[\d\s()\-.]{6,}\d", raw)
        if not candidates:
            candidates = [raw]

        for candidate in candidates:
            digits = re.sub(r"\D", "", candidate)
            if len(digits) < 8:
                continue
            if digits in seen:
                continue
            seen.add(digits)
            numbers.append(digits)

    return "; ".join(f"https://wa.me/{number}" for number in numbers)


def _sanitize_token(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"[^a-z0-9_]+", "", value)
    return value or datetime.now().strftime("%Y%m%d%H%M%S")


if __name__ == "__main__":
    # Disable debug mode to prevent auto-reload during long scraping sessions
    app.run(host="127.0.0.1", port=5000, debug=False, threaded=True)
