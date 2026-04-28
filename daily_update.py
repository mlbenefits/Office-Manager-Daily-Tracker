"""
daily_update.py
Runs every morning at 8 AM Eastern to:
1. Pull yesterday's new conversations from GHL and calculate response times
2. Write the avg response time (in minutes) into column J of the Daily Tracker
"""

import os
import statistics
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

import gspread
import requests
from dotenv import load_dotenv

# ── Load env from same directory as this script ───────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(SCRIPT_DIR, ".env"))

API_KEY = os.getenv("GHL_API_KEY")
LOCATION_ID = os.getenv("GHL_LOCATION_ID")
BASE_URL = "https://services.leadconnectorhq.com"

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
    "Version": "2021-04-15",
}

SPREADSHEET_ID = "1Lf0jBFtz8jZ_yTRrcZ3xQl-9-uN0c-mu7YijyCpJS5w"
WORKSHEET_NAME = "Daily Tracker"
COLUMN_J = 10
BASE_DATE = date(2026, 1, 1)
BASE_ROW = 6

_rate_lock = threading.Semaphore(10)


def rate_limited_get(url, headers, params=None):
    with _rate_lock:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
    return resp


def get_conversations_for_date(target_date):
    """Fetch conversations created on target_date."""
    start_of_day = datetime(target_date.year, target_date.month, target_date.day,
                            tzinfo=timezone.utc)
    end_of_day = start_of_day + timedelta(days=1)
    start_ms = int(start_of_day.timestamp() * 1000)
    end_ms = int(end_of_day.timestamp() * 1000)

    cursor_ms = int(end_of_day.timestamp() * 1000)
    cutoff_ms = start_ms
    conversations = []
    limit = 100

    while True:
        params = {
            "locationId": LOCATION_ID,
            "limit": limit,
            "startAfterDate": cursor_ms,
            "sort": "desc",
            "sortBy": "last_message_date",
        }
        resp = rate_limited_get(f"{BASE_URL}/conversations/search", HEADERS, params)
        resp.raise_for_status()
        data = resp.json()

        batch = data.get("conversations", [])
        if not batch:
            break

        passed_cutoff = False
        for convo in batch:
            last_msg_ms = convo.get("lastMessageDate")
            created_ms = convo.get("dateAdded")
            if last_msg_ms is None:
                continue
            if last_msg_ms < cutoff_ms:
                passed_cutoff = True
                continue
            if created_ms is not None and start_ms <= created_ms < end_ms:
                conversations.append(convo)

        if passed_cutoff or len(batch) < limit:
            break

        last_date = batch[-1].get("lastMessageDate")
        if not last_date or last_date < cutoff_ms:
            break
        cursor_ms = last_date

    return conversations


def get_messages(conversation_id):
    """Fetch all messages for a conversation."""
    messages = []
    next_page = None

    while True:
        params = {"limit": 100}
        if next_page:
            params["nextPage"] = next_page

        resp = rate_limited_get(
            f"{BASE_URL}/conversations/{conversation_id}/messages",
            HEADERS,
            params,
        )
        if resp.status_code in (401, 403, 404):
            return []
        if resp.status_code == 429:
            time.sleep(2)
            continue
        resp.raise_for_status()
        data = resp.json()

        batch = data.get("messages", {})
        if isinstance(batch, dict):
            msgs = batch.get("messages", [])
            next_page = batch.get("nextPage")
        else:
            msgs = batch
            next_page = data.get("nextPage")

        messages.extend(msgs)
        if not msgs or not next_page or next_page is True:
            break

    return messages


def parse_ts(ts):
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    if isinstance(ts, str):
        ts = ts.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(ts)
        except ValueError:
            return None
    return None


EASTERN = timezone(timedelta(hours=-4))  # EDT; change to -5 in winter (EST)


def is_business_hours(ts):
    """Mon–Fri, 9 AM–5 PM Eastern."""
    local = ts.astimezone(EASTERN)
    if local.weekday() >= 5:
        return False
    if local.hour < 9 or local.hour >= 17:
        return False
    return True


def get_first_response_time(convo):
    """
    Return minutes from first inbound (during business hours) to first
    outbound reply after it. Returns None if no qualifying pair found.
    """
    msgs = get_messages(convo.get("id"))
    if not msgs:
        return None

    msgs.sort(key=lambda m: parse_ts(m.get("dateAdded") or m.get("date") or 0)
              or datetime.min.replace(tzinfo=timezone.utc))

    def is_inbound_trackable(msg):
        return "ACTIVITY" not in str(msg.get("messageType", msg.get("type", ""))).upper()

    def is_outbound_trackable(msg):
        msg_type = str(msg.get("messageType", msg.get("type", ""))).upper()
        if "ACTIVITY" in msg_type:
            return False
        if "CALL" in msg_type or "VOICEMAIL" in msg_type:
            return str(msg.get("status", "")).lower() in ("answered", "voicemail", "completed")
        return True

    first_inbound_ts = None
    first_inbound_idx = None
    for idx, msg in enumerate(msgs):
        if msg.get("direction", "").lower() == "inbound" and is_inbound_trackable(msg):
            ts = parse_ts(msg.get("dateAdded") or msg.get("date"))
            if ts and not is_business_hours(ts):
                continue
            first_inbound_ts = ts
            first_inbound_idx = idx
            break

    if first_inbound_ts is None:
        return None

    for msg in msgs[first_inbound_idx + 1:]:
        if msg.get("direction", "").lower() == "outbound" and is_outbound_trackable(msg):
            reply_ts = parse_ts(msg.get("dateAdded") or msg.get("date"))
            if reply_ts and reply_ts > first_inbound_ts:
                return (reply_ts - first_inbound_ts).total_seconds() / 60

    return None


def compute_avg_response(target_date, max_workers=20):
    """Compute avg first-response time (minutes) for all new convos on target_date."""
    convos = get_conversations_for_date(target_date)
    if not convos:
        print(f"  No new conversations found for {target_date}")
        return None

    print(f"  Found {len(convos)} new conversations for {target_date}")

    times = []
    lock = threading.Lock()

    def process(convo):
        return get_first_response_time(convo)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for t in as_completed(executor.submit(process, c) for c in convos):
            result = t.result()
            if result is not None:
                with lock:
                    times.append(result)

    if not times:
        print(f"  No replied conversations found for {target_date}")
        return None

    avg = round(statistics.mean(times), 1)
    print(f"  Avg response time: {avg} min ({len(times)} replied, "
          f"{len(convos) - len(times)} no reply)")
    return avg


def get_sheets_client():
    """Return an authenticated gspread client using OAuth token cached in ~/.config/gspread/."""
    return gspread.oauth()


def write_to_tracker(target_date, avg_minutes):
    """Write avg_minutes into column J of Daily Tracker for target_date."""
    client = get_sheets_client()
    ws = client.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)
    row_num = BASE_ROW + (target_date - BASE_DATE).days
    ws.update_cell(row_num, COLUMN_J, avg_minutes)
    print(f"  Wrote {avg_minutes} min to row {row_num} (col J) for {target_date}")


def main():
    if not API_KEY or not LOCATION_ID:
        print("ERROR: GHL_API_KEY and GHL_LOCATION_ID must be set")
        sys.exit(1)

    now_eastern = datetime.now(timezone.utc).astimezone(EASTERN)
    yesterday = (now_eastern - timedelta(days=1)).date()

    print(f"=== Daily Response Time Update — {datetime.now().strftime('%Y-%m-%d %H:%M')} ===")
    print(f"Analyzing new conversations from: {yesterday}")

    if yesterday.weekday() >= 5:
        print(f"{yesterday} is a weekend — skipping.")
        sys.exit(0)

    avg = compute_avg_response(yesterday)
    if avg is None:
        print(f"No data to write for {yesterday}. Column J left unchanged.")
        sys.exit(0)

    print("\nWriting to Daily Tracker...")
    write_to_tracker(yesterday, avg)
    print("Done.")


if __name__ == "__main__":
    main()
