#!/usr/bin/env python3
"""
SDR Candidate Sourcer for Workstream
Searches LinkedIn for high-grit SDR/AE candidates using Google X-Ray searches.
"""

import csv
import re
import time
import random
from typing import List, Dict, Optional, Set
from urllib.parse import unquote
from datetime import datetime

import os

# Google Sheets integration
try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False
    print("Note: Install gspread for Google Sheets support: pip install gspread google-auth")

try:
    from googlesearch import search as google_search
except ImportError:
    google_search = None

try:
    from duckduckgo_search import DDGS
except ImportError:
    DDGS = None

try:
    from serpapi import GoogleSearch as SerpAPISearch
except ImportError:
    SerpAPISearch = None

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Please install requests and beautifulsoup4: pip install requests beautifulsoup4")
    exit(1)

# SerpAPI key - set via environment variable or directly here
# Get your free API key at: https://serpapi.com (100 free searches/month)
SERPAPI_KEY = os.environ.get('SERPAPI_KEY', '97b4d83e7fa21d9c50f11ebc42ccc2881095ae137250e9321aa5bf6377837a6e')

# Search engine priority: SerpAPI > Google > DuckDuckGo
# SerpAPI is most reliable but requires API key
USE_SERPAPI = bool(SERPAPI_KEY and SerpAPISearch)
USE_GOOGLE = not USE_SERPAPI and google_search is not None
USE_DUCKDUCKGO = not USE_SERPAPI and not USE_GOOGLE and DDGS is not None

if not (USE_SERPAPI or google_search or DDGS):
    print("Please install a search library:")
    print("  pip install google-search-results  (recommended, requires API key)")
    print("  pip install googlesearch-python")
    print("  pip install duckduckgo_search")
    exit(1)


# Search queries for Google (supports site: operator)
# Targeting Utah-based candidates
GOOGLE_QUERIES = [
    # Athletes - NCAA/Student Athletes (2024-2025 grads) in Utah
    'site:linkedin.com/in "Student Athlete" "2024" SDR Utah',
    'site:linkedin.com/in "NCAA" "2024" Sales Utah',
    'site:linkedin.com/in "Student Athlete" "2025" Sales Utah',
    'site:linkedin.com/in "BYU" OR "Utah State" OR "University of Utah" Sales 2024',

    # D2D Sales Experience (Utah is a D2D hotspot - Vivint HQ)
    'site:linkedin.com/in "Door to Door" Solar Sales Utah',
    'site:linkedin.com/in Vivint "Sales Representative" Utah',
    'site:linkedin.com/in "D2D Sales" Utah',
    'site:linkedin.com/in "Outside Sales" "Door-to-Door" Utah',

    # Restaurant Pivoters - Former restaurant workers in Utah
    'site:linkedin.com/in "Restaurant Manager" "Account Executive" Utah',
    'site:linkedin.com/in Bartender SDR Sales Utah',
    'site:linkedin.com/in "Restaurant" "SaaS Sales" Utah',

    # Restaurant Pivoters - Selling TO restaurants
    'site:linkedin.com/in Toast "Account Executive" Utah',
    'site:linkedin.com/in 7Shifts Sales Utah',
    'site:linkedin.com/in "Restaurant Tech" Sales Utah',

    # Entrepreneurs / Side Hustlers in Utah
    'site:linkedin.com/in Founder "Side Hustle" Sales Utah',
    'site:linkedin.com/in Entrepreneur "Small Business" SDR Utah',
]

# Search queries for DuckDuckGo (uses inurl: instead of site:)
DUCKDUCKGO_QUERIES = [
    # Athletes - NCAA/Student Athletes (2024-2025 grads)
    'linkedin.com/in Student Athlete 2024 SDR',
    'linkedin.com/in NCAA Division 2024 Sales',
    'linkedin.com/in Student Athlete 2025 Sales Representative',
    'linkedin.com/in Varsity Captain Sales 2024',

    # D2D Sales Experience
    'linkedin.com/in Door to Door Solar Sales Representative',
    'linkedin.com/in Vivint Sales Representative',
    'linkedin.com/in D2D Sales Solar',
    'linkedin.com/in Outside Sales Door-to-Door',

    # Restaurant Pivoters - Former restaurant workers
    'linkedin.com/in Restaurant Manager Account Executive',
    'linkedin.com/in Bartender SDR Sales Development',
    'linkedin.com/in Restaurant SaaS Sales',

    # Restaurant Pivoters - Selling TO restaurants
    'linkedin.com/in Toast Account Executive Restaurant',
    'linkedin.com/in 7Shifts Sales Representative',
    'linkedin.com/in Restaurant Tech Sales',

    # Entrepreneurs / Side Hustlers
    'linkedin.com/in Founder Side Hustle Sales',
    'linkedin.com/in Entrepreneur Small Business SDR',
]

# Default to Google queries
SEARCH_QUERIES = GOOGLE_QUERIES

# Configuration
RESULTS_PER_QUERY = 10
MIN_DELAY = 2  # Minimum seconds between requests (SerpAPI is more tolerant)
MAX_DELAY = 4  # Maximum seconds between requests
BATCH_SIZE = 8  # Number of queries to run before a longer pause
BATCH_PAUSE = 10  # Seconds to pause between batches

# Google Sheets Configuration
# Set your Google Sheet ID (from the URL: https://docs.google.com/spreadsheets/d/SHEET_ID/edit)
GOOGLE_SHEET_ID = os.environ.get('GOOGLE_SHEET_ID', '16VwCMk5pbInX7_YHMTRSaJp2caBd8v4GK-td6jux5JE')
# Path to your service account credentials JSON file
GOOGLE_CREDENTIALS_FILE = os.environ.get('GOOGLE_CREDENTIALS_FILE', 'credentials.json')
# Sheet name (tab) to use
SHEET_NAME = 'candidates'

# Headers to mimic a real browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}


def extract_name_from_url(url: str) -> Optional[str]:
    """Extract potential name from LinkedIn URL."""
    try:
        # LinkedIn URLs are like: linkedin.com/in/firstname-lastname-123abc
        match = re.search(r'linkedin\.com/in/([^/?]+)', url)
        if match:
            slug = match.group(1)
            # Remove trailing numbers/codes and convert hyphens to spaces
            name_part = re.sub(r'-[a-f0-9]{5,}$', '', slug)
            name_part = re.sub(r'-\d+$', '', name_part)
            name = name_part.replace('-', ' ').title()
            return name
    except Exception:
        pass
    return None


def parse_search_result(url: str, title: str, snippet: str) -> Dict[str, str]:
    """Parse a Google search result to extract candidate info."""
    candidate = {
        'full_name': '',
        'linkedin_url': url,
        'headline': '',
        'email': '',
        'phone': ''
    }

    # Try to extract name from title (usually "Name - Title | LinkedIn")
    if title:
        # Remove " | LinkedIn" suffix
        clean_title = re.sub(r'\s*[|\-–]\s*LinkedIn.*$', '', title, flags=re.IGNORECASE)
        # Try to split by " - " to get name and headline
        parts = re.split(r'\s*[-–]\s*', clean_title, maxsplit=1)
        if parts:
            candidate['full_name'] = parts[0].strip()
            if len(parts) > 1:
                candidate['headline'] = parts[1].strip()

    # If no name found, try URL
    if not candidate['full_name']:
        candidate['full_name'] = extract_name_from_url(url) or ''

    # If no headline from title, use snippet
    if not candidate['headline'] and snippet:
        # Clean up the snippet
        headline = snippet.replace('\n', ' ').strip()
        # Truncate if too long
        if len(headline) > 200:
            headline = headline[:200] + '...'
        candidate['headline'] = headline

    return candidate


def fetch_profile_details(url: str) -> Optional[Dict[str, str]]:
    """
    Attempt to fetch additional details from a public LinkedIn profile.
    Note: LinkedIn heavily restricts scraping, so this may have limited success.
    """
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')

            # Try to find name
            name_tag = soup.find('h1')
            name = name_tag.get_text(strip=True) if name_tag else None

            # Try to find headline
            headline_tag = soup.find('div', class_=re.compile(r'headline|subtitle', re.I))
            headline = headline_tag.get_text(strip=True) if headline_tag else None

            if name or headline:
                return {'name': name, 'headline': headline}
    except Exception as e:
        # Silently fail - LinkedIn blocks most scraping attempts
        pass

    return None


def search_with_serpapi(query: str, num_results: int = 10) -> List[Dict[str, str]]:
    """Search using SerpAPI (most reliable, requires API key)."""
    candidates = []

    try:
        params = {
            "q": query,
            "num": num_results,
            "api_key": SERPAPI_KEY
        }
        search = SerpAPISearch(params)
        results = search.get_dict()

        organic_results = results.get("organic_results", [])

        for result in organic_results:
            url = result.get('link', '')
            title = result.get('title', '')
            snippet = result.get('snippet', '')

            # Only process LinkedIn profile URLs
            if 'linkedin.com/in/' not in url.lower():
                continue

            # Clean the URL
            url = unquote(url).split('?')[0]

            candidate = parse_search_result(url, title, snippet)

            if candidate['full_name'] or candidate['linkedin_url']:
                candidates.append(candidate)
                print(f"    Found: {candidate['full_name'] or 'Unknown'}")

    except Exception as e:
        print(f"    SerpAPI error: {str(e)}")

    return candidates


def search_with_duckduckgo(query: str, num_results: int = 10, debug: bool = False) -> List[Dict[str, str]]:
    """Search using DuckDuckGo."""
    candidates = []

    try:
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=num_results))

        if debug:
            print(f"    DEBUG: Got {len(results)} results")

        for result in results:
            url = result.get('href', '')
            title = result.get('title', '')
            snippet = result.get('body', '')

            if debug:
                print(f"    DEBUG URL: {url[:80]}...")

            # Only process LinkedIn profile URLs
            if 'linkedin.com/in/' not in url.lower():
                continue

            # Clean the URL
            url = unquote(url).split('?')[0]

            candidate = parse_search_result(url, title, snippet)

            if candidate['full_name'] or candidate['linkedin_url']:
                candidates.append(candidate)
                print(f"    Found: {candidate['full_name'] or 'Unknown'}")

    except Exception as e:
        print(f"    DuckDuckGo error: {str(e)}")

    return candidates


def search_with_google(query: str, num_results: int = 10) -> List[Dict[str, str]]:
    """Search using Google."""
    candidates = []

    try:
        results = list(google_search(query, num_results=num_results, advanced=True))

        for result in results:
            if hasattr(result, 'url'):
                url = result.url
                title = getattr(result, 'title', '') or ''
                snippet = getattr(result, 'description', '') or ''
            elif isinstance(result, str):
                url = result
                title = ''
                snippet = ''
            else:
                continue

            if 'linkedin.com/in/' not in url.lower():
                continue

            url = unquote(url).split('?')[0]
            candidate = parse_search_result(url, title, snippet)

            if candidate['full_name'] or candidate['linkedin_url']:
                candidates.append(candidate)
                print(f"    Found: {candidate['full_name'] or 'Unknown'}")

    except Exception as e:
        error_msg = str(e)
        if '429' in error_msg or 'Too Many Requests' in error_msg:
            print(f"    Rate limited by Google. Wait a few minutes and try again.")
        else:
            print(f"    Google error: {error_msg}")

    return candidates


DEBUG_MODE = False  # Set to True to see what URLs are being returned


def search_candidates(query: str, num_results: int = 10) -> List[Dict[str, str]]:
    """Perform a search and return candidate results."""
    print(f"\n  Searching: {query[:80]}...")

    if USE_SERPAPI:
        candidates = search_with_serpapi(query, num_results)
    elif USE_GOOGLE:
        candidates = search_with_google(query, num_results)
    elif USE_DUCKDUCKGO:
        candidates = search_with_duckduckgo(query, num_results, debug=DEBUG_MODE)
    else:
        print("    No search engine available")
        return []

    if not candidates:
        print("    No LinkedIn profiles found in results")

    return candidates


# Executive titles to exclude (too senior for SDR/AE roles)
# Note: Founder and Owner are explicitly ALLOWED
EXCLUDED_TITLES = [
    r'\bvp\b',
    r'\bvice president\b',
    r'\bdirector\b',
    r'\bhead of\b',
    r'\bchief\b',
    r'\bceo\b',
    r'\bcro\b',
    r'\bcoo\b',
    r'\bcfo\b',
    r'\bcmo\b',
    r'\bcto\b',
    r'\bsvp\b',
    r'\bevp\b',
    r'\bsenior vice president\b',
    r'\bexecutive vice president\b',
    r'\bgeneral manager\b',
    r'\bgm\b',
    r'\bpresident\b',
    r'\bpartner\b',
    r'\bprincipal\b',
    r'\bmanaging director\b',
]

# Titles that are allowed even if they might match exclusion patterns
ALLOWED_TITLES = [
    r'\bfounder\b',
    r'\bowner\b',
    r'\bco-founder\b',
    r'\bcofounder\b',
]


def is_too_senior(headline: str) -> bool:
    """Check if a candidate's headline indicates they're too senior for SDR/AE roles."""
    if not headline:
        return False

    headline_lower = headline.lower()

    # First check if they have an allowed title (founder/owner) - these are always OK
    for pattern in ALLOWED_TITLES:
        if re.search(pattern, headline_lower):
            return False

    # Then check for excluded executive titles
    for pattern in EXCLUDED_TITLES:
        if re.search(pattern, headline_lower):
            return True

    return False


def filter_senior_candidates(candidates: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Filter out candidates who are too senior for SDR/AE roles."""
    filtered = []
    excluded_count = 0

    for candidate in candidates:
        if is_too_senior(candidate.get('headline', '')):
            excluded_count += 1
        else:
            filtered.append(candidate)

    if excluded_count > 0:
        print(f"  Filtered out {excluded_count} senior/executive candidates")

    return filtered


def deduplicate_candidates(candidates: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Remove duplicate candidates based on LinkedIn URL."""
    seen_urls = set()
    unique = []

    for candidate in candidates:
        url = candidate['linkedin_url'].lower().rstrip('/')
        if url not in seen_urls:
            seen_urls.add(url)
            unique.append(candidate)

    return unique


def load_existing_candidates(filename: str = 'candidates.csv') -> List[Dict[str, str]]:
    """Load existing candidates from CSV if it exists."""
    candidates = []
    try:
        with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                candidates.append({
                    'full_name': row.get('Full Name', ''),
                    'linkedin_url': row.get('LinkedIn URL', ''),
                    'headline': row.get('Headline', ''),
                    'email': row.get('Email', ''),
                    'phone': row.get('Phone', '')
                })
        print(f"Loaded {len(candidates)} existing candidates from {filename}")
    except FileNotFoundError:
        pass
    return candidates


def save_to_csv(candidates: List[Dict[str, str]], filename: str = 'candidates.csv'):
    """Save candidates to a CSV file."""
    fieldnames = ['Full Name', 'LinkedIn URL', 'Headline', 'Email', 'Phone']

    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for candidate in candidates:
            writer.writerow({
                'Full Name': candidate['full_name'],
                'LinkedIn URL': candidate['linkedin_url'],
                'Headline': candidate['headline'],
                'Email': candidate['email'],
                'Phone': candidate['phone']
            })

    print(f"\nSaved {len(candidates)} candidates to {filename}")


def get_google_sheets_client():
    """Initialize and return a Google Sheets client."""
    if not GSPREAD_AVAILABLE:
        return None

    try:
        # Define the scopes
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]

        # Load credentials from service account file
        creds = Credentials.from_service_account_file(
            GOOGLE_CREDENTIALS_FILE,
            scopes=scopes
        )

        client = gspread.authorize(creds)
        return client
    except FileNotFoundError:
        print(f"  Credentials file not found: {GOOGLE_CREDENTIALS_FILE}")
        return None
    except Exception as e:
        print(f"  Error initializing Google Sheets: {str(e)}")
        return None


def get_existing_urls_from_sheet(worksheet) -> Set[str]:
    """Get all existing LinkedIn URLs from the sheet to avoid duplicates."""
    try:
        # Get all values from LinkedIn URL column (column B)
        url_column = worksheet.col_values(2)
        # Skip header, normalize URLs
        existing_urls = {url.lower().rstrip('/') for url in url_column[1:] if url}
        return existing_urls
    except Exception as e:
        print(f"  Error reading existing URLs: {str(e)}")
        return set()


def upload_to_google_sheets(candidates: List[Dict[str, str]], sheet_id: str = None) -> int:
    """
    Upload candidates to Google Sheets, appending only new entries.
    Returns the number of new candidates added.
    """
    if not GSPREAD_AVAILABLE:
        print("  Google Sheets not available. Install with: pip install gspread google-auth")
        return 0

    sheet_id = sheet_id or GOOGLE_SHEET_ID
    if not sheet_id:
        print("  No Google Sheet ID configured. Set GOOGLE_SHEET_ID environment variable.")
        return 0

    print("\nUploading to Google Sheets...")

    client = get_google_sheets_client()
    if not client:
        return 0

    try:
        # Open the spreadsheet
        spreadsheet = client.open_by_key(sheet_id)

        # Try to get the worksheet, create if it doesn't exist
        try:
            worksheet = spreadsheet.worksheet(SHEET_NAME)
            print(f"  Found existing worksheet: {SHEET_NAME}")
        except gspread.WorksheetNotFound:
            print(f"  Creating new worksheet: {SHEET_NAME}")
            worksheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=1000, cols=6)
            # Add headers
            headers = ['Full Name', 'LinkedIn URL', 'Headline', 'Email', 'Phone', 'Date Added']
            worksheet.append_row(headers)

        # Check if headers exist, add if first row is empty
        first_row = worksheet.row_values(1)
        if not first_row or first_row[0] != 'Full Name':
            print(f"  Adding headers to worksheet")
            worksheet.insert_row(['Full Name', 'LinkedIn URL', 'Headline', 'Email', 'Phone', 'Date Added'], 1)

        # Get existing URLs to avoid duplicates
        existing_urls = get_existing_urls_from_sheet(worksheet)
        print(f"  Found {len(existing_urls)} existing candidates in sheet")

        # Filter to only new candidates
        new_candidates = []
        for candidate in candidates:
            url_normalized = candidate['linkedin_url'].lower().rstrip('/')
            if url_normalized not in existing_urls:
                new_candidates.append(candidate)

        if not new_candidates:
            print("  No new candidates to add (all already in sheet)")
            return 0

        # Prepare rows to append
        today = datetime.now().strftime('%Y-%m-%d')
        rows_to_add = []
        for candidate in new_candidates:
            rows_to_add.append([
                candidate['full_name'],
                candidate['linkedin_url'],
                candidate['headline'],
                candidate['email'],
                candidate['phone'],
                today
            ])

        # Batch append all new rows
        worksheet.append_rows(rows_to_add)

        print(f"  Added {len(new_candidates)} new candidates to Google Sheet")
        return len(new_candidates)

    except gspread.SpreadsheetNotFound:
        print(f"  Spreadsheet not found. Check the GOOGLE_SHEET_ID: {sheet_id}")
        return 0
    except Exception as e:
        print(f"  Error uploading to Google Sheets: {str(e)}")
        return 0


def main():
    """Main function to run the candidate sourcer."""
    import sys

    print("=" * 60)
    print("SDR Candidate Sourcer for Workstream")
    print("Searching for high-grit SDR/AE candidates...")
    print("=" * 60)

    # Select queries based on search engine
    if USE_SERPAPI:
        base_queries = GOOGLE_QUERIES  # SerpAPI uses Google, so use Google queries
        print("Using SerpAPI (Google) search engine")
    elif USE_GOOGLE:
        base_queries = GOOGLE_QUERIES
        print("Using Google search engine (may hit rate limits)")
    elif USE_DUCKDUCKGO:
        base_queries = DUCKDUCKGO_QUERIES
        print("Using DuckDuckGo search engine (limited LinkedIn indexing)")
    else:
        print("ERROR: No search engine available")
        return

    # Check for batch argument (e.g., python script.py 1 for first batch)
    batch_num = None
    if len(sys.argv) > 1:
        try:
            batch_num = int(sys.argv[1])
        except ValueError:
            pass

    queries_to_run = base_queries
    if batch_num is not None:
        start_idx = (batch_num - 1) * BATCH_SIZE
        end_idx = start_idx + BATCH_SIZE
        queries_to_run = base_queries[start_idx:end_idx]
        total_batches = (len(base_queries) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"\nRunning batch {batch_num} of {total_batches} ({len(queries_to_run)} queries)")
    else:
        print(f"\nRunning all {len(queries_to_run)} queries (use 'python script.py N' for batch N)")

    all_candidates = []

    for i, query in enumerate(queries_to_run, 1):
        print(f"\n[{i}/{len(queries_to_run)}] Running search...")

        candidates = search_candidates(query, num_results=RESULTS_PER_QUERY)
        all_candidates.extend(candidates)

        # Rate limiting with longer random interval
        if i < len(queries_to_run):
            # Longer pause every BATCH_SIZE queries
            if i % BATCH_SIZE == 0:
                print(f"    Batch complete. Pausing {BATCH_PAUSE}s to avoid rate limits...")
                time.sleep(BATCH_PAUSE)
            else:
                sleep_time = random.uniform(MIN_DELAY, MAX_DELAY)
                print(f"    Waiting {sleep_time:.1f}s before next search...")
                time.sleep(sleep_time)

    # Filter out senior/executive candidates
    print("\n" + "-" * 40)
    print(f"Total results from search: {len(all_candidates)}")
    all_candidates = filter_senior_candidates(all_candidates)
    print(f"After filtering executives: {len(all_candidates)}")

    # Load existing candidates and merge
    existing_candidates = load_existing_candidates()
    all_candidates = existing_candidates + all_candidates

    # Deduplicate results
    print(f"Total after merging with existing: {len(all_candidates)}")

    unique_candidates = deduplicate_candidates(all_candidates)
    print(f"Unique candidates after deduplication: {len(unique_candidates)}")

    # Save to CSV (local backup)
    if unique_candidates:
        save_to_csv(unique_candidates)

        # Upload to Google Sheets
        if GOOGLE_SHEET_ID:
            new_count = upload_to_google_sheets(unique_candidates)
            if new_count > 0:
                print(f"\n✓ Google Sheet updated with {new_count} new candidates")
        else:
            print("\nTip: Set GOOGLE_SHEET_ID to enable Google Sheets sync")
    else:
        print("\nNo candidates found. Try adjusting search queries.")

    print("\n" + "=" * 60)
    print("Search complete!")
    print("=" * 60)


if __name__ == '__main__':
    main()
