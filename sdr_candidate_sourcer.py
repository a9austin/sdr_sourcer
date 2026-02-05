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

# SDR-focused queries - fresh from college, entry level, high grit candidates
# We want people who have NOT worked as SDRs - fresh grads, career pivoters, athletes
SDR_GOOGLE_QUERIES = [
    # Recent College Graduates (2023-2025) in Utah - fresh from college
    'site:linkedin.com/in "Class of 2025" Utah',
    'site:linkedin.com/in "Class of 2024" Utah',
    'site:linkedin.com/in "Class of 2023" Utah',
    'site:linkedin.com/in "Recent Graduate" Utah 2024',
    'site:linkedin.com/in "Recent Graduate" Utah 2025',
    'site:linkedin.com/in "New Graduate" Utah Sales',
    'site:linkedin.com/in "BYU" "2024" OR "2025" -SDR -BDR',
    'site:linkedin.com/in "Utah State" "2024" OR "2025" -SDR -BDR',
    'site:linkedin.com/in "University of Utah" "2024" OR "2025" -SDR -BDR',
    'site:linkedin.com/in "UVU" OR "Utah Valley" "2024" OR "2025"',
    'site:linkedin.com/in "Weber State" "2024" OR "2025"',

    # Athletes - NCAA/Student Athletes (2023-2025 grads) in Utah
    'site:linkedin.com/in "Student Athlete" "2024" Utah -SDR -BDR',
    'site:linkedin.com/in "Student Athlete" "2025" Utah',
    'site:linkedin.com/in "NCAA" "2024" Utah -SDR -BDR',
    'site:linkedin.com/in "NCAA" "2025" Utah',
    'site:linkedin.com/in "Varsity" "Captain" Utah 2024',
    'site:linkedin.com/in "Student Athlete" "BYU" OR "Utah State" 2024',

    # D2D Sales Experience (Utah is a D2D hotspot - Vivint HQ)
    'site:linkedin.com/in "Door to Door" Solar Sales Utah',
    'site:linkedin.com/in Vivint "Sales Representative" Utah',
    'site:linkedin.com/in "D2D Sales" Utah',
    'site:linkedin.com/in "Outside Sales" "Door-to-Door" Utah',
    'site:linkedin.com/in "Pest Control" Sales Utah',
    'site:linkedin.com/in "Alarm" "Sales Rep" Utah',

    # Restaurant/Hospitality Pivoters - career changers in Utah
    'site:linkedin.com/in "Restaurant Manager" Utah -SDR',
    'site:linkedin.com/in Bartender Utah "looking for" OR "seeking"',
    'site:linkedin.com/in "Server" "Restaurant" Utah 2024',
    'site:linkedin.com/in "Hospitality" Utah "Sales" OR "Business"',

    # Restaurant Tech - Selling TO restaurants
    'site:linkedin.com/in Toast "Sales Representative" Utah',
    'site:linkedin.com/in 7Shifts Sales Utah',
    'site:linkedin.com/in "Restaurant Tech" Sales Utah',

    # Entrepreneurs / Side Hustlers in Utah
    'site:linkedin.com/in Founder "Side Hustle" Sales Utah',
    'site:linkedin.com/in Entrepreneur "Small Business" Utah',

    # Entry-level / Internship backgrounds in Utah
    'site:linkedin.com/in "Sales Intern" Utah 2024',
    'site:linkedin.com/in "Marketing Intern" Utah 2024',
    'site:linkedin.com/in "Business Intern" Utah 2024',
    'site:linkedin.com/in "Entry Level" Sales Utah -SDR -BDR',
    'site:linkedin.com/in "Seeking opportunities" Sales Utah',

    # Communications / Business Majors in Utah
    'site:linkedin.com/in "Communications" "Bachelor" Utah 2024',
    'site:linkedin.com/in "Business Administration" Utah 2024 OR 2025',
    'site:linkedin.com/in "Marketing" "Bachelor" Utah 2024 OR 2025',

    # Female candidates - fresh grads and career pivoters
    'site:linkedin.com/in "she/her" "2024" OR "2025" Utah Sales',
    'site:linkedin.com/in "she/her" "Student Athlete" Utah 2024',
    'site:linkedin.com/in "she/her" "Recent Graduate" Utah',
    'site:linkedin.com/in "Women in Sales" Utah "Entry Level" OR "New Grad"',
    'site:linkedin.com/in "she/her" "Door to Door" Sales Utah',
    'site:linkedin.com/in "she/her" "BYU" OR "Utah State" 2024 Sales',
    'site:linkedin.com/in "she/her" "Restaurant" OR "Hospitality" Utah',
]

# AE-focused queries - targeting Utah Tech companies, ~2 years SaaS experience
AE_GOOGLE_QUERIES = [
    # Utah Tech Companies - Account Executives with SaaS experience
    'site:linkedin.com/in "Account Executive" "SaaS" Utah "2 years"',
    'site:linkedin.com/in "Account Executive" "SaaS" Utah tech',
    'site:linkedin.com/in "AE" "B2B SaaS" Utah',

    # Specific Utah Tech Companies
    'site:linkedin.com/in "Account Executive" Qualtrics Utah',
    'site:linkedin.com/in "Account Executive" Pluralsight Utah',
    'site:linkedin.com/in "Account Executive" Podium Utah',
    'site:linkedin.com/in "Account Executive" Lucid Utah',
    'site:linkedin.com/in "Account Executive" Domo Utah',
    'site:linkedin.com/in "Account Executive" Entrata Utah',
    'site:linkedin.com/in "Account Executive" Weave Utah',
    'site:linkedin.com/in "Account Executive" Divvy Utah',
    'site:linkedin.com/in "Account Executive" MX Utah',
    'site:linkedin.com/in "Account Executive" Instructure Utah',

    # SDRs ready to promote to AE (have SDR experience at Utah tech)
    'site:linkedin.com/in "SDR" "promoted" "Account Executive" Utah',
    'site:linkedin.com/in "Sales Development" "SaaS" Utah "2022" OR "2023"',

    # AEs at Utah startups
    'site:linkedin.com/in "Account Executive" "Series A" OR "Series B" Utah SaaS',
    'site:linkedin.com/in "Account Executive" startup Utah tech sales',

    # Mid-market / SMB AEs in Utah
    'site:linkedin.com/in "Account Executive" "SMB" Utah SaaS',
    'site:linkedin.com/in "Account Executive" "Mid-Market" Utah',

    # Female AE candidates - pronoun and affinity signals
    'site:linkedin.com/in "she/her" "Account Executive" Utah',
    'site:linkedin.com/in "she/her" "Account Executive" SaaS Utah',
    'site:linkedin.com/in "Women in Sales" "Account Executive" Utah',
    'site:linkedin.com/in "Women in Tech" "Account Executive" Utah',
    'site:linkedin.com/in "she/her" AE SaaS Utah',
    'site:linkedin.com/in "she/her" "Account Executive" tech Utah',
]

# Combined queries for backward compatibility
GOOGLE_QUERIES = SDR_GOOGLE_QUERIES + AE_GOOGLE_QUERIES

# Search queries for DuckDuckGo (uses inurl: instead of site:)
SDR_DUCKDUCKGO_QUERIES = [
    # Recent College Graduates in Utah
    'linkedin.com/in Class of 2025 Utah',
    'linkedin.com/in Class of 2024 Utah',
    'linkedin.com/in Class of 2023 Utah',
    'linkedin.com/in Recent Graduate Utah 2024',
    'linkedin.com/in Recent Graduate Utah 2025',
    'linkedin.com/in New Graduate Utah Sales',
    'linkedin.com/in BYU 2024 2025 Utah',
    'linkedin.com/in Utah State University 2024 2025',
    'linkedin.com/in University of Utah 2024 2025',
    'linkedin.com/in Utah Valley University 2024 2025',
    'linkedin.com/in Weber State 2024 2025',

    # Athletes - NCAA/Student Athletes (2023-2025 grads)
    'linkedin.com/in Student Athlete 2024 Utah',
    'linkedin.com/in Student Athlete 2025 Utah',
    'linkedin.com/in NCAA 2024 Utah',
    'linkedin.com/in NCAA 2025 Utah',
    'linkedin.com/in Varsity Captain Utah 2024',
    'linkedin.com/in Student Athlete BYU Utah State 2024',

    # D2D Sales Experience
    'linkedin.com/in Door to Door Solar Sales Utah',
    'linkedin.com/in Vivint Sales Representative Utah',
    'linkedin.com/in D2D Sales Utah',
    'linkedin.com/in Outside Sales Door-to-Door Utah',
    'linkedin.com/in Pest Control Sales Utah',
    'linkedin.com/in Alarm Sales Rep Utah',

    # Restaurant/Hospitality Pivoters
    'linkedin.com/in Restaurant Manager Utah',
    'linkedin.com/in Bartender Utah seeking',
    'linkedin.com/in Server Restaurant Utah 2024',
    'linkedin.com/in Hospitality Utah Sales',

    # Restaurant Tech
    'linkedin.com/in Toast Sales Representative Utah',
    'linkedin.com/in 7Shifts Sales Utah',
    'linkedin.com/in Restaurant Tech Sales Utah',

    # Entrepreneurs / Side Hustlers
    'linkedin.com/in Founder Side Hustle Sales Utah',
    'linkedin.com/in Entrepreneur Small Business Utah',

    # Entry-level / Internship backgrounds
    'linkedin.com/in Sales Intern Utah 2024',
    'linkedin.com/in Marketing Intern Utah 2024',
    'linkedin.com/in Business Intern Utah 2024',
    'linkedin.com/in Entry Level Sales Utah',
    'linkedin.com/in Seeking opportunities Sales Utah',

    # Communications / Business Majors
    'linkedin.com/in Communications Bachelor Utah 2024',
    'linkedin.com/in Business Administration Utah 2024',
    'linkedin.com/in Marketing Bachelor Utah 2024',

    # Female candidates - fresh grads and career pivoters
    'linkedin.com/in she/her 2024 2025 Utah Sales',
    'linkedin.com/in she/her Student Athlete Utah 2024',
    'linkedin.com/in she/her Recent Graduate Utah',
    'linkedin.com/in Women in Sales Utah Entry Level',
    'linkedin.com/in she/her Door to Door Sales Utah',
    'linkedin.com/in she/her BYU Utah State 2024 Sales',
    'linkedin.com/in she/her Restaurant Hospitality Utah',
]

AE_DUCKDUCKGO_QUERIES = [
    # Utah Tech Companies - Account Executives
    'linkedin.com/in Account Executive SaaS Utah',
    'linkedin.com/in AE B2B SaaS Utah tech',

    # Specific Utah Tech Companies
    'linkedin.com/in Account Executive Qualtrics',
    'linkedin.com/in Account Executive Pluralsight',
    'linkedin.com/in Account Executive Podium Utah',
    'linkedin.com/in Account Executive Lucid Utah',
    'linkedin.com/in Account Executive Domo',
    'linkedin.com/in Account Executive Entrata',
    'linkedin.com/in Account Executive Weave',

    # SDRs ready for AE
    'linkedin.com/in SDR promoted Account Executive Utah',
    'linkedin.com/in Sales Development SaaS Utah',

    # AEs at startups
    'linkedin.com/in Account Executive startup Utah SaaS',
    'linkedin.com/in Account Executive SMB Utah',

    # Female AE candidates
    'linkedin.com/in she/her Account Executive Utah',
    'linkedin.com/in she/her Account Executive SaaS Utah',
    'linkedin.com/in Women in Sales Account Executive Utah',
    'linkedin.com/in Women in Tech Account Executive Utah',
    'linkedin.com/in she/her AE SaaS Utah',
    'linkedin.com/in she/her Account Executive tech Utah',
]

DUCKDUCKGO_QUERIES = SDR_DUCKDUCKGO_QUERIES + AE_DUCKDUCKGO_QUERIES

# Default to Google queries
SEARCH_QUERIES = GOOGLE_QUERIES

# Configuration
RESULTS_PER_QUERY = 15
MIN_DELAY = 2  # Minimum seconds between requests (SerpAPI is more tolerant)
MAX_DELAY = 4  # Maximum seconds between requests
BATCH_SIZE = 8  # Number of queries to run before a longer pause
BATCH_PAUSE = 10  # Seconds to pause between batches

# Google Sheets Configuration
# Set your Google Sheet ID (from the URL: https://docs.google.com/spreadsheets/d/SHEET_ID/edit)
GOOGLE_SHEET_ID = os.environ.get('GOOGLE_SHEET_ID', '16VwCMk5pbInX7_YHMTRSaJp2caBd8v4GK-td6jux5JE')
# Path to your service account credentials JSON file
GOOGLE_CREDENTIALS_FILE = os.environ.get('GOOGLE_CREDENTIALS_FILE', 'sales-sourcing-6ef512645e0f.json')
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


def parse_search_result(url: str, title: str, snippet: str, source_query: str = '') -> Dict[str, str]:
    """Parse a Google search result to extract candidate info."""
    candidate = {
        'full_name': '',
        'linkedin_url': url,
        'headline': '',
        'email': '',
        'phone': '',
        'role_type': '',
        'source_query': source_query,
        'snippet': snippet or ''
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

    # Determine role fit based on headline and source query
    candidate['role_type'] = determine_role_fit(candidate['headline'], source_query)

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

            candidate = parse_search_result(url, title, snippet, source_query=query)

            if candidate['full_name'] or candidate['linkedin_url']:
                candidates.append(candidate)
                role_icon = "🎯" if candidate['role_type'] == 'AE' else "📞" if candidate['role_type'] == 'SDR' else "🔄"
                print(f"    {role_icon} {candidate['full_name'] or 'Unknown'} [{candidate['role_type']}]")

    except Exception as e:
        print(f"    ❌ SerpAPI error: {str(e)}")

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

            candidate = parse_search_result(url, title, snippet, source_query=query)

            if candidate['full_name'] or candidate['linkedin_url']:
                candidates.append(candidate)
                role_icon = "🎯" if candidate['role_type'] == 'AE' else "📞" if candidate['role_type'] == 'SDR' else "🔄"
                print(f"    {role_icon} {candidate['full_name'] or 'Unknown'} [{candidate['role_type']}]")

    except Exception as e:
        print(f"    ❌ DuckDuckGo error: {str(e)}")

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
            candidate = parse_search_result(url, title, snippet, source_query=query)

            if candidate['full_name'] or candidate['linkedin_url']:
                candidates.append(candidate)
                role_icon = "🎯" if candidate['role_type'] == 'AE' else "📞" if candidate['role_type'] == 'SDR' else "🔄"
                print(f"    {role_icon} {candidate['full_name'] or 'Unknown'} [{candidate['role_type']}]")

    except Exception as e:
        error_msg = str(e)
        if '429' in error_msg or 'Too Many Requests' in error_msg:
            print(f"    ⚠️  Rate limited by Google. Wait a few minutes and try again.")
        else:
            print(f"    ❌ Google error: {error_msg}")

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
    r'\benterprise\b',
]

# Titles that are allowed even if they might match exclusion patterns
ALLOWED_TITLES = [
    r'\bfounder\b',
    r'\bowner\b',
    r'\bco-founder\b',
    r'\bcofounder\b',
]

# Existing SDR/BDR titles to exclude - we want fresh candidates, not current SDRs
EXISTING_SDR_TITLES = [
    r'\bsdr\b',
    r'\bbdr\b',
    r'\bsales development representative\b',
    r'\bbusiness development representative\b',
    r'\bsales development\b',
    r'\bbusiness development rep\b',
    r'\blead development representative\b',
    r'\bldr\b',
    r'\bmarket development representative\b',
    r'\bmdr\b',
]

# Utah location keywords for filtering candidates with Utah connections
UTAH_LOCATION_KEYWORDS = [
    r'\butah\b',
    r'\bsalt lake city\b',
    r'\bslc\b',
    r'\bprovo\b',
    r'\bogden\b',
    r'\borem\b',
    r'\blehi\b',
    r'\bsandy\b',
    r'\bdraper\b',
    r'\bst\.?\s*george\b',
    r'\blogan\b',
    r'\bpark city\b',
    r'\bsilicon slopes\b',
    r',\s*ut\b',
    r'\but\s*,',
    r'\bbountiful\b',
    r'\bmurray\b',
    r'\blayton\b',
    r'\bclearfield\b',
    r'\bamerican fork\b',
    r'\bpleasant grove\b',
    r'\bspanish fork\b',
    r'\bspringville\b',
    r'\bheriman\b',
    r'\bherriman\b',
    r'\briverton\b',
    r'\btooele\b',
]

# Utah colleges and universities
UTAH_COLLEGES = [
    r'\bbyu\b',
    r'\bbrigham young\b',
    r'\butah state\b',
    r'\buniversity of utah\b',
    r'\bu of u\b',
    r'\bweber state\b',
    r'\buvu\b',
    r'\butah valley\b',
    r'\bsouthern utah\b',
    r'\bdixie state\b',
    r'\butah tech\b',
    r'\bwestminster\b.*\butah\b',
    r'\bsnow college\b',
    r'\bslcc\b',
    r'\bsalt lake community\b',
    r'\bensign college\b',
    r'\butah state university\b',
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


def is_existing_sdr(headline: str, snippet: str = '') -> bool:
    """Check if a candidate already has SDR/BDR experience.

    We want fresh-from-college candidates or people who haven't worked as SDRs.
    This filters out anyone whose headline or snippet indicates current/past SDR roles.
    """
    if not headline:
        return False

    text = f"{headline} {snippet}".lower()

    # First check if they have an allowed title (founder/owner) - these are always OK
    for pattern in ALLOWED_TITLES:
        if re.search(pattern, text):
            return False

    # Check for existing SDR/BDR titles
    for pattern in EXISTING_SDR_TITLES:
        if re.search(pattern, text):
            return True

    return False


def is_utah_connected(headline: str, snippet: str) -> bool:
    """Check if a candidate has Utah connections based on headline and snippet.

    Checks for Utah locations, Utah colleges, and Utah tech companies.
    Does NOT use the source query as a positive signal since that's the problem we're solving.
    """
    text = f"{headline} {snippet}".lower()

    # Check Utah location keywords
    for pattern in UTAH_LOCATION_KEYWORDS:
        if re.search(pattern, text):
            return True

    # Check Utah colleges
    for pattern in UTAH_COLLEGES:
        if re.search(pattern, text):
            return True

    # Check Utah tech companies
    for company in UTAH_TECH_COMPANIES:
        if company in text:
            return True

    return False


# Indicators for AE-level candidates
AE_INDICATORS = [
    r'\baccount executive\b',
    r'\bae\b',
    r'\bclosing\b',
    r'\bfull.?cycle\b',
    r'\bmid.?market\b',
    r'\benterprise\b',
    r'\bsmb\b',
    r'\bquota\b',
    r'\b\$\d+[mk]\b',  # Revenue numbers like $500K, $1M
    r'\bclosed\b',
    r'\bsaas\b.*\b(2|3|4)\+?\s*years?\b',  # 2+ years SaaS experience
    r'\b(2|3|4)\+?\s*years?\b.*\bsaas\b',
    r'\bsenior\s*(account|sales)\b',
]

# Indicators for SDR-level candidates
SDR_INDICATORS = [
    r'\bsdr\b',
    r'\bbdr\b',
    r'\bsales development\b',
    r'\bbusiness development representative\b',
    r'\boutbound\b',
    r'\bcold calling\b',
    r'\blead generation\b',
    r'\bprospecting\b',
    r'\bstudent athlete\b',
    r'\bncaa\b',
    r'\bdoor.?to.?door\b',
    r'\bd2d\b',
    r'\brecent graduate\b',
    r'\bentry.?level\b',
    r'\bintern\b',
    r'\brestaurant\b',
    r'\bbartender\b',
    r'\bserver\b',
]

# Utah tech companies (strong AE signal)
UTAH_TECH_COMPANIES = [
    'qualtrics', 'pluralsight', 'podium', 'lucid', 'domo', 'entrata',
    'weave', 'divvy', 'mx', 'instructure', 'vivint', 'healthequity',
    'recursion', 'carta', 'workfront', 'bamboohr', 'workstream'
]


def determine_role_fit(headline: str, source_query: str = '') -> str:
    """
    Determine if a candidate is a better fit for SDR or AE role.

    Returns: 'SDR', 'AE', or 'SDR/AE' (if unclear or could be either)
    """
    if not headline:
        # If no headline, try to infer from the query that found them
        if source_query:
            source_lower = source_query.lower()
            if any(term in source_lower for term in ['account executive', 'ae ', 'saas']):
                return 'AE'
        return 'SDR'  # Default to SDR if no info

    headline_lower = headline.lower()

    ae_score = 0
    sdr_score = 0

    # Check AE indicators
    for pattern in AE_INDICATORS:
        if re.search(pattern, headline_lower):
            ae_score += 1

    # Check SDR indicators
    for pattern in SDR_INDICATORS:
        if re.search(pattern, headline_lower):
            sdr_score += 1

    # Check for Utah tech company experience (strong AE signal)
    for company in UTAH_TECH_COMPANIES:
        if company in headline_lower:
            ae_score += 2  # Weight Utah tech experience heavily
            break

    # Check for years of experience patterns
    years_match = re.search(r'(\d+)\+?\s*years?', headline_lower)
    if years_match:
        years = int(years_match.group(1))
        if years >= 2:
            ae_score += 1
        if years >= 4:
            ae_score += 1

    # Determine role based on scores
    if ae_score > sdr_score and ae_score >= 2:
        return 'AE'
    elif sdr_score > ae_score:
        return 'SDR'
    elif ae_score > 0 and sdr_score > 0:
        return 'SDR/AE'  # Could be transitioning or has mixed experience
    elif ae_score > 0:
        return 'AE'
    else:
        return 'SDR'  # Default to SDR for entry-level/unclear


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
                headline = row.get('Headline', '')
                # Get existing role_type or determine it from headline
                role_type = row.get('Role Fit', '')
                if not role_type:
                    role_type = determine_role_fit(headline)

                candidates.append({
                    'full_name': row.get('Full Name', ''),
                    'linkedin_url': row.get('LinkedIn URL', ''),
                    'headline': headline,
                    'email': row.get('Email', ''),
                    'phone': row.get('Phone', ''),
                    'role_type': role_type,
                    'source_query': row.get('Source Query', '')
                })
        print(f"Loaded {len(candidates)} existing candidates from {filename}")
    except FileNotFoundError:
        pass
    return candidates


def save_to_csv(candidates: List[Dict[str, str]], filename: str = 'candidates.csv'):
    """Save candidates to a CSV file."""
    fieldnames = ['Full Name', 'LinkedIn URL', 'Headline', 'Years of Experience', 'Role Fit', 'Notes', 'Email', 'Phone', 'Date Added', 'Status', 'AI Draft']

    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for candidate in candidates:
            writer.writerow({
                'Full Name': candidate['full_name'],
                'LinkedIn URL': candidate['linkedin_url'],
                'Role Fit': candidate.get('role_type', 'SDR'),
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


def get_existing_urls_from_sheet(worksheet) -> Dict[str, int]:
    """Get all existing LinkedIn URLs from the sheet with their row numbers."""
    try:
        # Get all values from LinkedIn URL column (column B - index 2)
        url_column = worksheet.col_values(2)
        # Skip header, map normalized URLs to row numbers (1-indexed, +1 for header)
        existing_urls = {}
        for idx, url in enumerate(url_column[1:], start=2):  # start=2 because row 1 is header
            if url:
                existing_urls[url.lower().rstrip('/')] = idx
        return existing_urls
    except Exception as e:
        print(f"  Error reading existing URLs: {str(e)}")
        return {}


def get_column_index(worksheet, column_name: str) -> Optional[int]:
    """Find the 1-based column index for a given header name."""
    try:
        first_row = worksheet.row_values(1)
        for idx, header in enumerate(first_row, start=1):
            if header.strip().lower() == column_name.strip().lower():
                return idx
    except Exception:
        pass
    return None


def get_or_create_worksheet(client, sheet_id: str):
    """Get or create the worksheet, returning it along with existing URLs."""
    try:
        spreadsheet = client.open_by_key(sheet_id)

        try:
            worksheet = spreadsheet.worksheet(SHEET_NAME)
        except gspread.WorksheetNotFound:
            print(f"  📋 Creating new worksheet: {SHEET_NAME}")
            worksheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=1000, cols=11)
            headers = ['Full Name', 'LinkedIn URL', 'Headline', 'Years of Experience', 'Role Fit', 'Notes', 'Email', 'Phone', 'Date Added', 'Status', 'AI Draft']
            worksheet.append_row(headers)

        # Check if headers exist
        first_row = worksheet.row_values(1)
        if not first_row or first_row[0] != 'Full Name':
            worksheet.insert_row(['Full Name', 'LinkedIn URL', 'Headline', 'Years of Experience', 'Role Fit', 'Notes', 'Email', 'Phone', 'Date Added', 'Status', 'AI Draft'], 1)

        existing_urls = get_existing_urls_from_sheet(worksheet)
        return worksheet, existing_urls
    except Exception as e:
        print(f"  ❌ Error accessing sheet: {str(e)}")
        return None, {}


def upload_candidate_realtime(worksheet, candidate: Dict[str, str], existing_urls: Dict[str, int], date_added_col: int = None) -> str:
    """Upload a single candidate in real-time. Returns 'new', 'updated', or 'skipped'."""
    if not worksheet:
        return 'skipped'

    today = datetime.now().strftime('%Y-%m-%d')
    url_normalized = candidate['linkedin_url'].lower().rstrip('/')

    # Find the Date Added column dynamically if not provided
    if date_added_col is None:
        date_added_col = get_column_index(worksheet, 'Date Added')
        if date_added_col is None:
            date_added_col = 9  # fallback

    try:
        if url_normalized in existing_urls:
            # Update existing candidate
            row_num = existing_urls[url_normalized]
            role_type = candidate.get('role_type', 'SDR')
            role_col = get_column_index(worksheet, 'Role Fit') or 5
            worksheet.update_cell(row_num, role_col, role_type)
            worksheet.update_cell(row_num, date_added_col, today)
            return 'updated'
        else:
            # Add new candidate
            row = [
                candidate['full_name'],
                candidate['linkedin_url'],
                candidate['headline'],
                '',  # Years of Experience
                candidate.get('role_type', 'SDR'),
                '',  # Notes
                candidate['email'],
                candidate['phone'],
                today,
                '',  # Status
                '',  # AI Draft
            ]
            worksheet.append_row(row)
            # Track the new URL
            new_row_num = len(existing_urls) + 2  # +2 for header and 1-indexing
            existing_urls[url_normalized] = new_row_num
            return 'new'
    except Exception as e:
        print(f"      ⚠️  Sheet error: {str(e)[:50]}")
        return 'skipped'


def upload_to_google_sheets(candidates: List[Dict[str, str]], sheet_id: str = None) -> int:
    """
    Upload candidates to Google Sheets, appending new entries and updating existing ones.
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
            worksheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=1000, cols=11)
            # Add headers
            headers = ['Full Name', 'LinkedIn URL', 'Headline', 'Years of Experience', 'Role Fit', 'Notes', 'Email', 'Phone', 'Date Added', 'Status', 'AI Draft']
            worksheet.append_row(headers)

        # Check if headers exist, add if first row is empty
        first_row = worksheet.row_values(1)
        if not first_row or first_row[0] != 'Full Name':
            print(f"  Adding headers to worksheet")
            worksheet.insert_row(['Full Name', 'LinkedIn URL', 'Headline', 'Years of Experience', 'Role Fit', 'Notes', 'Email', 'Phone', 'Date Added', 'Status', 'AI Draft'], 1)

        # Get existing URLs with their row numbers
        existing_urls = get_existing_urls_from_sheet(worksheet)
        print(f"  Found {len(existing_urls)} existing candidates in sheet")

        today = datetime.now().strftime('%Y-%m-%d')

        # Separate new candidates from existing ones
        new_candidates = []
        candidates_to_update = []

        for candidate in candidates:
            url_normalized = candidate['linkedin_url'].lower().rstrip('/')
            if url_normalized in existing_urls:
                # Existing candidate - mark for update
                row_num = existing_urls[url_normalized]
                candidates_to_update.append((row_num, candidate))
            else:
                new_candidates.append(candidate)

        # Find column indices dynamically by header name
        date_added_col = get_column_index(worksheet, 'Date Added') or 9
        role_fit_col = get_column_index(worksheet, 'Role Fit') or 5

        # Update existing candidates (update Role Fit and Date Added)
        updated_count = 0
        if candidates_to_update:
            print(f"  Updating {len(candidates_to_update)} existing candidates...")
            for row_num, candidate in candidates_to_update:
                try:
                    role_type = candidate.get('role_type', 'SDR')
                    worksheet.update_cell(row_num, role_fit_col, role_type)
                    worksheet.update_cell(row_num, date_added_col, today)
                    updated_count += 1
                except Exception as e:
                    print(f"    Error updating row {row_num}: {str(e)}")
            print(f"  Updated {updated_count} existing candidates with today's date")

        # Append new candidates
        if new_candidates:
            rows_to_add = []
            for candidate in new_candidates:
                rows_to_add.append([
                    candidate['full_name'],
                    candidate['linkedin_url'],
                    candidate['headline'],
                    '',  # Years of Experience
                    candidate.get('role_type', 'SDR'),
                    '',  # Notes
                    candidate['email'],
                    candidate['phone'],
                    today,
                    '',  # Status
                    '',  # AI Draft
                ])

            # Batch append all new rows
            worksheet.append_rows(rows_to_add)
            print(f"  Added {len(new_candidates)} new candidates")
        else:
            print("  No new candidates to add")

        return len(new_candidates), updated_count

    except gspread.SpreadsheetNotFound:
        print(f"  Spreadsheet not found. Check the GOOGLE_SHEET_ID: {sheet_id}")
        return 0, 0
    except Exception as e:
        print(f"  Error uploading to Google Sheets: {str(e)}")
        return 0, 0


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
        print(f"\n📦 Running batch {batch_num} of {total_batches} ({len(queries_to_run)} queries)")
    else:
        print(f"\n🚀 Running all {len(queries_to_run)} queries")

    # Initialize Google Sheets for real-time updates
    worksheet = None
    existing_urls = {}
    date_added_col = None
    if GOOGLE_SHEET_ID and GSPREAD_AVAILABLE:
        print("\n📊 Connecting to Google Sheets...")
        client = get_google_sheets_client()
        if client:
            worksheet, existing_urls = get_or_create_worksheet(client, GOOGLE_SHEET_ID)
            if worksheet:
                date_added_col = get_column_index(worksheet, 'Date Added')
                print(f"   ✓ Connected! {len(existing_urls)} existing candidates in sheet")
                print(f"   ✓ Date Added column: {date_added_col}")

    all_candidates = []
    seen_urls = set()  # Track URLs we've already processed this session
    stats = {'new': 0, 'updated': 0, 'skipped': 0, 'filtered': 0, 'filtered_non_utah': 0, 'filtered_existing_sdr': 0}

    for i, query in enumerate(queries_to_run, 1):
        # Determine if this is an SDR or AE query
        query_type = "AE" if query in AE_GOOGLE_QUERIES or query in AE_DUCKDUCKGO_QUERIES else "SDR"
        print(f"\n{'─' * 50}")
        print(f"🔍 [{i}/{len(queries_to_run)}] {query_type} Search")
        print(f"   {query[:60]}...")

        candidates = search_candidates(query, num_results=RESULTS_PER_QUERY)

        # Process each candidate in real-time
        for candidate in candidates:
            url_normalized = candidate['linkedin_url'].lower().rstrip('/')

            # Skip if already seen this session
            if url_normalized in seen_urls:
                continue
            seen_urls.add(url_normalized)

            # Filter out senior candidates
            if is_too_senior(candidate.get('headline', '')):
                stats['filtered'] += 1
                continue

            # Filter out candidates who already have SDR/BDR experience (for SDR sourcing)
            # We want fresh-from-college or career pivoters, not existing SDRs
            if query_type == "SDR" and is_existing_sdr(candidate.get('headline', ''), candidate.get('snippet', '')):
                stats['filtered_existing_sdr'] += 1
                continue

            # Filter out candidates without Utah connections
            if not is_utah_connected(candidate.get('headline', ''), candidate.get('snippet', '')):
                stats['filtered_non_utah'] += 1
                continue

            all_candidates.append(candidate)

            # Real-time upload to Google Sheets
            if worksheet:
                result = upload_candidate_realtime(worksheet, candidate, existing_urls, date_added_col)
                stats[result] += 1

                # Visual feedback
                role_icon = "🎯" if candidate['role_type'] == 'AE' else "📞" if candidate['role_type'] == 'SDR' else "🔄"
                status_icon = "✨" if result == 'new' else "🔄" if result == 'updated' else "⏭️"
                name = candidate['full_name'] or 'Unknown'
                print(f"      {status_icon} {role_icon} {name[:30]} [{candidate['role_type']}] → Sheet {result}")

        # Rate limiting
        if i < len(queries_to_run):
            if i % BATCH_SIZE == 0:
                print(f"\n   ⏸️  Batch pause ({BATCH_PAUSE}s)...")
                time.sleep(BATCH_PAUSE)
            else:
                sleep_time = random.uniform(MIN_DELAY, MAX_DELAY)
                time.sleep(sleep_time)

    # Final summary
    print("\n" + "═" * 50)
    print("📈 SUMMARY")
    print("═" * 50)

    # Role breakdown
    sdr_count = sum(1 for c in all_candidates if c.get('role_type') == 'SDR')
    ae_count = sum(1 for c in all_candidates if c.get('role_type') == 'AE')
    mixed_count = sum(1 for c in all_candidates if c.get('role_type') == 'SDR/AE')

    print(f"\n👥 Candidates found: {len(all_candidates)}")
    print(f"   📞 SDR: {sdr_count}")
    print(f"   🎯 AE:  {ae_count}")
    print(f"   🔄 Both: {mixed_count}")
    print(f"   🚫 Filtered (too senior): {stats['filtered']}")
    print(f"   🚫 Filtered (existing SDR/BDR): {stats['filtered_existing_sdr']}")
    print(f"   🚫 Filtered (non-Utah): {stats['filtered_non_utah']}")

    if worksheet:
        print(f"\n📊 Google Sheets:")
        print(f"   ✨ New:     {stats['new']}")
        print(f"   🔄 Updated: {stats['updated']}")

    # Save to CSV (local backup)
    if all_candidates:
        # Load and merge with existing
        existing_candidates = load_existing_candidates()
        merged = existing_candidates + all_candidates
        unique_candidates = deduplicate_candidates(merged)
        save_to_csv(unique_candidates)

    print("\n" + "═" * 50)
    print("✅ Search complete!")
    print("═" * 50)


if __name__ == '__main__':
    main()
