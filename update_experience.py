#!/usr/bin/env python3
"""
Update Years of Experience column in Google Sheet.
Estimates experience based on headline text analysis.
"""

import re
import os
from datetime import datetime

try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    print("Please install: pip install gspread google-auth")
    exit(1)

# Configuration
GOOGLE_SHEET_ID = os.environ.get('GOOGLE_SHEET_ID', '16VwCMk5pbInX7_YHMTRSaJp2caBd8v4GK-td6jux5JE')
GOOGLE_CREDENTIALS_FILE = os.environ.get('GOOGLE_CREDENTIALS_FILE', 'credentials.json')
SHEET_NAME = 'candidates'

# Column positions (1-indexed)
COL_HEADLINE = 3  # Column C
COL_YEARS_EXP = 8  # Column H (Years of Experience)


def estimate_years_of_experience(headline: str) -> str:
    """
    Estimate years of experience from headline text.
    Returns a string like "2", "3+", "5-7", or "" if unknown.
    """
    if not headline:
        return ""

    headline_lower = headline.lower()

    # Pattern 1: Explicit years mentioned (e.g., "5 years", "3+ years", "2-3 years")
    years_patterns = [
        r'(\d+)\s*\+?\s*years?\s+(?:of\s+)?(?:experience|exp)',
        r'(\d+)\s*\+?\s*years?\s+in\s+(?:sales|saas|tech|b2b)',
        r'(\d+)\s*-\s*(\d+)\s*years?',
        r'(\d+)\s*\+\s*years?',
        r'(\d+)\s*years?\s+(?:sales|saas|b2b|account)',
    ]

    for pattern in years_patterns:
        match = re.search(pattern, headline_lower)
        if match:
            groups = match.groups()
            if len(groups) == 2 and groups[1]:
                return f"{groups[0]}-{groups[1]}"
            elif groups[0]:
                if '+' in match.group(0):
                    return f"{groups[0]}+"
                return groups[0]

    # Pattern 2: Graduation year to estimate experience
    current_year = datetime.now().year
    grad_patterns = [
        r"class of ['\"]?(\d{4})",
        r"graduated?\s*[:\-]?\s*(\d{4})",
        r"['\"](\d{2})\b",  # '22, '23, etc.
        r"\b(202[0-5])\s+(?:graduate|grad|alumni)",
    ]

    for pattern in grad_patterns:
        match = re.search(pattern, headline_lower)
        if match:
            year_str = match.group(1)
            if len(year_str) == 2:
                year = 2000 + int(year_str)
            else:
                year = int(year_str)

            if 2015 <= year <= current_year:
                years_exp = current_year - year
                if years_exp <= 0:
                    return "<1"
                return str(years_exp)

    # Pattern 3: Title-based estimation
    title_experience = {
        # Entry level (0-1 years)
        r'\b(intern|internship)\b': '<1',
        r'\bstudent\b': '<1',
        r'\bentry.?level\b': '<1',
        r'\brecent\s+grad': '<1',

        # Junior (1-2 years)
        r'\b(sdr|bdr)\b(?!.*(?:manager|lead|senior))': '1-2',
        r'\bjunior\b': '1-2',
        r'\bassociate\b(?!.*director)': '1-2',

        # Mid-level (2-4 years)
        r'\baccount\s+executive\b(?!.*senior)': '2-4',
        r'\b(ae)\b(?!.*senior)': '2-4',
        r'\bmid.?market\b': '2-4',

        # Senior (4+ years)
        r'\bsenior\s+(account\s+executive|ae|sdr)\b': '4+',
        r'\b(smb|enterprise)\s+account\s+executive\b': '3-5',
        r'\bteam\s+lead\b': '3+',
        r'\bsales\s+manager\b': '4+',
    }

    for pattern, exp in title_experience.items():
        if re.search(pattern, headline_lower):
            return exp

    # Pattern 4: Company tenure hints
    tenure_patterns = [
        (r'(\d+)\s*(?:yr|year)s?\s+at\b', lambda m: m.group(1)),
        (r'since\s+(\d{4})\b', lambda m: str(current_year - int(m.group(1)))),
    ]

    for pattern, extractor in tenure_patterns:
        match = re.search(pattern, headline_lower)
        if match:
            try:
                return extractor(match)
            except:
                pass

    # If headline mentions specific sales roles without clear seniority
    if re.search(r'\b(sales|account|business\s+development)\b', headline_lower):
        # Check for experience indicators
        if re.search(r'\b(proven|experienced|seasoned|successful)\b', headline_lower):
            return '3+'

    return ""


def get_sheets_client():
    """Initialize Google Sheets client."""
    try:
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(
            GOOGLE_CREDENTIALS_FILE,
            scopes=scopes
        )
        return gspread.authorize(creds)
    except Exception as e:
        print(f"❌ Error connecting to Google Sheets: {e}")
        return None


def main():
    print("=" * 60)
    print("📊 Updating Years of Experience Column")
    print("=" * 60)

    client = get_sheets_client()
    if not client:
        return

    try:
        spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = spreadsheet.worksheet(SHEET_NAME)
        print(f"✓ Connected to sheet: {SHEET_NAME}")

        # Get all data
        all_data = worksheet.get_all_values()
        headers = all_data[0] if all_data else []

        print(f"📋 Found {len(all_data) - 1} candidates")
        print(f"📑 Headers: {headers}")

        # Find the Years of Experience column
        yoe_col = None
        for idx, header in enumerate(headers):
            if 'year' in header.lower() and 'exp' in header.lower():
                yoe_col = idx + 1  # 1-indexed
                break

        if not yoe_col:
            print("⚠️  'Years of Experience' column not found. Looking for column H...")
            yoe_col = 8  # Default to column H

        print(f"📍 Years of Experience column: {yoe_col}")

        # Process each row
        updates = []
        updated_count = 0
        skipped_count = 0

        for row_idx, row in enumerate(all_data[1:], start=2):  # Skip header, 1-indexed
            # Get headline (column C = index 2)
            headline = row[2] if len(row) > 2 else ""

            # Get current years of experience value
            current_yoe = row[yoe_col - 1] if len(row) >= yoe_col else ""

            # Skip if already has a value
            if current_yoe.strip():
                skipped_count += 1
                continue

            # Estimate years of experience
            estimated_yoe = estimate_years_of_experience(headline)

            if estimated_yoe:
                updates.append({
                    'row': row_idx,
                    'name': row[0] if row else 'Unknown',
                    'headline': headline[:50] + '...' if len(headline) > 50 else headline,
                    'yoe': estimated_yoe
                })

        print(f"\n📝 Found {len(updates)} candidates to update")
        print(f"⏭️  Skipped {skipped_count} (already have values)")

        if not updates:
            print("\n✅ No updates needed!")
            return

        # Show preview
        print("\n📋 Preview of updates:")
        print("-" * 60)
        for update in updates[:10]:
            print(f"  Row {update['row']}: {update['name'][:20]} → {update['yoe']} yrs")
            print(f"         Headline: {update['headline']}")

        if len(updates) > 10:
            print(f"  ... and {len(updates) - 10} more")

        print("-" * 60)

        # Perform batch update (much more efficient)
        print(f"\n🔄 Updating {len(updates)} cells using batch update...")

        # Prepare batch update data
        # Format: list of {'range': 'A1', 'values': [[value]]}
        batch_data = []
        col_letter = chr(ord('A') + yoe_col - 1)  # Convert column number to letter

        for update in updates:
            cell_ref = f"{col_letter}{update['row']}"
            batch_data.append({
                'range': cell_ref,
                'values': [[update['yoe']]]
            })

        try:
            # Batch update all cells at once
            worksheet.batch_update(batch_data)
            updated_count = len(updates)
            print(f"   ✓ Batch updated {updated_count} cells")
        except Exception as e:
            print(f"   ❌ Batch update error: {e}")
            print("   Falling back to individual updates with rate limiting...")

            # Fallback to individual updates with delay
            import time
            for i, update in enumerate(updates):
                try:
                    worksheet.update_cell(update['row'], yoe_col, update['yoe'])
                    updated_count += 1

                    # Rate limiting - pause every 10 updates
                    if (i + 1) % 10 == 0:
                        print(f"   ✓ Updated {i + 1}/{len(updates)} - pausing...")
                        time.sleep(10)

                except Exception as e2:
                    print(f"   ❌ Error updating row {update['row']}: {e2}")

        print(f"\n{'=' * 60}")
        print(f"✅ Complete! Updated {updated_count} candidates")
        print(f"{'=' * 60}")

    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == '__main__':
    main()
