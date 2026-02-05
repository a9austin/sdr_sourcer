#!/usr/bin/env python3
"""
Agent Commands - Lightweight wrapper for Claude agent interactions.
Provides simple functions that can be called directly or via CLI.
"""

import subprocess
import sys
import csv
from datetime import datetime
from typing import Optional


def stats():
    """Get current sourcing statistics."""
    result = subprocess.run(
        [sys.executable, 'sdr_candidate_sourcer.py', '--stats'],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr)
    return result.returncode


def source(count: int = 10, role_type: str = 'both'):
    """
    Source a specific number of candidates.

    Args:
        count: Number of new candidates to find
        role_type: 'sdr', 'ae', or 'both'
    """
    cmd = [sys.executable, 'sdr_candidate_sourcer.py', '--count', str(count), '--type', role_type]
    result = subprocess.run(cmd, capture_output=False)
    return result.returncode


def source_sdr(count: int = 10):
    """Source SDR candidates."""
    return source(count, 'sdr')


def source_ae(count: int = 5):
    """Source AE candidates."""
    return source(count, 'ae')


def dry_run(role_type: str = 'both'):
    """Preview what queries would be run."""
    cmd = [sys.executable, 'sdr_candidate_sourcer.py', '--dry-run', '--type', role_type]
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    return result.returncode


def update_experience():
    """Run experience estimation on candidates."""
    result = subprocess.run(
        [sys.executable, 'update_experience.py'],
        capture_output=False
    )
    return result.returncode


def recent_candidates(n: int = 10):
    """Show the N most recently added candidates from local CSV."""
    try:
        with open('candidates.csv', 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            candidates = list(reader)

        # Sort by date if available, otherwise just take last N
        candidates = candidates[-n:]

        print(f"\n{'='*60}")
        print(f"📋 {len(candidates)} Most Recent Candidates")
        print('='*60)

        for i, c in enumerate(candidates, 1):
            name = c.get('Full Name', 'Unknown')[:25]
            role = c.get('Role Fit', 'SDR')
            headline = c.get('Headline', '')[:40]
            print(f"\n{i}. {name} [{role}]")
            print(f"   {headline}...")

        print('\n' + '='*60)
        return 0
    except FileNotFoundError:
        print("No candidates.csv file found.")
        return 1
    except Exception as e:
        print(f"Error reading candidates: {e}")
        return 1


def custom_query(query: str):
    """Run a custom search query."""
    cmd = [sys.executable, 'sdr_candidate_sourcer.py', '--query', query]
    result = subprocess.run(cmd, capture_output=False)
    return result.returncode


def help():
    """Show available commands."""
    print("""
SDR Sourcer - Agent Commands
=============================

Available functions:
  stats()                  - Show current sourcing statistics
  source(count, type)      - Source candidates (type: 'sdr', 'ae', 'both')
  source_sdr(count)        - Source SDR candidates
  source_ae(count)         - Source AE candidates
  dry_run(type)            - Preview queries without executing
  update_experience()      - Estimate years of experience
  recent_candidates(n)     - Show N most recent candidates
  custom_query(query)      - Run a custom search query

CLI usage:
  python agent_commands.py stats
  python agent_commands.py source 10 sdr
  python agent_commands.py source_ae 5
  python agent_commands.py dry_run
  python agent_commands.py recent 10
  python agent_commands.py update_experience
""")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        help()
        sys.exit(0)

    command = sys.argv[1].lower()

    if command == 'stats':
        sys.exit(stats())
    elif command == 'source':
        count = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        role_type = sys.argv[3] if len(sys.argv) > 3 else 'both'
        sys.exit(source(count, role_type))
    elif command == 'source_sdr':
        count = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        sys.exit(source_sdr(count))
    elif command == 'source_ae':
        count = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        sys.exit(source_ae(count))
    elif command == 'dry_run':
        role_type = sys.argv[2] if len(sys.argv) > 2 else 'both'
        sys.exit(dry_run(role_type))
    elif command == 'update_experience':
        sys.exit(update_experience())
    elif command == 'recent':
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        sys.exit(recent_candidates(n))
    elif command == 'help':
        help()
    else:
        print(f"Unknown command: {command}")
        help()
        sys.exit(1)
