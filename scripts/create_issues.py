"""OPTIONAL: create issue drafts in your existing remote GitHub repository with gh CLI.

Usage (after gh auth login and checking repo):
    python scripts/create_issues.py --repo OWNER/REPO --dry-run
    python scripts/create_issues.py --repo OWNER/REPO --create

Does not assign issues automatically. Run once; rerunning --create creates duplicates.
"""
import argparse, json, subprocess
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--repo',required=True)
group=parser.add_mutually_exclusive_group(required=True)
group.add_argument('--dry-run', action='store_true')
group.add_argument('--create', action='store_true')
args=parser.parse_args()
items=json.loads((Path(__file__).resolve().parents[1]/'planning/issues.json').read_text())
for item in items:
    cmd=['gh','issue','create','--repo',args.repo,'--title',item['title'],'--body',f"**Proposed owner:** {item['owner']}\n\n{item['body']}"]
    if args.dry_run:
        print(f"WOULD CREATE: {item['title']} ({item['owner']})")
    else:
        subprocess.run(cmd,check=True)
