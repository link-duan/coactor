#!/usr/bin/env python3
"""Check public documentation language and local Markdown links."""

from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parent.parent
PUBLIC_DOCS = [
    ROOT / "README.md",
    ROOT / "docs" / "getting-started.md",
    ROOT / "docs" / "runtime.md",
    ROOT / "docs" / "s3.md",
    ROOT / "docs" / "testing.md",
]
MARKDOWN_LINK = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
HAN = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff]")

errors: list[str] = []

for path in PUBLIC_DOCS:
    text = path.read_text()
    if HAN.search(text):
        errors.append(f"{path.relative_to(ROOT)}: public documentation must be English")

for path in [ROOT / "README.md", *(ROOT / "docs").rglob("*.md")]:
    for link in MARKDOWN_LINK.findall(path.read_text()):
        target = link.split("#", 1)[0]
        if not target or "://" in target or target.startswith("mailto:"):
            continue
        if not (path.parent / target).resolve().exists():
            errors.append(f"{path.relative_to(ROOT)}: broken local link: {link}")

if errors:
    print("\n".join(errors), file=sys.stderr)
    raise SystemExit(1)

print(f"checked {len(PUBLIC_DOCS)} public documents and local Markdown links")
