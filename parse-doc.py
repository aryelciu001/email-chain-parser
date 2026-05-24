"""
Quick test script: parse /test doc format into a list of email dicts.
Usage: python parse-doc.py test/doc1.txt [test/doc2.txt ...]
"""
import re
import sys
from datetime import datetime
from email.utils import getaddresses

from dateutil import parser as dateparser

SEPARATOR = re.compile(r"\n-{40}\n")
# matches "| ", "> ", ">  ", ">> " etc. at the start of every quoted line
QUOTE_PREFIX = re.compile(r"^[|>]+ *", re.MULTILINE)
# inline quoted block starting with "> From:" or ">> From:" in the body
INLINE_QUOTE_START = re.compile(r"\n(>{1,}|[|]) *From:", re.IGNORECASE)
HEADER_RE = re.compile(r"^(From|To|Cc|Date|Subject|Message-ID):\s*(.+)", re.IGNORECASE)


def addrs(header: str | None) -> list[str]:
    if not header:
        return []
    return sorted(a.lower() for _, a in getaddresses([header]) if a)


def strip_prefix(block: str) -> str:
    return QUOTE_PREFIX.sub("", block)


def parse_block(block: str) -> dict:
    lines = block.splitlines()
    headers: dict[str, str] = {}
    body_lines: list[str] = []
    in_body = False

    for line in lines:
        if in_body:
            body_lines.append(line)
            continue
        if line.strip() == "":
            in_body = True
            continue
        m = HEADER_RE.match(line)
        if m:
            key, val = m.group(1).lower().replace("-", "_"), m.group(2).strip()
            # handle folded headers (continuation lines are indented — rare here)
            headers[key] = val
        else:
            # not a header and not blank: treat as start of body
            in_body = True
            body_lines.append(line)

    return {
        "from": addrs(headers.get("from")),
        "to": addrs(headers.get("to")),
        "cc": addrs(headers.get("cc")),
        "date": headers.get("date"),
        "subject": headers.get("subject"),
        "message_id": headers.get("message_id"),
        "content": "\n".join(body_lines).strip(),
    }


def split_inline(block: str) -> list[tuple[str, bool]]:
    """Return list of (text, is_quoted) sub-blocks from a single block.
    Splits on inline '>> From:' / '> From:' patterns in the body.
    """
    parts = []
    remaining = block
    quoted = False
    while True:
        m = INLINE_QUOTE_START.search(remaining)
        if not m:
            parts.append((remaining, quoted))
            break
        parts.append((remaining[: m.start()], quoted))
        remaining = remaining[m.start() + 1:]  # skip leading \n
        quoted = True
    return parts


def parse_date(date_str: str | None) -> datetime:
    if not date_str:
        return datetime.min
    try:
        return dateparser.parse(date_str, ignoretz=True)
    except Exception:
        return datetime.min


def parse_doc(path: str) -> list[dict]:
    with open(path) as f:
        raw = f.read()

    sep_blocks = SEPARATOR.split(raw)
    emails = []
    for sep_idx, block in enumerate(sep_blocks):
        block = block.strip()
        if not block:
            continue
        if sep_idx > 0:
            block = strip_prefix(block)
        sub_blocks = split_inline(block)
        for text, is_quoted in sub_blocks:
            text = text.strip()
            if not text:
                continue
            if is_quoted:
                text = strip_prefix(text)
            emails.append(parse_block(text))

    # assign canon_order by ascending date (oldest = 0)
    emails.sort(key=lambda e: parse_date(e.get("date")))
    for i, email in enumerate(emails):
        email["canon_order"] = i
    return emails


if __name__ == "__main__":
    import json
    import os

    paths = sys.argv[1:] or ["test/doc1.txt"]
    os.makedirs("result", exist_ok=True)
    for path in paths:
        stem = os.path.splitext(os.path.basename(path))[0]  # e.g. "doc1"
        out_path = f"result/{stem}-result.json"
        emails = parse_doc(path)
        with open(out_path, "w") as f:
            json.dump(emails, f, indent=2)
        print(f"{path} -> {out_path} ({len(emails)} emails)")
