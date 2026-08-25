#!/usr/bin/env python3
"""Validate docs/llms.txt and its wiring into the README and the docs site.

Checks:
  1. docs/llms.txt exists and every `- [label](url): description` entry has a
     non-empty description, a unique URL, and a URL that resolves to an
     existing source file (a docs page, the rustdoc landing directory, or a
     file in this repository on GitHub).
  2. docs/_quarto.yml publishes llms.txt as a site resource.
  3. README.md links docs/llms.txt and llms.txt links back to the README, so
     the README stays the single router.
  4. With --docs-site-root, the rendered site carries llms.txt at its root.
"""
from __future__ import annotations

import argparse
import pathlib
import re
import sys
from urllib.parse import unquote, urlsplit

SITE_NETLOC = "shinaoka.github.io"
SITE_PREFIX = "/hataori-rs/"
GITHUB_NETLOC = "github.com"
GITHUB_PREFIX = "/shinaoka/hataori-rs/blob/main/"
README_URL = "https://github.com/shinaoka/hataori-rs/blob/main/README.md"
RUSTDOC_PREFIX = "api/hataori/"

LINK_RE = re.compile(r"^\s*-\s+\[([^]]+)\]\(([^)]+)\):\s*(.*)$", re.MULTILINE)
MARKDOWN_LINK_RE = re.compile(r"\[[^]]*\]\(([^)]+)\)")


def source_path(root: pathlib.Path, url: str):
    """Map a llms.txt URL to the repository file it is published from."""
    parsed = urlsplit(url)
    if parsed.netloc == SITE_NETLOC and parsed.path.startswith(SITE_PREFIX):
        relative = unquote(parsed.path[len(SITE_PREFIX):])
        if relative.startswith(RUSTDOC_PREFIX):
            # Rustdoc is generated from the crate; the crate root is the source.
            return root / "src" / "lib.rs"
        if relative == "" or relative.endswith("/"):
            relative += "index.md"
        elif relative.endswith(".html"):
            relative = relative[:-5] + ".md"
        else:
            return None
        return root / "docs" / relative
    if parsed.netloc == GITHUB_NETLOC and parsed.path.startswith(GITHUB_PREFIX):
        return root / unquote(parsed.path[len(GITHUB_PREFIX):])
    return None


def check_index(root: pathlib.Path, docs_site_root):
    errors = []
    index = root / "docs" / "llms.txt"
    if not index.is_file():
        return ["docs/llms.txt is missing"]
    text = index.read_text(encoding="utf-8")

    quarto = root / "docs" / "_quarto.yml"
    if not quarto.is_file() or not re.search(
        r"(?m)^\s*-\s*llms\.txt\s*$", quarto.read_text(encoding="utf-8")
    ):
        errors.append("docs/_quarto.yml must list llms.txt under project resources")

    entries = list(LINK_RE.finditer(text))
    if not entries:
        errors.append("docs/llms.txt has no described Markdown links")
    seen = set()
    for match in entries:
        label, url, description = match.groups()
        if url in seen:
            errors.append(f"docs/llms.txt repeats URL: {url}")
        seen.add(url)
        if not description.strip():
            errors.append(f"docs/llms.txt has an empty description for: {label}")
        target = source_path(root, url)
        if target is None:
            errors.append(f"docs/llms.txt has an unsupported URL: {url}")
        elif not target.is_file():
            errors.append(
                f"docs/llms.txt target does not exist: {url} -> {target.relative_to(root)}"
            )

    readme = root / "README.md"
    if not readme.is_file():
        errors.append("README.md is missing")
    else:
        readme_targets = set(MARKDOWN_LINK_RE.findall(readme.read_text(encoding="utf-8")))
        if not any(
            t == "docs/llms.txt" or t.endswith("/docs/llms.txt") for t in readme_targets
        ):
            errors.append("README.md must link docs/llms.txt (single router)")
    if README_URL not in set(MARKDOWN_LINK_RE.findall(text)):
        errors.append(f"docs/llms.txt must link back to the README ({README_URL})")

    if docs_site_root is not None and docs_site_root.exists():
        if not (docs_site_root / "llms.txt").is_file():
            errors.append(f"built docs site is missing root llms.txt: {docs_site_root / 'llms.txt'}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root-dir", default=".", help="Repository root (default: current directory)")
    parser.add_argument(
        "--docs-site-root",
        help="Rendered Quarto site root; when given and present, its root llms.txt is required",
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress success output")
    args = parser.parse_args()

    root = pathlib.Path(args.root_dir).resolve()
    site = pathlib.Path(args.docs_site_root).resolve() if args.docs_site_root else None
    errors = check_index(root, site)
    if errors:
        print("llms.txt validation failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    if not args.quiet:
        print("llms-index-ok: docs/llms.txt entries resolve and the README router links it")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
