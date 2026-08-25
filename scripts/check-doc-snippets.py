#!/usr/bin/env python3
"""Keeps Rust code blocks in the user-facing docs in sync with runnable sources.

A doc marks a block with

    <!-- snippet-source: examples/serial_mandelbrot.rs#serial-map -->
    ```rust
    ...
    ```
    <!-- end-snippet-source -->

and this script replaces the fenced block with the region between
`// snippet-start:NAME` and `// snippet-end:NAME` in that file (or the whole
file when no `#NAME` is given). `--check` fails instead of rewriting, and also
fails on any unmarked ```rust fence in docs/getting-started, docs/guides, and
docs/tutorials so every published snippet is compiled and run by
`scripts/check-tutorial-examples.sh`.
"""
from __future__ import annotations

import argparse
import pathlib
import re
import sys

START_RE = re.compile(r"<!--\s*snippet-source:\s*([^>]+?)\s*-->")
END_RE = re.compile(r"<!--\s*end-snippet-source\s*-->")
REGION_RE = re.compile(r"^\s*//\s*snippet-(start|end):([A-Za-z0-9_-]+)\s*$")
CHECKED_DIRS = ("getting-started", "guides", "tutorials")
EXCLUDED_DIRS = {"design", "superpowers", "worklogs", "api"}


def source_regions(source: pathlib.Path) -> dict[str, str]:
    lines = source.read_text(encoding="utf-8").splitlines(keepends=True)
    regions: dict[str, str] = {}
    active: tuple[str, int] | None = None
    for index, line in enumerate(lines):
        marker = REGION_RE.match(line.rstrip("\r\n"))
        if marker is None:
            continue
        kind, name = marker.groups()
        if kind == "start":
            if active is not None:
                raise ValueError(f"{source}: nested snippet region {name!r}")
            if name in regions:
                raise ValueError(f"{source}: duplicate snippet region {name!r}")
            active = (name, index)
        elif active is None:
            raise ValueError(f"{source}: region end without start: {name!r}")
        else:
            active_name, start = active
            if active_name != name:
                raise ValueError(
                    f"{source}: expected end for {active_name!r}, got {name!r}"
                )
            content = "".join(lines[start + 1 : index])
            if not content.strip():
                raise ValueError(f"{source}: empty snippet region {name!r}")
            regions[name] = dedent_region(content)
            active = None
    if active is not None:
        raise ValueError(f"{source}: region missing end: {active[0]!r}")
    return regions


def dedent_region(content: str) -> str:
    lines = content.splitlines(keepends=True)
    indents = [len(l) - len(l.lstrip(" ")) for l in lines if l.strip()]
    cut = min(indents) if indents else 0
    return "".join(l[cut:] if l.strip() else l.lstrip(" ") for l in lines)


def snippet_source(root: pathlib.Path, doc: pathlib.Path, source_rel: str) -> str:
    source_name, separator, region_name = source_rel.partition("#")
    source = (root / source_name).resolve()
    try:
        source.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{doc}: snippet source escapes repository: {source_rel}") from exc
    if not source.is_file():
        raise ValueError(f"{doc}: snippet source does not exist: {source_rel}")
    if not separator:
        return source.read_text(encoding="utf-8")
    regions = source_regions(source)
    try:
        return regions[region_name]
    except KeyError as exc:
        raise ValueError(f"{doc}: unknown region {region_name!r} in {source_rel}") from exc


def fenced(source: str) -> str:
    return "```rust\n" + source.rstrip() + "\n```\n"


def rewrite_doc(root: pathlib.Path, doc: pathlib.Path) -> tuple[str, bool]:
    text = doc.read_text(encoding="utf-8")
    out: list[str] = []
    pos = 0
    changed = False
    while True:
        start = START_RE.search(text, pos)
        stray_end = END_RE.search(text, pos)
        if stray_end and (not start or stray_end.start() < start.start()):
            raise ValueError(f"{doc}: end-snippet-source without snippet-source")
        if not start:
            out.append(text[pos:])
            break
        end = END_RE.search(text, start.end())
        if not end:
            raise ValueError(f"{doc}: missing end-snippet-source marker")
        replacement = (
            text[start.start() : start.end()]
            + "\n"
            + fenced(snippet_source(root, doc, start.group(1).strip()))
            + text[end.start() : end.end()]
        )
        current = text[start.start() : end.end()]
        out.append(text[pos : start.start()])
        out.append(replacement)
        changed = changed or current != replacement
        pos = end.end()
    return "".join(out), changed


def user_facing_docs(root: pathlib.Path) -> list[pathlib.Path]:
    docs_root = root / "docs"
    docs = [root / "README.md"]
    for path in sorted(docs_root.rglob("*.md")):
        relative = path.relative_to(docs_root)
        if relative.parts[0] in EXCLUDED_DIRS:
            continue
        docs.append(path)
    return docs


def unmarked_rust_fences(root: pathlib.Path) -> list[str]:
    found: list[str] = []
    for directory in CHECKED_DIRS:
        for doc in sorted((root / "docs" / directory).glob("*.md")):
            marked = False
            for number, line in enumerate(doc.read_text(encoding="utf-8").splitlines(), 1):
                if START_RE.search(line):
                    marked = True
                elif END_RE.search(line):
                    marked = False
                elif line.strip() == "```rust" and not marked:
                    found.append(f"{doc.relative_to(root)}:{number}")
    return found


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root-dir", default=pathlib.Path(__file__).resolve().parents[1])
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    root = pathlib.Path(args.root_dir).resolve()

    changed_docs: list[pathlib.Path] = []
    try:
        for doc in user_facing_docs(root):
            new_text, changed = rewrite_doc(root, doc)
            if changed:
                changed_docs.append(doc)
                if not args.check:
                    doc.write_text(new_text, encoding="utf-8")
        unmarked = unmarked_rust_fences(root)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.check and unmarked:
        print(f"unmarked plain Rust fences ({len(unmarked)}):", file=sys.stderr)
        for fence in unmarked:
            print(f"- {fence}", file=sys.stderr)
        return 1
    if changed_docs and args.check:
        print("stale doc snippets:", file=sys.stderr)
        for doc in changed_docs:
            print(f"- {doc.relative_to(root)}", file=sys.stderr)
        print("run: python3 scripts/check-doc-snippets.py", file=sys.stderr)
        return 1
    if changed_docs:
        print(f"updated {len(changed_docs)} doc snippet file(s)")
    else:
        print("doc snippets are up to date")
    return 0


if __name__ == "__main__":
    sys.exit(main())
