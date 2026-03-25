#!/usr/bin/env python3
"""
EVE Imperium — Icon Builder
Generates type icons (including composited blueprint icons) using the eve_icon_builder.
Output: icons.zip containing type_{typeId}_64.png files.

Usage:
    python3 build_icons.py [options]

Options:
    --out PATH            Output ZIP path (default: icons.zip)
    --cache-dir PATH      Cache directory for EVE game files (default: ./icon_cache)
    --icon-dir PATH       Working directory for generated icons (default: ./icon_work)
    --skip-skins          Skip SKIN type icons
    --force               Force rebuild all icons

Requirements:
    pip install requests Pillow numpy
"""

import argparse
import sys
import os
import time
from zipfile import ZipFile, ZIP_STORED

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from eve_icon_builder.cache import CacheDownloader, CacheError
from eve_icon_builder.sde import (
    update_sde, read_types, read_group_categories,
    read_icons, read_graphics, read_skin_materials,
)
from eve_icon_builder.icons import IconBuildData, build_icon_export
from pathlib import Path


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="EVE Imperium Icon Builder")
    parser.add_argument("--out", default="icons.zip", help="Output ZIP path")
    parser.add_argument("--cache-dir", default="./icon_cache", help="Cache directory")
    parser.add_argument("--icon-dir", default="./icon_work", help="Icon working directory")
    parser.add_argument("--skip-skins", action="store_true", help="Skip SKIN icons")
    parser.add_argument("--force", action="store_true", help="Force rebuild")
    args = parser.parse_args()

    user_agent = "EveImperium-SDE/1.0 (icon-builder)"
    start = time.time()

    log("Initializing EVE game cache...")
    cache = CacheDownloader(Path(args.cache_dir), user_agent)
    log(f"EVE client version: {cache.client_version()}")

    log("Downloading/updating SDE (JSONL)...")
    sde = update_sde(silent_mode=False)

    log("Parsing SDE data...")
    data = IconBuildData(
        types=read_types(sde, silent_mode=False),
        group_categories=read_group_categories(sde, silent_mode=False),
        icon_files=read_icons(sde, silent_mode=False),
        graphics_folders=read_graphics(sde, silent_mode=False),
        skin_materials=read_skin_materials(sde, silent_mode=False),
    )
    sde.close()
    log(f"Loaded {len(data.types)} types")

    log("Building icons (IEC format with type_ prefix)...")
    added, removed = build_icon_export(
        output_mode="iec",
        skip_output_if_fresh=False,
        data=data,
        cache=cache,
        icon_dir=Path(args.icon_dir),
        force_rebuild=args.force,
        silent_mode=False,
        skip_skins=args.skip_skins,
        out=args.out,
    )

    log("Appending Icons/items/ attribute icons to output...")
    attr_count = 0
    with ZipFile(args.out, "a", ZIP_STORED) as zf:
        existing = set(zf.namelist())
        for icon_id, resource in data.icon_files.items():
            if not resource.endswith(".png"):
                continue
            parts = resource.rsplit("/", 1)
            if len(parts) < 2:
                continue
            filename = parts[-1]
            arc_name = f"Icons/items/{filename}"
            if arc_name in existing:
                continue
            try:
                resource_path = cache.path_of(resource)
                zf.write(str(resource_path), arc_name)
                attr_count += 1
            except (CacheError, Exception):
                pass
    log(f"Added {attr_count} attribute icons to Icons/items/")

    elapsed = time.time() - start
    log(f"Done in {elapsed:.1f}s ({added} new type icons, {removed} removed)")
    log(f"Output: {args.out}")

    cache.purge(["sde.zip"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
