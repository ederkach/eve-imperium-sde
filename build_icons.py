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

    log("Building icons (IEC format with type_ prefix, no renders/bpc)...")
    added, removed = build_icon_export(
        output_mode="iec",
        skip_output_if_fresh=False,
        data=data,
        cache=cache,
        icon_dir=Path(args.icon_dir),
        force_rebuild=args.force,
        silent_mode=False,
        skip_skins=args.skip_skins,
        skip_renders=True,
        skip_bpc=True,
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
                existing.add(arc_name)
                attr_count += 1
            except (CacheError, Exception):
                pass
    log(f"Added {attr_count} attribute icons to Icons/items/")

    log("Appending category icons to output...")
    CATEGORY_ICON_MAP = {
        0: "res:/ui/texture/icons/7_64_4.png",
        1: "res:/ui/texture/icons/70_128_11.png",
        2: "type_6_64.png",
        3: "type_1932_64.png",
        4: "type_34_64.png",
        5: "type_29668_64.png",
        6: "res:/ui/texture/icons/26_64_2.png",
        7: "res:/ui/texture/icons/2_64_11.png",
        8: "res:/ui/texture/icons/5_64_2.png",
        9: "type_1002_64.png",
        10: "res:/ui/texture/icons/6_64_3.png",
        11: "res:/ui/texture/icons/26_64_10.png",
        14: "res:/ui/texture/icons/modules/fleetboost_infobase.png",
        16: "type_2403_64.png",
        17: "type_11068_64.png",
        18: "type_2454_64.png",
        20: "res:/ui/texture/icons/40_64_16.png",
        22: "type_33475_64.png",
        23: "type_17174_64.png",
        24: "res:/ui/texture/icons/comprfuel_amarr.png",
        25: "res:/ui/texture/icons/inventory/moonasteroid_r4.png",
        30: "res:/ui/texture/icons/inventory/cratexvishirt.png",
        32: "res:/ui/texture/icons/76_64_7.png",
        34: "type_30752_64.png",
        35: "res:/ui/texture/icons/55_64_11.png",
        39: "res:/ui/texture/icons/95_64_6.png",
        40: "type_32458_64.png",
        41: "type_2409_64.png",
        42: "res:/ui/texture/icons/97_64_10.png",
        43: "res:/ui/texture/icons/99_64_8.png",
        46: "type_2233_64.png",
        63: "type_19658_64.png",
        65: "type_40340_64.png",
        66: "type_35923_64.png",
        87: "type_23061_64.png",
        91: "res:/ui/texture/icons/rewardtrack/crateskincontainer.png",
        2100: "type_57203_64.png",
        2118: "type_83291_64.png",
        2143: "type_81143_64.png",
    }
    cat_count = 0
    with ZipFile(args.out, "a", ZIP_STORED) as zf:
        existing = set(zf.namelist())
        for cat_id, source in CATEGORY_ICON_MAP.items():
            arc_name = f"category_{cat_id}.png"
            if arc_name in existing:
                continue
            try:
                if source.startswith("res:/"):
                    resource_path = cache.path_of(source)
                    zf.write(str(resource_path), arc_name)
                    cat_count += 1
                elif source.startswith("type_"):
                    if source in existing:
                        with zf.open(source) as src:
                            zf.writestr(arc_name, src.read())
                        cat_count += 1
                    else:
                        type_path = Path(args.icon_dir) / source
                        if type_path.exists():
                            zf.write(str(type_path), arc_name)
                            cat_count += 1
            except (CacheError, Exception) as e:
                log(f"  Warning: failed category_{cat_id}: {e}")
        existing.add(arc_name)
    log(f"Added {cat_count} category icons")

    elapsed = time.time() - start
    log(f"Done in {elapsed:.1f}s ({added} new type icons, {removed} removed)")
    log(f"Output: {args.out}")

    cache.purge(["sde.zip"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
