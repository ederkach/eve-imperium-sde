#!/usr/bin/env python3
"""
EVE Imperium — SDE Generator
Builds item_db_en.sqlite from the CCP Static Data Export (SDE) YAML files.

Usage:
    python3 scripts/generate_sde.py [options]

Options:
    --sde-zip PATH        Path to already-downloaded sde.zip (skips download)
    --sde-dir PATH        Path to already-extracted sde/ directory (skips download+extract)
    --out PATH            Output SQLite path (default: composeApp/src/commonMain/composeResources/files/item_db_en.sqlite)
    --ru-descriptions     Fetch Russian descriptions for all types from ESI (slow, ~30 min)
    --workers N           Thread count for --ru-descriptions (default: 30)

Requirements:
    pip install pyyaml requests
"""

import argparse
import json
import os
import pickle
import sqlite3
import sys
import time
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

try:
    import yaml
    try:
        from yaml import CLoader as YamlLoader
    except ImportError:
        from yaml import SafeLoader as YamlLoader
except ImportError:
    print("PyYAML not found. Install with: pip install pyyaml")
    sys.exit(1)

SDE_URL = "https://developers.eveonline.com/static-data/eve-online-static-data-latest-yaml.zip"
ESI_BASE = "https://esi.evetech.net/latest"
DEFAULT_OUT = "composeApp/src/commonMain/composeResources/files/item_db_en.sqlite"

PLANET_TYPE_TO_COLUMN = {
    2016: "temperate",
    2015: "oceanic",
    2017: "ice",
    2063: "gas",
    13:   "lava",
    11:   "barren",
    2025: "storm",
    2024: "plasma",
}

SKILL_REQ_ATTR_PAIRS = [
    (182, 277),
    (183, 278),
    (184, 279),
    (1285, 1286),
    (1289, 1287),
    (1290, 1288),
]


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def download_sde(dest_path: str):
    log(f"Downloading SDE from {SDE_URL} ...")
    log("This is ~1 GB — may take 10–30 minutes depending on connection.")

    def progress(count, block_size, total_size):
        if total_size > 0 and count % 200 == 0:
            pct = min(count * block_size * 100 // total_size, 100)
            mb = count * block_size // (1024 * 1024)
            print(f"\r  {pct}%  {mb} MB", end="", flush=True)

    urllib.request.urlretrieve(SDE_URL, dest_path, reporthook=progress)
    print()
    log(f"Downloaded to {dest_path}")


def extract_sde(zip_path: str, extract_dir: str):
    log(f"Extracting {zip_path} → {extract_dir} ...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)
    log("Extraction complete.")


def load_yaml(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.load(f, Loader=YamlLoader)


def load_fsd_strings(sde_dir: str) -> dict:
    """Load English string lookup from EVE SDE localization pickle (nameID -> text)."""
    for subdir in ("fsd", ""):
        candidate = os.path.join(sde_dir, subdir, "localization_fsd_en-us.pickle") if subdir else os.path.join(sde_dir, "localization_fsd_en-us.pickle")
        if os.path.exists(candidate):
            break
    else:
        candidate = os.path.join(sde_dir, "fsd", "localization_fsd_en-us.pickle")
    pickle_path = candidate
    if not os.path.exists(pickle_path):
        log("  localization_fsd_en-us.pickle not found — nameID refs will resolve as empty")
        return {}
    try:
        with open(pickle_path, "rb") as f:
            data = pickle.load(f)
        result = {}
        for k, v in data.items():
            try:
                if isinstance(v, dict):
                    text = v.get("text") or v.get("en") or ""
                elif isinstance(v, str):
                    text = v
                else:
                    continue
                result[int(k)] = text
            except (TypeError, ValueError):
                pass
        log(f"  Loaded {len(result)} localization strings")
        return result
    except Exception as e:
        log(f"  Warning: could not load localization pickle: {e}")
        return {}


def fsd_path(sde_dir: str, *names: str) -> str:
    for name in names:
        for subdir in ("fsd", ""):
            p = os.path.join(sde_dir, subdir, name) if subdir else os.path.join(sde_dir, name)
            if os.path.exists(p):
                return p
    return os.path.join(sde_dir, "fsd", names[0])


def multiname(entry, field="name") -> dict:
    v = entry.get(field)
    if v is None:
        return {}
    if isinstance(v, dict):
        return v
    return {"en": str(v)}


def load_icon_filenames(sde_dir: str) -> dict:
    """Load iconID -> short filename from iconIDs.yaml or icons.yaml.
    Returns {iconID: "4_64_9"} (basename without path/extension).
    """
    path = fsd_path(sde_dir, "iconIDs.yaml", "icons.yaml")
    if not os.path.exists(path):
        return {}
    try:
        data = load_yaml(path)
        result = {}
        for icon_id, entry in data.items():
            if not isinstance(entry, dict):
                continue
            icon_file = entry.get("iconFile") or ""
            if icon_file:
                basename = icon_file.split("/")[-1]
                if basename.endswith(".png"):
                    basename = basename[:-4]
                result[int(icon_id)] = basename
        log(f"  Loaded {len(result)} icon filenames from iconIDs.yaml")
        return result
    except Exception as e:
        log(f"  Warning: could not load iconIDs.yaml: {e}")
        return {}


def resolve_name_id(name_id, fsd_strings: dict) -> str:
    """Resolve a nameID value to an English string.
    nameID can be:
      - an int  → look up in fsd_strings (localization pickle)
      - a dict  → already a multilingual map, return entry for 'en'
      - a str   → treat as literal name
    """
    if name_id is None:
        return ""
    if isinstance(name_id, dict):
        return name_id.get("en") or name_id.get("de") or name_id.get("zh") or ""
    if isinstance(name_id, str):
        return name_id
    try:
        return fsd_strings.get(int(name_id), "")
    except (TypeError, ValueError):
        return ""


def esi_get(url: str):
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "eve-imperium/sde-generator"})
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except Exception:
        return None


def create_schema(conn: sqlite3.Connection):
    conn.executescript("""
        PRAGMA journal_mode=OFF;
        PRAGMA synchronous=OFF;
        PRAGMA cache_size=-65536;
        PRAGMA temp_store=MEMORY;
        PRAGMA locking_mode=EXCLUSIVE;

        CREATE TABLE IF NOT EXISTS categories (
            category_id INTEGER NOT NULL PRIMARY KEY,
            name TEXT,
            de_name TEXT, en_name TEXT, es_name TEXT,
            fr_name TEXT, ja_name TEXT, ko_name TEXT,
            ru_name TEXT, zh_name TEXT,
            icon_filename TEXT, iconID INTEGER, published BOOLEAN
        );

        CREATE TABLE IF NOT EXISTS groups (
            group_id INTEGER NOT NULL PRIMARY KEY,
            name TEXT,
            de_name TEXT, en_name TEXT, es_name TEXT,
            fr_name TEXT, ja_name TEXT, ko_name TEXT,
            ru_name TEXT, zh_name TEXT,
            iconID INTEGER, categoryID INTEGER,
            anchorable BOOLEAN, anchored BOOLEAN,
            fittableNonSingleton BOOLEAN, published BOOLEAN,
            useBasePrice BOOLEAN, icon_filename TEXT,
            representative_type_id INTEGER
        );

        CREATE TABLE IF NOT EXISTS metaGroups (
            metagroup_id INTEGER NOT NULL PRIMARY KEY,
            name TEXT
        );

        CREATE TABLE IF NOT EXISTS types (
            type_id INTEGER NOT NULL PRIMARY KEY,
            name TEXT,
            de_name TEXT, en_name TEXT, es_name TEXT,
            fr_name TEXT, ja_name TEXT, ko_name TEXT,
            ru_name TEXT, zh_name TEXT,
            description TEXT,
            description_ru TEXT,
            icon_filename TEXT, bpc_icon_filename TEXT,
            published BOOLEAN, volume REAL, repackaged_volume REAL,
            capacity REAL, mass REAL,
            marketGroupID INTEGER, metaGroupID INTEGER, iconID INTEGER,
            groupID INTEGER, group_name TEXT,
            categoryID INTEGER, category_name TEXT,
            pg_need REAL, cpu_need REAL, rig_cost INTEGER,
            em_damage REAL, them_damage REAL, kin_damage REAL, exp_damage REAL,
            high_slot INTEGER, mid_slot INTEGER, low_slot INTEGER,
            rig_slot INTEGER, gun_slot INTEGER, miss_slot INTEGER,
            variationParentTypeID INTEGER, process_size INTEGER,
            npc_ship_scene TEXT, npc_ship_faction TEXT,
            npc_ship_type TEXT, npc_ship_faction_icon TEXT
        );

        CREATE TABLE IF NOT EXISTS dogmaAttributeCategories (
            attribute_category_id INTEGER NOT NULL PRIMARY KEY,
            name TEXT, description TEXT
        );

        CREATE TABLE IF NOT EXISTS dogmaAttributes (
            attribute_id INTEGER NOT NULL PRIMARY KEY,
            categoryID INTEGER, name TEXT, display_name TEXT,
            tooltipDescription TEXT, iconID INTEGER, icon_filename TEXT,
            unitID INTEGER, stackable BOOLEAN, highIsGood BOOLEAN,
            defaultValue REAL, published BOOLEAN, display_when_zero BOOLEAN
        );

        CREATE TABLE IF NOT EXISTS dogmaEffects (
            effect_id INTEGER NOT NULL PRIMARY KEY,
            effect_category INTEGER, effect_name TEXT, display_name TEXT,
            description TEXT, published BOOLEAN,
            is_assistance BOOLEAN, is_offensive BOOLEAN,
            resistance_attribute_id INTEGER, modifier_info TEXT
        );

        CREATE TABLE IF NOT EXISTS typeAttributes (
            type_id INTEGER NOT NULL,
            attribute_id INTEGER NOT NULL,
            value REAL,
            PRIMARY KEY (type_id, attribute_id)
        );

        CREATE TABLE IF NOT EXISTS typeEffects (
            type_id INTEGER NOT NULL,
            effect_id INTEGER NOT NULL,
            is_default BOOLEAN,
            PRIMARY KEY (type_id, effect_id)
        );

        CREATE TABLE IF NOT EXISTS typeSkillRequirement (
            typeid INTEGER NOT NULL,
            typename TEXT, typeicon TEXT, published BOOLEAN,
            categoryID INTEGER, category_name TEXT,
            required_skill_id INTEGER NOT NULL,
            required_skill_level INTEGER,
            PRIMARY KEY (typeid, required_skill_id)
        );

        CREATE TABLE IF NOT EXISTS regions (
            regionID INTEGER NOT NULL PRIMARY KEY,
            regionName TEXT,
            regionName_de TEXT, regionName_en TEXT, regionName_es TEXT,
            regionName_fr TEXT, regionName_ja TEXT, regionName_ko TEXT,
            regionName_ru TEXT, regionName_zh TEXT
        );

        CREATE TABLE IF NOT EXISTS constellations (
            constellationID INTEGER NOT NULL PRIMARY KEY,
            constellationName TEXT,
            constellationName_de TEXT, constellationName_en TEXT,
            constellationName_es TEXT, constellationName_fr TEXT,
            constellationName_ja TEXT, constellationName_ko TEXT,
            constellationName_ru TEXT, constellationName_zh TEXT
        );

        CREATE TABLE IF NOT EXISTS solarsystems (
            solarSystemID INTEGER NOT NULL PRIMARY KEY,
            solarSystemName TEXT,
            solarSystemName_de TEXT, solarSystemName_en TEXT,
            solarSystemName_es TEXT, solarSystemName_fr TEXT,
            solarSystemName_ja TEXT, solarSystemName_ko TEXT,
            solarSystemName_ru TEXT, solarSystemName_zh TEXT,
            security_status REAL
        );

        CREATE TABLE IF NOT EXISTS universe (
            region_id INTEGER NOT NULL,
            constellation_id INTEGER NOT NULL,
            solarsystem_id INTEGER NOT NULL,
            system_security REAL,
            system_type INTEGER,
            x REAL, y REAL, z REAL,
            hasStation BOOLEAN NOT NULL DEFAULT 0,
            hasJumpGate BOOLEAN NOT NULL DEFAULT 0,
            isJSpace BOOLEAN NOT NULL DEFAULT 0,
            jove BOOLEAN NOT NULL DEFAULT 0,
            temperate INTEGER NOT NULL DEFAULT 0,
            barren INTEGER NOT NULL DEFAULT 0,
            oceanic INTEGER NOT NULL DEFAULT 0,
            ice INTEGER NOT NULL DEFAULT 0,
            gas INTEGER NOT NULL DEFAULT 0,
            lava INTEGER NOT NULL DEFAULT 0,
            storm INTEGER NOT NULL DEFAULT 0,
            plasma INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (region_id, constellation_id, solarsystem_id)
        );

        CREATE TABLE IF NOT EXISTS stations (
            stationID INTEGER NOT NULL PRIMARY KEY,
            stationTypeID INTEGER, stationName TEXT,
            regionID INTEGER, solarSystemID INTEGER, security REAL
        );

        CREATE TABLE IF NOT EXISTS factions (
            id INTEGER NOT NULL PRIMARY KEY,
            name TEXT,
            de_name TEXT, en_name TEXT, es_name TEXT,
            fr_name TEXT, ja_name TEXT, ko_name TEXT,
            ru_name TEXT, zh_name TEXT,
            description TEXT, shortDescription TEXT, iconName TEXT
        );

        CREATE TABLE IF NOT EXISTS npcCorporations (
            corporation_id INTEGER NOT NULL PRIMARY KEY,
            name TEXT,
            de_name TEXT, en_name TEXT, es_name TEXT,
            fr_name TEXT, ja_name TEXT, ko_name TEXT,
            ru_name TEXT, zh_name TEXT,
            description TEXT, faction_id INTEGER,
            militia_faction INTEGER, icon_filename TEXT
        );

        CREATE TABLE IF NOT EXISTS divisions (
            division_id INTEGER NOT NULL PRIMARY KEY,
            name TEXT
        );

        CREATE TABLE IF NOT EXISTS agents (
            agent_id INTEGER NOT NULL PRIMARY KEY,
            agent_type INTEGER, corporationID INTEGER, divisionID INTEGER,
            isLocator INTEGER, level INTEGER, locationID INTEGER,
            solarSystemID INTEGER, agent_name TEXT
        );

        CREATE TABLE IF NOT EXISTS planetSchematics (
            schematic_id INTEGER NOT NULL,
            output_typeid INTEGER NOT NULL PRIMARY KEY,
            name TEXT, facilitys TEXT, cycle_time INTEGER,
            output_value INTEGER, input_typeid TEXT, input_value TEXT
        );

        CREATE TABLE IF NOT EXISTS loyalty_offers (
            corporation_id INTEGER NOT NULL,
            offer_id INTEGER NOT NULL,
            PRIMARY KEY (corporation_id, offer_id)
        );

        CREATE TABLE IF NOT EXISTS loyalty_offer_outputs (
            offer_id INTEGER PRIMARY KEY,
            type_id INTEGER NOT NULL, quantity INTEGER NOT NULL DEFAULT 1,
            isk_cost INTEGER NOT NULL DEFAULT 0, lp_cost INTEGER NOT NULL DEFAULT 0,
            ak_cost INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS loyalty_offer_requirements (
            offer_id INTEGER NOT NULL,
            required_type_id INTEGER NOT NULL,
            required_quantity INTEGER NOT NULL,
            PRIMARY KEY (offer_id, required_type_id)
        );

        CREATE TABLE IF NOT EXISTS blueprint_process_time (
            blueprintTypeID INTEGER NOT NULL PRIMARY KEY,
            blueprintTypeName TEXT, blueprintTypeIcon TEXT,
            manufacturing_time INTEGER, research_material_time INTEGER,
            research_time_time INTEGER, copying_time INTEGER,
            invention_time INTEGER, maxRunsPerCopy INTEGER
        );

        CREATE TABLE IF NOT EXISTS blueprint_manufacturing_materials (
            blueprintTypeID INTEGER NOT NULL, blueprintTypeName TEXT,
            blueprintTypeIcon TEXT, typeID INTEGER NOT NULL,
            typeName TEXT, typeIcon TEXT, quantity INTEGER,
            PRIMARY KEY (blueprintTypeID, typeID)
        );

        CREATE TABLE IF NOT EXISTS blueprint_manufacturing_output (
            blueprintTypeID INTEGER NOT NULL, blueprintTypeName TEXT,
            blueprintTypeIcon TEXT, typeID INTEGER NOT NULL,
            typeName TEXT, typeIcon TEXT, quantity INTEGER,
            PRIMARY KEY (blueprintTypeID, typeID)
        );

        CREATE TABLE IF NOT EXISTS blueprint_manufacturing_skills (
            blueprintTypeID INTEGER NOT NULL, blueprintTypeName TEXT,
            blueprintTypeIcon TEXT, typeID INTEGER NOT NULL,
            typeName TEXT, typeIcon TEXT, level INTEGER,
            PRIMARY KEY (blueprintTypeID, typeID)
        );

        CREATE TABLE IF NOT EXISTS blueprint_research_material_materials (
            blueprintTypeID INTEGER NOT NULL, blueprintTypeName TEXT,
            blueprintTypeIcon TEXT, typeID INTEGER NOT NULL,
            typeName TEXT, typeIcon TEXT, quantity INTEGER,
            PRIMARY KEY (blueprintTypeID, typeID)
        );

        CREATE TABLE IF NOT EXISTS blueprint_research_material_skills (
            blueprintTypeID INTEGER NOT NULL, blueprintTypeName TEXT,
            blueprintTypeIcon TEXT, typeID INTEGER NOT NULL,
            typeName TEXT, typeIcon TEXT, level INTEGER,
            PRIMARY KEY (blueprintTypeID, typeID)
        );

        CREATE TABLE IF NOT EXISTS blueprint_research_time_materials (
            blueprintTypeID INTEGER NOT NULL, blueprintTypeName TEXT,
            blueprintTypeIcon TEXT, typeID INTEGER NOT NULL,
            typeName TEXT, typeIcon TEXT, quantity INTEGER,
            PRIMARY KEY (blueprintTypeID, typeID)
        );

        CREATE TABLE IF NOT EXISTS blueprint_research_time_skills (
            blueprintTypeID INTEGER NOT NULL, blueprintTypeName TEXT,
            blueprintTypeIcon TEXT, typeID INTEGER NOT NULL,
            typeName TEXT, typeIcon TEXT, level INTEGER,
            PRIMARY KEY (blueprintTypeID, typeID)
        );

        CREATE TABLE IF NOT EXISTS blueprint_copying_materials (
            blueprintTypeID INTEGER NOT NULL, blueprintTypeName TEXT,
            blueprintTypeIcon TEXT, typeID INTEGER NOT NULL,
            typeName TEXT, typeIcon TEXT, quantity INTEGER,
            PRIMARY KEY (blueprintTypeID, typeID)
        );

        CREATE TABLE IF NOT EXISTS blueprint_copying_skills (
            blueprintTypeID INTEGER NOT NULL, blueprintTypeName TEXT,
            blueprintTypeIcon TEXT, typeID INTEGER NOT NULL,
            typeName TEXT, typeIcon TEXT, level INTEGER,
            PRIMARY KEY (blueprintTypeID, typeID)
        );

        CREATE TABLE IF NOT EXISTS blueprint_invention_materials (
            blueprintTypeID INTEGER NOT NULL, blueprintTypeName TEXT,
            blueprintTypeIcon TEXT, typeID INTEGER NOT NULL,
            typeName TEXT, typeIcon TEXT, quantity INTEGER,
            PRIMARY KEY (blueprintTypeID, typeID)
        );

        CREATE TABLE IF NOT EXISTS blueprint_invention_products (
            blueprintTypeID INTEGER NOT NULL, blueprintTypeName TEXT,
            blueprintTypeIcon TEXT, typeID INTEGER NOT NULL,
            typeName TEXT, typeIcon TEXT, quantity INTEGER, probability REAL,
            PRIMARY KEY (blueprintTypeID, typeID)
        );

        CREATE TABLE IF NOT EXISTS blueprint_invention_skills (
            blueprintTypeID INTEGER NOT NULL, blueprintTypeName TEXT,
            blueprintTypeIcon TEXT, typeID INTEGER NOT NULL,
            typeName TEXT, typeIcon TEXT, level INTEGER,
            PRIMARY KEY (blueprintTypeID, typeID)
        );

        CREATE TABLE IF NOT EXISTS traits (
            typeid INTEGER NOT NULL,
            content TEXT NOT NULL,
            content_ru TEXT,
            skill INTEGER NOT NULL DEFAULT -1,
            importance INTEGER, bonus_type TEXT,
            PRIMARY KEY (typeid, content, skill)
        );

        CREATE TABLE IF NOT EXISTS compressible_types (
            origin INTEGER NOT NULL, compressed INTEGER NOT NULL,
            PRIMARY KEY (origin)
        );

        CREATE TABLE IF NOT EXISTS dynamic_item_attributes (
            type_id INTEGER,
            attribute_id INTEGER,
            min_value REAL, max_value REAL,
            PRIMARY KEY (type_id, attribute_id)
        );

        CREATE TABLE IF NOT EXISTS dynamic_item_mappings (
            type_id INTEGER,
            applicable_type INTEGER,
            resulting_type INTEGER,
            PRIMARY KEY (type_id, applicable_type)
        );

        CREATE TABLE IF NOT EXISTS celestialNames (
            itemID INTEGER NOT NULL PRIMARY KEY,
            itemName TEXT
        );

        CREATE TABLE IF NOT EXISTS facility_rig_effects (
            id INTEGER NOT NULL,
            category INTEGER NOT NULL,
            group_id INTEGER NOT NULL,
            PRIMARY KEY (id, category, group_id)
        );

        CREATE TABLE IF NOT EXISTS marketGroups (
            group_id INTEGER NOT NULL PRIMARY KEY,
            name TEXT,
            icon_name TEXT,
            parentgroup_id INTEGER,
            show INTEGER DEFAULT 1,
            representative_type_id INTEGER
        );

        CREATE TABLE IF NOT EXISTS version_info (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            build_number INTEGER NOT NULL,
            patch_number INTEGER DEFAULT 0,
            release_date TEXT,
            build_key TEXT,
            description TEXT DEFAULT 'EVE SDE Database Version Information'
        );
    """)
    conn.commit()


def insert_categories(conn: sqlite3.Connection, sde_dir: str, icon_filenames: dict):
    path = fsd_path(sde_dir, "categoryIDs.yaml", "categories.yaml")
    if not os.path.exists(path):
        log("SKIP: fsd/categoryIDs.yaml not found")
        return
    log("Inserting categories...")
    data = load_yaml(path)
    rows = []
    for cat_id, entry in data.items():
        names = multiname(entry)
        icon_id = entry.get("iconID")
        icon_name = icon_filenames.get(int(icon_id)) if icon_id else None
        rows.append((
            int(cat_id),
            names.get("en"), names.get("de"), names.get("en"),
            names.get("es"), names.get("fr"), names.get("ja"),
            names.get("ko"), names.get("ru"), names.get("zh"),
            icon_name, entry.get("iconID"), bool(entry.get("published", False)),
        ))
    conn.executemany(
        "INSERT OR REPLACE INTO categories VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows
    )
    conn.commit()
    log(f"  {len(rows)} categories")


def insert_groups(conn: sqlite3.Connection, sde_dir: str, icon_filenames: dict):
    path = fsd_path(sde_dir, "groupIDs.yaml", "groups.yaml")
    if not os.path.exists(path):
        log("SKIP: fsd/groupIDs.yaml not found")
        return
    log("Inserting groups...")
    data = load_yaml(path)
    rows = []
    for grp_id, entry in data.items():
        names = multiname(entry)
        icon_id = entry.get("iconID")
        icon_name = icon_filenames.get(int(icon_id)) if icon_id else None
        rows.append((
            int(grp_id),
            names.get("en"), names.get("de"), names.get("en"),
            names.get("es"), names.get("fr"), names.get("ja"),
            names.get("ko"), names.get("ru"), names.get("zh"),
            entry.get("iconID"), entry.get("categoryID"),
            bool(entry.get("anchorable", False)),
            bool(entry.get("anchored", False)),
            bool(entry.get("fittableNonSingleton", False)),
            bool(entry.get("published", False)),
            bool(entry.get("useBasePrice", False)),
            icon_name,
            None,
        ))
    conn.executemany(
        "INSERT OR REPLACE INTO groups VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows
    )
    conn.commit()
    log(f"  {len(rows)} groups")


_IOS_CURATED_MARKET_ICONS: dict[int, int] = {
    2: 2703, 4: 1443, 5: 1443, 6: 1443, 7: 1443, 8: 1443, 9: 1432, 10: 365, 11: 1299, 14: 68,
    19: 2340, 20: 1192, 22: 29, 23: 2543, 24: 2563, 27: 2062, 52: 72, 54: 1277, 61: 20966, 64: 20968,
    65: 2199, 72: 20959, 73: 20968, 74: 20959, 75: 20966, 76: 20967, 77: 20967, 78: 20968, 79: 20959, 80: 20966,
    81: 20967, 82: 20968, 83: 20967, 84: 20966, 85: 20959, 86: 376, 87: 387, 88: 352, 99: 1004, 100: 1047,
    101: 1131, 102: 1142, 103: 1142, 105: 1142, 106: 1047, 107: 1047, 108: 1047, 109: 1004, 112: 1004, 113: 1004,
    114: 1334, 115: 1335, 116: 192, 117: 192, 118: 1352, 120: 1723, 126: 26451, 128: 86, 131: 10149, 132: 97,
    133: 79, 134: 80, 135: 92, 139: 1033, 140: 168, 141: 112, 143: 21440, 150: 33, 157: 1084, 158: 1084,
    159: 1084, 204: 2703, 205: 2703, 206: 2703, 207: 2703, 208: 2703, 209: 2703, 210: 2703, 211: 2703, 214: 2703,
    252: 2703, 261: 2703, 264: 2703, 272: 2703, 273: 2703, 274: 2703, 275: 2703, 276: 2703, 277: 2703, 278: 2703,
    279: 2703, 280: 2703, 281: 2703, 282: 2703, 283: 2703, 284: 2703, 285: 2703, 286: 2703, 287: 2703, 288: 2703,
    289: 2703, 290: 2703, 291: 2703, 292: 2703, 293: 2703, 295: 2703, 296: 2703, 297: 2703, 298: 2703, 299: 2703,
    300: 2703, 301: 2703, 302: 2703, 303: 2703, 305: 2703, 306: 2703, 307: 2703, 308: 2703, 309: 2703, 312: 2703,
    313: 2703, 314: 2703, 315: 2703, 316: 2703, 318: 2703, 320: 2703, 325: 2703, 331: 2703, 332: 2703, 335: 2703,
    338: 2703, 339: 2703, 340: 2703, 341: 2703, 343: 2703, 357: 2703, 358: 2703, 359: 2703, 364: 33, 365: 33,
    366: 33, 367: 33, 368: 33, 369: 33, 370: 33, 372: 33, 373: 33, 374: 33, 375: 33, 376: 33,
    377: 33, 378: 33, 379: 16, 380: 112, 381: 112, 382: 112, 383: 112, 387: 1349, 390: 2703, 391: 1443,
    393: 20959, 394: 20968, 395: 20967, 396: 20966, 399: 1443, 400: 20959, 401: 20966, 402: 20967, 403: 20968, 404: 16,
    405: 16, 406: 2703, 407: 2703, 408: 2703, 410: 2703, 411: 2703, 412: 2703, 413: 2703, 414: 2703, 415: 2703,
    416: 2703, 417: 2703, 418: 2703, 419: 2703, 420: 1443, 421: 20959, 422: 20966, 423: 20967, 424: 20968, 425: 2703,
    427: 2703, 428: 2703, 429: 2703, 430: 2703, 432: 1443, 433: 20959, 434: 20966, 435: 20967, 436: 20968, 437: 1443,
    438: 20959, 439: 20966, 440: 20967, 441: 20968, 442: 2703, 443: 2703, 444: 2703, 445: 2703, 446: 2703, 448: 1443,
    449: 20959, 450: 20966, 451: 20967, 452: 20968, 453: 2703, 454: 2703, 455: 2703, 456: 2703, 457: 2703, 458: 2703,
    459: 2703, 461: 2703, 462: 2703, 463: 2703, 464: 1443, 465: 20959, 466: 20966, 467: 20967, 468: 20968, 469: 1443,
    470: 20959, 471: 20966, 472: 20967, 473: 20968, 475: 1436, 477: 2222, 478: 2222, 479: 2222, 480: 2222, 481: 2222,
    482: 2222, 483: 2222, 484: 2222, 485: 2222, 488: 2222, 490: 2222, 491: 1194, 492: 1182, 494: 1443, 496: 2703,
    497: 2703, 499: 2679, 500: 2664, 501: 2668, 502: 1004, 503: 1142, 504: 1047, 505: 1346, 506: 2222, 512: 1277,
    514: 1273, 515: 231, 516: 230, 517: 1274, 518: 232, 519: 1356, 521: 1272, 522: 1275, 523: 1270, 525: 1377,
    526: 1271, 527: 1269, 528: 1282, 529: 1279, 530: 2102, 531: 2224, 532: 2062, 533: 1201, 535: 1030, 537: 80,
    538: 80, 540: 1030, 541: 2066, 542: 96, 550: 20939, 551: 1044, 552: 84, 553: 81, 554: 69, 555: 366,
    556: 365, 557: 361, 558: 360, 559: 381, 560: 381, 561: 376, 562: 371, 563: 365, 564: 349, 565: 370,
    566: 366, 567: 352, 568: 355, 569: 361, 570: 350, 572: 356, 573: 360, 574: 387, 575: 386, 576: 381,
    577: 389, 578: 384, 579: 379, 580: 184, 581: 186, 582: 2703, 583: 2703, 584: 2703, 585: 2703, 586: 2703,
    588: 2703, 589: 2703, 590: 2703, 591: 2703, 592: 2703, 593: 24968, 594: 2222, 595: 2222, 596: 2222, 597: 2703,
    598: 2703, 599: 2703, 600: 86, 601: 86, 602: 86, 603: 86, 604: 86, 605: 1044, 606: 1044, 608: 1044,
    609: 84, 610: 84, 611: 84, 612: 84, 613: 84, 614: 2302, 615: 77, 616: 2552, 617: 2703, 618: 2053,
    619: 2061, 620: 2054, 621: 2062, 622: 2060, 629: 1443, 630: 20959, 631: 20966, 632: 20967, 633: 20968, 634: 2703,
    635: 2703, 636: 2703, 637: 2703, 638: 2703, 639: 1345, 640: 168, 641: 1345, 642: 169, 643: 2530, 644: 170,
    645: 21440, 646: 26452, 647: 26453, 648: 26454, 655: 70, 656: 104, 657: 111, 658: 26546, 659: 26547, 660: 2105,
    661: 1283, 662: 1029, 663: 1035, 664: 89, 665: 26457, 666: 26456, 667: 26455, 668: 1031, 669: 26727, 670: 26721,
    671: 26726, 672: 26724, 673: 26725, 675: 2106, 676: 1405, 677: 109, 678: 109, 679: 105, 680: 1639, 681: 104,
    683: 1284, 685: 104, 686: 110, 687: 26449, 688: 26450, 689: 1283, 690: 1283, 691: 1283, 692: 1029, 693: 1029,
    694: 1029, 695: 1035, 696: 1035, 697: 1035, 698: 1031, 699: 1031, 700: 1031, 701: 1031, 702: 89, 703: 89,
    704: 89, 705: 89, 706: 3346, 707: 1640, 708: 3346, 711: 106, 712: 2677, 713: 107, 714: 2732, 715: 3227,
    716: 3228, 717: 3226, 718: 3229, 719: 109, 720: 104, 721: 104, 722: 104, 723: 104, 724: 104, 725: 104,
    726: 104, 727: 104, 728: 104, 729: 104, 730: 2552, 731: 2552, 732: 2552, 733: 2552, 734: 2552, 735: 2552,
    736: 2552, 737: 2552, 738: 2038, 739: 2325, 740: 2312, 741: 2317, 742: 2317, 743: 2312, 744: 2327, 745: 2327,
    746: 2332, 747: 2322, 748: 2322, 749: 2332, 750: 2319, 751: 2039, 752: 2302, 753: 2703, 754: 1204, 757: 2983,
    761: 1443, 762: 20959, 763: 20966, 764: 20967, 765: 20968, 766: 1443, 767: 20959, 768: 20966, 769: 20967, 770: 20968,
    771: 2836, 772: 2840, 773: 2837, 774: 2841, 775: 2842, 776: 2838, 777: 3955, 778: 84, 779: 20969, 781: 2863,
    782: 2703, 783: 2703, 784: 2703, 785: 2703, 786: 2703, 787: 2703, 788: 2703, 789: 2703, 790: 2703, 791: 2703,
    792: 2703, 793: 2703, 794: 2703, 796: 2703, 798: 2703, 799: 2703, 800: 2703, 801: 2851, 802: 20959, 803: 20966,
    812: 1443, 813: 20959, 814: 20966, 815: 20967, 816: 20968, 817: 1443, 818: 20959, 819: 20966, 820: 20967, 821: 20968,
    822: 1443, 823: 1443, 824: 1443, 825: 20959, 826: 20959, 827: 20959, 828: 20966, 829: 20966, 830: 20966, 831: 20967,
    832: 20967, 833: 20967, 834: 20968, 835: 20968, 836: 20968, 837: 1084, 838: 1084, 839: 1084, 840: 1084, 841: 1084,
    842: 1084, 843: 1084, 845: 1004, 846: 1287, 847: 1291, 848: 1047, 849: 1310, 850: 1311, 851: 1142, 852: 1145,
    853: 1140, 854: 1287, 855: 1287, 856: 1287, 857: 1291, 858: 1291, 859: 1291, 860: 1310, 861: 1310, 862: 1310,
    863: 1311, 864: 1311, 865: 1311, 866: 1145, 867: 1145, 868: 1145, 869: 1140, 870: 1140, 871: 1140, 872: 2986,
    874: 1443, 878: 2703, 879: 2703, 880: 2703, 881: 2703, 882: 2703, 883: 2703, 884: 2703, 885: 2703, 886: 2703,
    887: 2703, 888: 2703, 889: 2703, 890: 2703, 891: 2703, 892: 2703, 893: 2703, 894: 2703, 895: 2703, 896: 2703,
    897: 2703, 898: 2703, 899: 2703, 900: 2703, 901: 2703, 902: 2703, 903: 2703, 904: 2703, 905: 2703, 910: 1035,
    911: 1084, 912: 2934, 913: 2703, 914: 1335, 917: 192, 918: 184, 919: 186, 920: 192, 921: 184, 922: 1352,
    923: 1349, 924: 186, 925: 184, 926: 186, 927: 192, 928: 1352, 929: 1349, 930: 1352, 931: 1349, 932: 2222,
    933: 2222, 935: 1046, 937: 2703, 938: 2989, 939: 2703, 940: 1192, 943: 2703, 944: 2703, 945: 2703, 946: 2703,
    948: 2703, 949: 2703, 950: 2703, 951: 2703, 952: 2703, 953: 2703, 954: 2703, 955: 2887, 956: 3194, 957: 3196,
    958: 3200, 960: 3198, 961: 3195, 962: 3203, 963: 3202, 964: 3197, 965: 3193, 967: 1046, 968: 186, 971: 186,
    972: 186, 973: 186, 974: 3241, 975: 2703, 977: 3211, 979: 3201, 983: 3220, 986: 1288, 987: 1288, 988: 1288,
    989: 1288, 990: 1313, 991: 1313, 992: 1313, 993: 1313, 994: 1141, 995: 1141, 996: 1141, 997: 1141, 998: 192,
    999: 1352, 1000: 1349, 1001: 184, 1002: 186, 1003: 186, 1004: 1313, 1006: 1288, 1007: 1141, 1008: 2703, 1009: 2222,
    1010: 2222, 1011: 2222, 1012: 2222, 1013: 2222, 1014: 2677, 1015: 3281, 1016: 2703, 1018: 80, 1019: 2703, 1021: 3303,
    1022: 3303, 1027: 3303, 1028: 2703, 1029: 2703, 1030: 2703, 1031: 1277, 1032: 3219, 1033: 2697, 1034: 2567, 1035: 1436,
    1037: 3074, 1038: 2526, 1039: 1061, 1040: 2527, 1041: 2703, 1045: 2703, 1046: 2703, 1047: 1443, 1048: 27058, 1049: 80,
    1050: 80, 1051: 80, 1052: 80, 1053: 80, 1054: 80, 1055: 80, 1056: 80, 1057: 80, 1058: 80, 1059: 80,
    1060: 80, 1061: 80, 1062: 80, 1063: 80, 1065: 1443, 1066: 20959, 1067: 20966, 1068: 20967, 1069: 20968, 1070: 1443,
    1071: 20959, 1072: 20966, 1073: 20967, 1074: 20968, 1075: 1443, 1076: 20959, 1077: 20966, 1078: 20967, 1079: 20968, 1080: 1443,
    1081: 20959, 1082: 20966, 1083: 20967, 1084: 20968, 1085: 21489, 1086: 1041, 1087: 98, 1088: 97, 1089: 1443, 1090: 20959,
    1091: 20966, 1092: 20967, 1093: 20968, 1094: 3345, 1097: 2703, 1103: 3302, 1105: 2703, 1109: 3755, 1110: 33, 1111: 3203,
    1112: 3641, 1122: 3636, 1123: 3636, 1124: 3636, 1125: 3636, 1126: 3631, 1127: 3631, 1128: 3631, 1129: 3631, 1130: 3641,
    1131: 3641, 1132: 3641, 1133: 3641, 1134: 3646, 1135: 3646, 1136: 3646, 1137: 3646, 1138: 1443, 1139: 20959, 1140: 20966,
    1141: 20967, 1142: 20968, 1147: 3721, 1191: 2703, 1192: 1335, 1193: 1346, 1194: 1346, 1195: 76, 1196: 1042, 1197: 92,
    1198: 2703, 1199: 1722, 1200: 2663, 1201: 1721, 1202: 2703, 1203: 2703, 1204: 2703, 1206: 3194, 1207: 3194, 1208: 3194,
    1210: 3196, 1211: 3196, 1212: 3196, 1213: 3200, 1214: 3200, 1215: 3200, 1219: 3198, 1220: 3198, 1221: 3198, 1222: 3195,
    1223: 3195, 1224: 3195, 1225: 3203, 1226: 3203, 1227: 3203, 1228: 3202, 1229: 3202, 1230: 3202, 1231: 3197, 1232: 3197,
    1233: 3197, 1234: 3193, 1235: 3193, 1236: 3193, 1237: 3201, 1238: 3201, 1239: 3201, 1240: 2703, 1241: 2703, 1242: 2703,
    1243: 2703, 1244: 2703, 1245: 2703, 1249: 2703, 1250: 2703, 1251: 2703, 1252: 2703, 1253: 2703, 1254: 2703, 1255: 2703,
    1256: 2703, 1257: 2703, 1258: 2703, 1259: 2703, 1260: 2703, 1261: 2703, 1262: 2703, 1263: 2703, 1264: 2703, 1265: 2703,
    1266: 2703, 1267: 2703, 1268: 2703, 1269: 2703, 1272: 3952, 1273: 3952, 1274: 3952, 1275: 3952, 1276: 3947, 1282: 3946,
    1283: 3948, 1284: 3947, 1285: 2222, 1286: 2703, 1287: 183, 1310: 1084, 1313: 2703, 1316: 183, 1317: 183, 1320: 2881,
    1322: 2875, 1323: 33, 1332: 10073, 1333: 10012, 1334: 10023, 1335: 1362, 1336: 10040, 1337: 10074, 1338: 2703, 1339: 2703,
    1340: 2703, 1341: 2703, 1342: 2703, 1343: 2703, 1344: 2703, 1345: 2703, 1346: 2703, 1347: 2703, 1348: 2703, 1349: 2703,
    1350: 2703, 1351: 2703, 1352: 2703, 1353: 2703, 1354: 2703, 1355: 2703, 1356: 2703, 1358: 2703, 1359: 2703, 1361: 1443,
    1362: 1443, 1364: 1443, 1365: 1443, 1366: 1443, 1367: 1443, 1368: 1443, 1369: 1443, 1370: 1443, 1371: 1443, 1372: 1443,
    1373: 1443, 1374: 1443, 1375: 1443, 1376: 1443, 1377: 1443, 1378: 1443, 1379: 1443, 1380: 1443, 1381: 1443, 1382: 1443,
    1384: 1443, 1385: 1443, 1389: 2703, 1390: 27058, 1392: 1443, 1396: 10256, 1397: 10829, 1398: 10785, 1399: 10236, 1400: 10254,
    1401: 10756, 1402: 10830, 1403: 10570, 1404: 10624, 1405: 10234, 1406: 10684, 1407: 10828, 1408: 10224, 1410: 2875, 1416: 10933,
    1426: 10932, 1427: 10831, 1469: 2224, 1470: 2224, 1471: 2224, 1472: 2224, 1473: 2224, 1474: 2224, 1475: 2224, 1476: 2224,
    1477: 2224, 1478: 2224, 1479: 2224, 1480: 2224, 1481: 2224, 1482: 2224, 1483: 2224, 1484: 2224, 1485: 2224, 1486: 2224,
    1489: 2224, 1490: 2224, 1491: 2224, 1492: 2224, 1493: 2224, 1494: 2224, 1495: 2224, 1496: 2224, 1497: 2224, 1498: 2224,
    1499: 2224, 1500: 2224, 1501: 2224, 1502: 2224, 1504: 2224, 1505: 2224, 1506: 2224, 1507: 2224, 1508: 2224, 1509: 2224,
    1512: 2224, 1513: 2224, 1514: 2224, 1515: 2224, 1516: 2224, 1517: 2224, 1518: 2224, 1520: 2703, 1521: 2703, 1522: 2703,
    1525: 2703, 1526: 2703, 1527: 2703, 1528: 2703, 1529: 2703, 1530: 2703, 1531: 2703, 1532: 2703, 1533: 2703, 1534: 2703,
    1535: 2703, 1536: 2703, 1537: 2703, 1538: 2703, 1539: 2703, 1540: 2703, 1541: 2703, 1542: 2703, 1543: 2703, 1544: 2703,
    1545: 2703, 1546: 2703, 1547: 2703, 1548: 2703, 1549: 2703, 1550: 2703, 1551: 2703, 1552: 2703, 1553: 2703, 1554: 2703,
    1555: 2703, 1556: 2703, 1557: 2703, 1558: 2703, 1559: 2703, 1560: 2703, 1561: 2703, 1562: 2703, 1563: 2703, 1564: 2703,
    1565: 2703, 1566: 2703, 1567: 2703, 1568: 2703, 1570: 2703, 1571: 2703, 1572: 2703, 1574: 2703, 1575: 2703, 1576: 2703,
    1577: 2703, 1578: 2703, 1579: 2703, 1580: 2703, 1581: 2703, 1582: 2703, 1583: 2703, 1584: 2703, 1585: 2703, 1586: 2703,
    1587: 2703, 1588: 2703, 1589: 2703, 1590: 2703, 1591: 2703, 1592: 2703, 1593: 2703, 1594: 2703, 1595: 2703, 1598: 10942,
    1599: 10940, 1600: 10941, 1601: 2703, 1602: 2703, 1603: 2703, 1610: 20959, 1612: 1443, 1614: 1443, 1616: 27058, 1617: 2703,
    1618: 1443, 1619: 1443, 1620: 1443, 1621: 1443, 1623: 1443, 1624: 1443, 1625: 20966, 1626: 20968, 1627: 20967, 1631: 1443,
    1633: 20970, 1639: 1444, 1640: 2985, 1641: 1444, 1642: 34, 1643: 2703, 1646: 1084, 1650: 20971, 1651: 16, 1652: 16,
    1653: 16, 1657: 16, 1658: 16, 1659: 21065, 1660: 21065, 1661: 2231, 1662: 20977, 1663: 20973, 1665: 1394, 1666: 1393,
    1667: 1395, 1668: 1396, 1669: 1030, 1670: 1030, 1672: 79, 1673: 79, 1674: 79, 1675: 79, 1676: 79, 1678: 1394,
    1679: 1393, 1680: 1395, 1681: 1396, 1682: 1395, 1683: 1394, 1684: 1396, 1685: 1393, 1686: 2066, 1687: 2066, 1688: 1394,
    1689: 1393, 1690: 1395, 1691: 1396, 1692: 1394, 1693: 1393, 1694: 1395, 1695: 1396, 1696: 81, 1697: 2703, 1698: 1443,
    1699: 1443, 1700: 2040, 1701: 2703, 1702: 2222, 1703: 1443, 1704: 1443, 1707: 2703, 1708: 107, 1709: 21025, 1710: 2703,
    1711: 2703, 1712: 2703, 1713: 1061, 1715: 3240, 1716: 2703, 1717: 2677, 1718: 2856, 1719: 2703, 1720: 2703, 1721: 2703,
    1723: 2703, 1724: 2703, 1725: 2703, 1726: 2703, 1727: 2703, 1728: 2703, 1729: 2703, 1730: 3194, 1731: 3201, 1732: 3193,
    1733: 3197, 1734: 3202, 1735: 3203, 1736: 3195, 1737: 3198, 1739: 3200, 1740: 3196, 1745: 33, 1746: 33, 1747: 33,
    1748: 33, 1761: 2224, 1762: 2224, 1763: 2224, 1764: 2224, 1765: 2224, 1766: 2224, 1767: 2224, 1768: 2224, 1769: 2224,
    1770: 2224, 1771: 2224, 1772: 2224, 1773: 2224, 1774: 2224, 1775: 2224, 1776: 2224, 1777: 2224, 1779: 21057, 1780: 21058,
    1781: 21059, 1782: 21057, 1783: 21057, 1784: 21057, 1785: 21057, 1786: 21058, 1787: 21058, 1788: 21058, 1789: 21058, 1790: 21059,
    1791: 21059, 1792: 21059, 1793: 21059, 1794: 2703, 1795: 2703, 1796: 2703, 1797: 2703, 1798: 2703, 1799: 2703, 1800: 2703,
    1801: 2703, 1802: 2703, 1803: 2703, 1804: 2703, 1805: 2703, 1806: 2703, 1807: 2703, 1808: 2703, 1809: 2703, 1810: 21065,
    1811: 20974, 1812: 21065, 1813: 20974, 1814: 21047, 1815: 1443, 1816: 20959, 1817: 20966, 1818: 20967, 1819: 20968, 1822: 21048,
    1823: 33, 1824: 33, 1827: 21074, 1828: 2703, 1829: 2703, 1830: 2703, 1831: 16, 1832: 16, 1833: 16, 1834: 2703,
    1835: 16, 1836: 21078, 1837: 1443, 1838: 1443, 1840: 2039, 1841: 2703, 1842: 2703, 1843: 2703, 1844: 16, 1845: 16,
    1846: 24565, 1847: 16, 1849: 21783, 1850: 2665, 1851: 2665, 1852: 2665, 1853: 2665, 1854: 2665, 1855: 2554, 1856: 2215,
    1857: 404, 1858: 2664, 1859: 3222, 1860: 3751, 1861: 3256, 1862: 3722, 1863: 3260, 1864: 3303, 1865: 2875, 1866: 20959,
    1867: 20966, 1868: 20967, 1869: 20968, 1870: 10835, 1872: 2225, 1873: 2885, 1880: 3233, 1883: 2193, 1884: 20959, 1885: 20966,
    1886: 20967, 1887: 20968, 1888: 20967, 1889: 20968, 1897: 2890, 1898: 2887, 1899: 2890, 1900: 2888, 1901: 2890, 1902: 2889,
    1903: 2886, 1904: 2890, 1905: 2886, 1906: 3335, 1907: 2225, 1908: 2226, 1909: 3736, 1912: 2703, 1913: 2703, 1918: 2703,
    1919: 2703, 1920: 2703, 1921: 2222, 1922: 21481, 1923: 21481, 1924: 1443, 1931: 98, 1932: 1443, 1935: 111, 1936: 3433,
    1937: 2990, 1938: 2703, 1939: 2703, 1940: 2703, 1941: 98, 1942: 21335, 1943: 21204, 1944: 21275, 1945: 2703, 1949: 2703,
    1950: 27058, 1951: 1443, 1952: 20959, 1953: 20968, 1954: 21420, 1955: 21420, 1956: 20959, 1957: 20966, 1958: 20967, 1959: 20968,
    1960: 21420, 1961: 21420, 1962: 21420, 1963: 21420, 1964: 20959, 1965: 20966, 1966: 20967, 1967: 20968, 1968: 21420, 1969: 21420,
    1970: 21420, 1971: 21420, 1972: 21420, 1973: 21420, 1974: 20959, 1975: 20966, 1976: 20967, 1977: 20968, 1978: 20959, 1979: 20967,
    1980: 20959, 1981: 20966, 1982: 20967, 1983: 20968, 1984: 20959, 1985: 20966, 1986: 20967, 1987: 20968, 1988: 21420, 1989: 21420,
    1990: 20959, 1991: 20966, 1992: 20967, 1993: 20968, 1994: 20959, 1995: 20966, 1996: 20967, 1997: 20968, 1998: 21420, 1999: 21420,
    2000: 21420, 2001: 21420, 2002: 20959, 2003: 20966, 2004: 20967, 2005: 20968, 2006: 21420, 2007: 20959, 2008: 20966, 2009: 20967,
    2010: 20968, 2011: 21420, 2012: 21420, 2013: 2889, 2014: 2703, 2015: 2703, 2016: 2703, 2017: 2703, 2018: 21421, 2020: 2703,
    2021: 20966, 2022: 21420, 2023: 21420, 2024: 20959, 2025: 20966, 2026: 20967, 2027: 20968, 2028: 21420, 2029: 21420, 2030: 21420,
    2031: 21420, 2032: 21437, 2033: 21439, 2034: 20967, 2035: 21420, 2036: 21420, 2037: 21420, 2038: 20959, 2039: 20966, 2040: 20967,
    2041: 20968, 2042: 21420, 2043: 21420, 2044: 21420, 2045: 21420, 2046: 21420, 2047: 20959, 2048: 20966, 2049: 20967, 2050: 20968,
    2051: 20959, 2052: 20966, 2053: 20967, 2054: 20968, 2055: 20959, 2056: 20966, 2057: 20967, 2058: 20968, 2059: 20959, 2060: 20966,
    2061: 20967, 2062: 20968, 2063: 21420, 2064: 21420, 2065: 21420, 2066: 21420, 2067: 21420, 2068: 21420, 2069: 20959, 2070: 20966,
    2071: 20967, 2072: 20968, 2073: 20959, 2074: 20966, 2075: 20967, 2076: 20968, 2077: 20959, 2078: 20966, 2079: 20967, 2080: 20968,
    2081: 20959, 2082: 20966, 2083: 20967, 2084: 20968, 2085: 21420, 2086: 21420, 2087: 21420, 2088: 20959, 2089: 20966, 2090: 20967,
    2091: 20968, 2092: 20966, 2093: 20968, 2094: 21420, 2095: 20959, 2096: 20966, 2097: 20967, 2098: 20968, 2099: 21420, 2100: 21420,
    2101: 21420, 2102: 21420, 2103: 21420, 2104: 20959, 2105: 20966, 2106: 20967, 2107: 20968, 2108: 21420, 2109: 21420, 2110: 20959,
    2111: 20966, 2112: 20967, 2113: 20968, 2114: 21420, 2115: 1443, 2119: 21420, 2120: 21420, 2125: 1443, 2126: 20959, 2131: 20966,
    2132: 20967, 2133: 20968, 2134: 2703, 2135: 20971, 2136: 21420, 2137: 20959, 2138: 20966, 2139: 20967, 2140: 20968, 2141: 21420,
    2142: 20959, 2143: 20966, 2144: 20967, 2145: 20968, 2146: 1443, 2147: 20959, 2148: 20966, 2149: 20967, 2150: 20968, 2151: 1061,
    2152: 33, 2153: 2703, 2154: 21581, 2155: 2890, 2156: 2703, 2157: 2703, 2158: 2703, 2159: 2703, 2160: 2703, 2161: 2703,
    2162: 2703, 2163: 2703, 2164: 2703, 2165: 2703, 2166: 2703, 2167: 2703, 2168: 2703, 2169: 2703, 2170: 2703, 2171: 2703,
    2172: 2703, 2173: 2703, 2174: 2703, 2175: 2703, 2176: 2703, 2177: 2703, 2178: 2703, 2179: 2703, 2180: 2703, 2181: 2703,
    2182: 2703, 2183: 2703, 2184: 2703, 2185: 2703, 2186: 2703, 2187: 2703, 2188: 2703, 2189: 2703, 2190: 2703, 2191: 2703,
    2192: 2703, 2193: 2703, 2196: 21568, 2197: 21567, 2198: 21569, 2199: 2222, 2200: 2222, 2201: 2222, 2202: 21561, 2203: 2887,
    2204: 21602, 2205: 21599, 2206: 111, 2207: 104, 2208: 70, 2209: 365, 2210: 21596, 2211: 109, 2212: 105, 2213: 1284,
    2214: 2983, 2215: 1639, 2216: 111, 2218: 1405, 2219: 104, 2220: 26455, 2221: 21561, 2222: 26547, 2223: 1283, 2224: 89,
    2226: 168, 2227: 21440, 2228: 2934, 2229: 21564, 2230: 21565, 2231: 21566, 2232: 21596, 2233: 21597, 2234: 21440, 2235: 21439,
    2236: 1084, 2237: 2703, 2238: 2703, 2239: 1084, 2240: 79, 2241: 1044, 2242: 89, 2243: 1031, 2244: 80, 2245: 10933,
    2246: 81, 2247: 170, 2248: 2703, 2249: 21604, 2250: 1283, 2251: 1029, 2252: 1287, 2253: 1291, 2254: 1310, 2255: 1311,
    2256: 1140, 2257: 1145, 2258: 1346, 2259: 1346, 2260: 183, 2261: 183, 2262: 2703, 2263: 2703, 2264: 2703, 2265: 2703,
    2266: 2703, 2267: 2934, 2268: 2934, 2269: 2934, 2270: 2934, 2271: 1443, 2272: 20959, 2273: 20966, 2274: 20967, 2275: 20968,
    2276: 2934, 2277: 21420, 2278: 20959, 2279: 20966, 2280: 20967, 2281: 20968, 2283: 21420, 2285: 21420, 2286: 21420, 2287: 1443,
    2288: 1443, 2290: 2703, 2291: 2703, 2292: 2703, 2293: 2703, 2294: 2703, 2295: 2703, 2297: 20970, 2298: 20970, 2299: 20970,
    2300: 20970, 2301: 20970, 2302: 20970, 2306: 21420, 2307: 21420, 2308: 20959, 2309: 21420, 2310: 20959, 2311: 21420, 2312: 21420,
    2313: 21420, 2314: 21420, 2315: 21420, 2316: 21420, 2317: 2039, 2318: 1443, 2319: 21420, 2320: 1443, 2321: 1443, 2322: 2703,
    2323: 2703, 2324: 2222, 2325: 2934, 2327: 2222, 2328: 1443, 2330: 21420, 2331: 1443, 2332: 3007, 2333: 2703, 2334: 2703,
    2335: 1443, 2336: 27058, 2337: 21420, 2338: 1443, 2339: 2703, 2340: 21729, 2341: 21602, 2342: 21602, 2343: 21602, 2344: 21599,
    2345: 21599, 2346: 21599, 2347: 21729, 2348: 21729, 2349: 21729, 2350: 1443, 2351: 1345, 2353: 20966, 2354: 20967, 2355: 20968,
    2356: 20967, 2357: 1443, 2358: 21531, 2359: 21420, 2360: 21420, 2361: 21420, 2362: 21420, 2369: 21420, 2370: 20966, 2371: 20959,
    2372: 20967, 2373: 20968, 2374: 21420, 2375: 21420, 2376: 21420, 2377: 21420, 2378: 21420, 2380: 21420, 2381: 21420, 2382: 21420,
    2383: 21420, 2387: 20959, 2388: 20966, 2389: 20967, 2390: 20968, 2391: 20966, 2392: 20968, 2393: 2703, 2395: 1270, 2396: 232,
    2397: 1356, 2398: 231, 2400: 230, 2401: 1271, 2402: 21783, 2403: 21783, 2404: 21783, 2406: 21420, 2407: 2703, 2408: 21604,
    2409: 1084, 2410: 1084, 2411: 1084, 2412: 1084, 2413: 1084, 2414: 79, 2415: 2703, 2416: 1443, 2417: 1443, 2418: 21420,
    2419: 1443, 2420: 21420, 2421: 21420, 2425: 1443, 2426: 24135, 2427: 1443, 2428: 24135, 2429: 1443, 2430: 24135, 2431: 21923,
    2432: 21921, 2433: 21923, 2434: 21922, 2435: 21921, 2436: 21891, 2437: 21989, 2438: 21985, 2439: 21984, 2440: 21987, 2441: 21993,
    2442: 21992, 2456: 21924, 2457: 21905, 2458: 21906, 2459: 21907, 2460: 21904, 2461: 21903, 2462: 21918, 2463: 21918, 2464: 21920,
    2465: 21918, 2466: 21918, 2467: 21918, 2468: 21920, 2469: 21920, 2470: 21920, 2471: 26521, 2473: 2224, 2474: 2224, 2475: 2224,
    2476: 2224, 2477: 2224, 2478: 2224, 2479: 2103, 2480: 21982, 2481: 21420, 2482: 21420, 2483: 21420, 2484: 21420, 2485: 21420,
    2486: 21420, 2487: 10144, 2488: 3211, 2489: 3211, 2490: 3211, 2491: 3215, 2492: 3211, 2493: 3214, 2494: 3217, 2495: 21485,
    2496: 3212, 2497: 3213, 2498: 3216, 2499: 21487, 2500: 3210, 2501: 21483, 2502: 3211, 2503: 21841, 2504: 21840, 2505: 21839,
    2506: 3211, 2508: 2703, 2509: 97, 2510: 2703, 2511: 2222, 2512: 22064, 2518: 21420, 2519: 21420, 2520: 21420, 2521: 21420,
    2522: 1443, 2523: 24135, 2524: 1443, 2525: 24135, 2526: 24135, 2527: 80, 2529: 80, 2530: 3211, 2531: 3215, 2532: 24129,
    2535: 24135, 2536: 24135, 2537: 24135, 2538: 24143, 2539: 24144, 2540: 24136, 2658: 24135, 2690: 24135, 2691: 24237, 2692: 21918,
    2693: 24135, 2701: 24205, 2702: 1443, 2703: 21420, 2704: 21420, 2706: 21924, 2728: 24481, 2729: 24481, 2730: 24480, 2734: 24469,
    2735: 24475, 2736: 24481, 2737: 24468, 2738: 24474, 2739: 24480, 2740: 24467, 2741: 24466, 2742: 24464, 2743: 24465, 2744: 24466,
    2747: 21925, 2749: 21729, 2756: 24530, 2757: 24530, 2761: 10144, 2763: 16, 2764: 2703, 2765: 24608, 2766: 24623, 2767: 24619,
    2768: 24638, 2769: 21783, 2770: 2703, 2771: 16, 2772: 2703, 2783: 24730, 2790: 3215, 2791: 3215, 2792: 3215, 2795: 3074,
    2797: 2703, 2798: 16, 2799: 21785, 2801: 10065, 2804: 24968, 2805: 25021, 2806: 2703, 2807: 2703, 2814: 25169, 2815: 25152,
    2816: 2703, 2819: 1443, 2820: 1004, 2821: 2703, 3450: 16, 3451: 2703, 3453: 1436, 3454: 1436, 3455: 1436, 3456: 1436,
    3478: 24411, 3480: 1443, 3481: 1443, 3483: 1443, 3484: 1443, 3487: 15, 3488: 15, 3489: 15, 3490: 15, 3495: 21420,
    3496: 21420, 3497: 21420, 3508: 1443, 3509: 1443, 3510: 1443, 3511: 1443, 3512: 20959, 3513: 20966, 3514: 20968, 3515: 20967,
    3519: 21420, 3520: 21420, 3521: 20967, 3523: 20968, 3531: 1443, 3534: 1443, 3535: 20959, 3536: 24419, 3537: 24419, 3538: 24419,
    3539: 21420, 3540: 21420, 3541: 21420, 3546: 20966, 3548: 21420, 3549: 21420, 3567: 21420, 3568: 21420, 3577: 1443, 3578: 1443,
    3579: 2703, 3580: 1443, 3591: 2703, 3593: 2703, 3594: 1443, 3596: 24296, 3624: 2222, 3625: 2222, 3626: 2703, 3627: 2703,
    3628: 26056, 3629: 26055, 3630: 26056, 3631: 26053, 3632: 26052, 3633: 26054, 3636: 1377, 3637: 1269, 3638: 1277, 3639: 1274,
    3640: 1271, 3641: 2888, 3642: 2888, 3643: 2888, 3651: 2703, 3653: 26053, 3654: 26053, 3655: 26053, 3656: 33, 3667: 24129,
    3672: 1443, 3673: 1443, 3680: 16, 3681: 2703, 3696: 24296, 3697: 26356, 3719: 24905, 3720: 24905, 3721: 24905, 3722: 24905,
    3723: 24905, 3724: 24905, 3725: 26372, 3726: 26396, 3732: 2038, 3734: 2325, 3736: 3948, 3737: 3755, 3738: 2703, 3739: 3953,
    3740: 2703, 3741: 26779, 3742: 2703, 3743: 26785, 3744: 24135, 3745: 26799, 3746: 1443, 3754: 2703, 3755: 27058, 3756: 20970,
    3757: 2703, 3759: 1443, 3762: 1546, 3764: 27058, 3765: 20959, 3766: 20966, 3767: 20967, 3768: 20968, 3769: 27058, 3770: 20959,
    3771: 20966, 3772: 20967, 3773: 20968, 3774: 27058, 3776: 27139, 3779: 2703, 3781: 16, 3782: 21783, 3784: 1277, 3786: 27212,
    3790: 27212, 3792: 27215, 3794: 21420, 3795: 27208, 3797: 27205, 3798: 27201, 3799: 27198, 3800: 24905, 3801: 24968, 3802: 24973,
    3803: 24971, 3804: 24972, 3805: 24969, 3806: 24970, 3807: 27218, 3808: 25027, 3809: 25021, 3810: 25033, 3811: 25039, 3812: 25045,
    3813: 27058, 3818: 27058, 3831: 26518, 3832: 22082, 3833: 1660, 3834: 21898, 3835: 21060, 3836: 21900, 3837: 21902, 3838: 21725,
    3839: 21901, 3840: 24281, 3841: 21899,
}


def _apply_ios_curated_icons(conn: sqlite3.Connection):
    """Override representative_type_id with curated icons from the iOS EVE Nexus app."""
    for gid, tid in _IOS_CURATED_MARKET_ICONS.items():
        conn.execute(
            "UPDATE marketGroups SET representative_type_id = ? WHERE group_id = ?",
            (tid, gid),
        )
    conn.commit()
    log(f"  Applied {len(_IOS_CURATED_MARKET_ICONS)} curated market group icons")


def populate_representative_types(conn: sqlite3.Connection):
    log("Populating representative_type_id for groups and marketGroups...")
    conn.execute("""
        UPDATE groups SET representative_type_id = (
            SELECT MIN(type_id) FROM types
            WHERE types.groupID = groups.group_id AND types.published = 1
        )
    """)
    conn.execute("""
        UPDATE marketGroups SET representative_type_id = (
            SELECT MIN(t.type_id) FROM types t
            WHERE t.marketGroupID = marketGroups.group_id AND t.published = 1
        )
    """)
    conn.commit()
    for _ in range(10):
        updated = conn.execute("""
            UPDATE marketGroups SET representative_type_id = (
                SELECT MIN(child.representative_type_id)
                FROM marketGroups child
                WHERE child.parentgroup_id = marketGroups.group_id
                  AND child.representative_type_id IS NOT NULL
            )
            WHERE representative_type_id IS NULL
        """).rowcount
        conn.commit()
        if updated == 0:
            break
    _apply_ios_curated_icons(conn)
    log("  Done.")


def insert_meta_groups(conn: sqlite3.Connection, sde_dir: str, fsd_strings: dict):
    path = fsd_path(sde_dir, "metaGroups.yaml")
    if not os.path.exists(path):
        return
    log("Inserting metaGroups...")
    data = load_yaml(path)
    rows = []
    for mg_id, entry in data.items():
        names = multiname(entry)
        name = names.get("en") or names.get("de") or ""
        if not name:
            name = resolve_name_id(entry.get("nameID"), fsd_strings)
        rows.append((int(mg_id), name))
    conn.executemany("INSERT OR REPLACE INTO metaGroups VALUES (?,?)", rows)
    conn.commit()
    log(f"  {len(rows)} metaGroups")


def insert_market_groups(conn: sqlite3.Connection, sde_dir: str, fsd_strings: dict, icon_filenames: dict):
    fsd_mg_path = fsd_path(sde_dir, "marketGroups.yaml")
    bsd_mg_path = os.path.join(sde_dir, "bsd", "invMarketGroups.yaml")

    if not os.path.exists(fsd_mg_path):
        log("SKIP: fsd/marketGroups.yaml not found")
        return
    log("Inserting marketGroups...")

    bsd_names: dict[int, str] = {}
    if os.path.exists(bsd_mg_path):
        for entry in load_yaml(bsd_mg_path):
            gid = entry.get("marketGroupID")
            n = entry.get("marketGroupName") or entry.get("nameID") or ""
            if gid and n:
                bsd_names[int(gid)] = str(n)
        log(f"  bsd/invMarketGroups.yaml: {len(bsd_names)} name entries")

    data = load_yaml(fsd_mg_path)
    rows = []
    for grp_id, entry in data.items():
        gid = int(grp_id)
        names = multiname(entry)
        name = names.get("en") or names.get("de") or names.get("zh") or ""
        if not name:
            name = resolve_name_id(entry.get("nameID"), fsd_strings)
        if not name:
            name = bsd_names.get(gid, "")
        icon_id = entry.get("iconID")
        icon_name = icon_filenames.get(int(icon_id)) if icon_id else None
        parent_id = entry.get("parentGroupID")
        rows.append((gid, name, icon_name, parent_id, 1, None))
    conn.executemany(
        "INSERT OR REPLACE INTO marketGroups VALUES (?,?,?,?,?,?)",
        rows
    )
    conn.commit()
    log(f"  {len(rows)} marketGroups")


def insert_types(conn: sqlite3.Connection, sde_dir: str, icon_filenames: dict = None):
    path = fsd_path(sde_dir, "typeIDs.yaml", "types.yaml")
    if not os.path.exists(path):
        log("SKIP: fsd/typeIDs.yaml not found")
        return
    log(f"Inserting types from {os.path.basename(path)} (loading YAML — may take 30–60s)...")
    data = load_yaml(path)

    log(f"  Loaded {len(data)} types, building group/category lookup...")
    group_data = {}
    grp_path = fsd_path(sde_dir, "groupIDs.yaml", "groups.yaml")
    if os.path.exists(grp_path):
        raw_groups = load_yaml(grp_path)
        cat_path = fsd_path(sde_dir, "categoryIDs.yaml", "categories.yaml")
        raw_cats = load_yaml(cat_path) if os.path.exists(cat_path) else {}
        for grp_id, grp in raw_groups.items():
            cat_id = grp.get("categoryID")
            cat_entry = raw_cats.get(cat_id, {})
            cat_names = multiname(cat_entry)
            grp_names = multiname(grp)
            group_data[int(grp_id)] = {
                "group_name": grp_names.get("en", ""),
                "category_id": cat_id,
                "category_name": cat_names.get("en", ""),
            }

    rows = []
    for type_id_raw, entry in data.items():
        type_id = int(type_id_raw)
        names = multiname(entry)
        desc = entry.get("description")
        if isinstance(desc, dict):
            desc_en = desc.get("en")
        else:
            desc_en = desc

        grp_id = entry.get("groupID")
        gd = group_data.get(grp_id, {})

        icon_id = entry.get("iconID") or 0
        icon_fn = (icon_filenames or {}).get(int(icon_id)) if icon_id else None

        rows.append((
            type_id,
            names.get("en"), names.get("de"), names.get("en"),
            names.get("es"), names.get("fr"), names.get("ja"),
            names.get("ko"), names.get("ru"), names.get("zh"),
            desc_en,
            None,
            icon_fn, None,
            bool(entry.get("published", False)),
            entry.get("volume"),
            entry.get("packagedVolume"),
            entry.get("capacity"),
            entry.get("mass"),
            entry.get("marketGroupID"),
            entry.get("metaGroupID"),
            icon_id,
            grp_id,
            gd.get("group_name"),
            gd.get("category_id"),
            gd.get("category_name"),
            None, None, None,
            None, None, None, None,
            None, None, None,
            None, None, None,
            entry.get("variationParentTypeID"),
            None,
            None, None, None, None,
        ))

        if len(rows) >= 2000:
            conn.executemany(
                "INSERT OR REPLACE INTO types VALUES (" + ",".join(["?"] * 45) + ")",
                rows
            )
            rows.clear()

    if rows:
        conn.executemany(
            "INSERT OR REPLACE INTO types VALUES (" + ",".join(["?"] * 45) + ")",
            rows
        )
    conn.commit()
    log(f"  {len(data)} types inserted")

    insert_traits(conn, data, sde_dir)


def _format_bonus_prefix(bonus_val, unit_id):
    """Build HTML prefix like '<b>5%</b> ' for a trait bonus."""
    if isinstance(bonus_val, (int, float)) and unit_id:
        num = int(bonus_val) if isinstance(bonus_val, float) and bonus_val == int(bonus_val) else bonus_val
        if unit_id == 105:
            return f"<b>{num}%</b> "
        elif unit_id == 104:
            return f"<b>{num}x</b> "
        elif unit_id == 139:
            return f"<b>{num}+</b> "
        else:
            return f"<b>{num}</b> "
    return ""


def insert_traits(conn: sqlite3.Connection, type_data: dict, sde_dir: str = ""):
    """Extract traits from types data or typeBonus.yaml and insert into traits table."""
    log("Inserting traits...")

    # The S3 SDE embeds traits inside each type entry in types.yaml.
    # The developers.eveonline.com SDE puts them in a separate typeBonus.yaml.
    has_embedded = any(entry.get("traits") for entry in type_data.values())
    if has_embedded:
        trait_source = {tid: entry.get("traits") for tid, entry in type_data.items() if entry.get("traits")}
    else:
        bonus_path = fsd_path(sde_dir, "typeBonus.yaml", "typeBonus.yaml") if sde_dir else ""
        if sde_dir and os.path.exists(bonus_path):
            log(f"  Loading traits from {os.path.basename(bonus_path)}...")
            trait_source = load_yaml(bonus_path)
        else:
            log("  No traits data found (neither embedded nor typeBonus.yaml)")
            trait_source = {}

    rows = []
    for type_id_raw, traits in trait_source.items():
        type_id = int(type_id_raw)
        if not traits:
            continue

        def _extract_bonus(bonus, skill_id, bonus_type):
            bt = bonus.get("bonusText")
            if isinstance(bt, dict):
                content_en = bt.get("en", "")
                content_ru = bt.get("ru")
            elif isinstance(bt, str):
                content_en = bt
                content_ru = None
            else:
                return
            if not content_en:
                return
            prefix = _format_bonus_prefix(bonus.get("bonus"), bonus.get("unitID"))
            rows.append((type_id, prefix + content_en, prefix + content_ru if content_ru else None,
                         skill_id, bonus.get("importance", 999999), bonus_type))

        for bonus in traits.get("roleBonuses", []):
            _extract_bonus(bonus, -1, "roleBonuses")

        types_dict = traits.get("types", {})
        if isinstance(types_dict, dict):
            for skill_id_raw, bonuses in types_dict.items():
                skill_id = int(skill_id_raw)
                if not isinstance(bonuses, list):
                    continue
                for bonus in bonuses:
                    _extract_bonus(bonus, skill_id, "typeBonuses")

        for bonus in traits.get("miscBonuses", []):
            _extract_bonus(bonus, -1, "miscBonuses")

    conn.executemany(
        "INSERT OR REPLACE INTO traits (typeid, content, content_ru, skill, importance, bonus_type) VALUES (?,?,?,?,?,?)",
        rows
    )
    conn.commit()
    log(f"  {len(rows)} traits")


DOGMA_ATTRIBUTE_CATEGORIES = [
    (1, "Fitting", "Fitting capabilities of a ship"),
    (2, "Shield", "Shield attributes of ships"),
    (3, "Armor", "Armor attributes of ships"),
    (4, "Structure", "Structure attributes of ships"),
    (5, "Capacitor", "Capacitor attributes for ships"),
    (6, "Targeting", "Targeting Attributes for ships"),
    (7, "Miscellaneous", "Misc. attributes"),
    (8, "Required Skills", "Skill requirements"),
    (9, "NULL", "Attributes already checked and not going into a category"),
    (10, "Drones", "All you need to know about drones"),
    (12, "AI", "Attribs for the AI configuration"),
    (17, "Speed and Travel", "Attributes used for velocity, speed and such"),
    (19, "Loot", "Attributes that affect loot drops"),
    (20, "Remote Assistance", "Remote shield transfers, armor, structure and such"),
    (21, "EW - Target Painting", "NPC Target Painting Attributes"),
    (22, "EW - Energy Neutralizing", "NPC Energy Neutralizing Attributes"),
    (23, "EW - Remote Electronic Counter Measures", "NPC Remote Electronic Counter Measures Attributes"),
    (24, "EW - Sensor Dampening", "NPC Sensor Dampening Attributes"),
    (25, "EW - Target Jamming", "NPC Target Jamming Attributes"),
    (26, "EW - Tracking Disruption", "NPC Tracking Disruption Attributes"),
    (27, "EW - Warp Scrambling", "NPC Warp Scrambling Attributes"),
    (28, "EW - Webbing", "NPC Stasis Webbing Attributes"),
    (29, "Turrets", "NPC Turrets Attributes"),
    (30, "Missile", "NPC Missile Attributes"),
    (31, "Graphics", "NPC Graphic Attributes"),
    (32, "Entity Rewards", "NPC Entity Rewards Attributes"),
    (33, "Entity Extra Attributes", "NPC Extra Attributes"),
    (34, "Fighter Abilities", "Fighter abilities are like built-in modules on fighters"),
    (36, "EW - Resistance", "Resistances to different types of EWar Effects"),
    (37, "Bonuses", "Bonuses"),
    (38, "Fighter Attributes", "Attributes related to fighters (but not abilities)"),
    (39, "Superweapons", "Attributes relating to Doomsdays and Superweapons"),
    (40, "Hangars & Bays", "Hangars & Bays"),
    (41, "On Death", "Attributes relating to the death of a ship"),
    (42, "Behavior Attributes", "NPC Behavior Attributes"),
    (51, "Mining", "Mining related attributes"),
    (52, "Heat", ""),
]


def insert_dogma_attribute_categories(conn: sqlite3.Connection):
    log("Inserting dogmaAttributeCategories...")
    conn.executemany(
        "INSERT OR REPLACE INTO dogmaAttributeCategories VALUES (?,?,?)",
        DOGMA_ATTRIBUTE_CATEGORIES
    )
    conn.commit()
    log(f"  {len(DOGMA_ATTRIBUTE_CATEGORIES)} dogmaAttributeCategories")


def insert_dogma_attributes(conn: sqlite3.Connection, sde_dir: str, icon_filenames: dict):
    path = fsd_path(sde_dir, "dogmaAttributes.yaml")
    if not os.path.exists(path):
        return
    log("Inserting dogmaAttributes...")
    data = load_yaml(path)
    rows = []
    for attr_id, entry in data.items():
        display = entry.get("displayName") or entry.get("displayNameID")
        if isinstance(display, dict):
            display_name = display.get("en")
        elif isinstance(display, str):
            display_name = display
        else:
            display_name = None

        tooltip = entry.get("tooltipDescription") or entry.get("tooltipDescriptionID")
        if isinstance(tooltip, dict):
            tooltip_str = tooltip.get("en")
        else:
            tooltip_str = None

        icon_id = entry.get("iconID")
        icon_fn = icon_filenames.get(int(icon_id)) if icon_id else None

        category_id = entry.get("attributeCategoryID") or entry.get("categoryID")

        display_when_zero = entry.get("displayWhenZero", True)

        rows.append((
            int(attr_id),
            category_id, entry.get("name"), display_name,
            tooltip_str, icon_id, icon_fn,
            entry.get("unitID"),
            bool(entry.get("stackable", True)),
            bool(entry.get("highIsGood", True)),
            entry.get("defaultValue"), bool(entry.get("published", False)),
            bool(display_when_zero),
        ))
    conn.executemany("INSERT OR REPLACE INTO dogmaAttributes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    log(f"  {len(rows)} dogmaAttributes")


def insert_dogma_effects(conn: sqlite3.Connection, sde_dir: str):
    path = fsd_path(sde_dir, "dogmaEffects.yaml")
    if not os.path.exists(path):
        return
    log("Inserting dogmaEffects...")
    data = load_yaml(path)
    rows = []
    for eff_id, entry in data.items():
        display = entry.get("displayNameID")
        display_name = display.get("en") if isinstance(display, dict) else display

        rows.append((
            int(eff_id),
            entry.get("effectCategory"), entry.get("effectName"), display_name,
            None, bool(entry.get("published", False)),
            bool(entry.get("isAssistance", False)),
            bool(entry.get("isOffensive", False)),
            entry.get("resistanceAttributeID"), None,
        ))
    conn.executemany("INSERT OR REPLACE INTO dogmaEffects VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    log(f"  {len(rows)} dogmaEffects")


def insert_types_dogma(conn: sqlite3.Connection, sde_dir: str):
    path = fsd_path(sde_dir, "typesDogma.yaml", "typeDogma.yaml")
    if not os.path.exists(path):
        return
    log("Inserting typeAttributes and typeSkillRequirements (loading YAML)...")
    data = load_yaml(path)

    type_info = {}
    cur = conn.cursor()
    cur.execute("SELECT type_id, en_name, icon_filename, published, categoryID, category_name FROM types")
    for row in cur.fetchall():
        type_info[row[0]] = row

    attr_rows = []
    effect_rows = []
    skill_req_rows = []

    for type_id_raw, entry in data.items():
        type_id = int(type_id_raw)
        attrs = {a["attributeID"]: a["value"] for a in entry.get("dogmaAttributes", [])}
        effects = entry.get("dogmaEffects", [])

        for attr_id, value in attrs.items():
            attr_rows.append((type_id, attr_id, value))
            if len(attr_rows) >= 5000:
                conn.executemany("INSERT OR REPLACE INTO typeAttributes VALUES (?,?,?)", attr_rows)
                attr_rows.clear()

        for eff in effects:
            effect_rows.append((type_id, eff["effectID"], bool(eff.get("isDefault", False))))
            if len(effect_rows) >= 5000:
                conn.executemany("INSERT OR REPLACE INTO typeEffects VALUES (?,?,?)", effect_rows)
                effect_rows.clear()

        ti = type_info.get(type_id, (type_id, None, None, False, None, None))
        type_name = ti[1]
        type_icon = ti[2]
        published = ti[3]
        cat_id = ti[4]
        cat_name = ti[5]

        for skill_attr, level_attr in SKILL_REQ_ATTR_PAIRS:
            skill_id = attrs.get(skill_attr)
            skill_level = attrs.get(level_attr)
            if skill_id is not None and skill_level is not None:
                skill_req_rows.append((
                    type_id, type_name, type_icon, published, cat_id, cat_name,
                    int(skill_id), int(skill_level),
                ))

    if attr_rows:
        conn.executemany("INSERT OR REPLACE INTO typeAttributes VALUES (?,?,?)", attr_rows)
    if effect_rows:
        conn.executemany("INSERT OR REPLACE INTO typeEffects VALUES (?,?,?)", effect_rows)
    if skill_req_rows:
        conn.executemany("INSERT OR REPLACE INTO typeSkillRequirement VALUES (?,?,?,?,?,?,?,?)", skill_req_rows)
    conn.commit()
    log(f"  {len(data)} types processed for dogma")
    log(f"  {len(skill_req_rows)} skill requirements")


def _parse_system(args):
    sys_yaml_path, sys_name, region_id, const_id, is_jspace = args
    try:
        sys_data = load_yaml(sys_yaml_path)
    except Exception:
        return None
    sys_id = sys_data.get("solarSystemID")
    if sys_id is None:
        return None

    security = sys_data.get("security", 0.0)
    center = sys_data.get("center", [0, 0, 0]) or [0, 0, 0]
    x = center[0] if len(center) > 0 else 0
    y = center[1] if len(center) > 1 else 0
    z = center[2] if len(center) > 2 else 0
    system_type = sys_data.get("sunTypeID", 0)
    has_gate = 1 if sys_data.get("stargates") else 0

    planets = sys_data.get("planets", {}) or {}
    planet_counts = {col: 0 for col in ["temperate", "barren", "oceanic", "ice", "gas", "lava", "storm", "plasma"]}
    for _, planet_data in planets.items():
        col = PLANET_TYPE_TO_COLUMN.get(planet_data.get("typeID"))
        if col:
            planet_counts[col] += 1

    sys_row = (int(sys_id), sys_name, None, sys_name, None, None, None, None, None, None, security)
    univ_row = (
        int(region_id), int(const_id), int(sys_id),
        security, system_type, x, y, z, 0, has_gate,
        1 if is_jspace else 0, 0,
        planet_counts["temperate"], planet_counts["barren"],
        planet_counts["oceanic"], planet_counts["ice"],
        planet_counts["gas"], planet_counts["lava"],
        planet_counts["storm"], planet_counts["plasma"],
    )
    return sys_row, univ_row


def insert_universe(conn: sqlite3.Connection, sde_dir: str):
    universe_root = os.path.join(sde_dir, "universe")
    if not os.path.exists(universe_root):
        log("SKIP: universe/ folder not found")
        return
    log("Inserting universe data (parallel read)...")

    region_rows = []
    const_rows = []
    system_tasks = []

    for space_type in ["eve", "wormhole", "abyssal", "void"]:
        space_dir = os.path.join(universe_root, space_type)
        if not os.path.isdir(space_dir):
            continue
        is_jspace = space_type == "wormhole"

        for region_name in os.listdir(space_dir):
            region_dir = os.path.join(space_dir, region_name)
            region_yaml_path = os.path.join(region_dir, "region.yaml")
            if not os.path.exists(region_yaml_path):
                continue
            region_data = load_yaml(region_yaml_path)
            region_id = region_data.get("regionID")
            if region_id is None:
                continue
            region_rows.append((int(region_id), region_name, None, region_name, None, None, None, None, None, None))

            for const_name in os.listdir(region_dir):
                const_dir = os.path.join(region_dir, const_name)
                const_yaml_path = os.path.join(const_dir, "constellation.yaml")
                if not os.path.exists(const_yaml_path):
                    continue
                const_data = load_yaml(const_yaml_path)
                const_id = const_data.get("constellationID")
                if const_id is None:
                    continue
                const_rows.append((int(const_id), const_name, None, const_name, None, None, None, None, None, None))

                for sys_name in os.listdir(const_dir):
                    sys_yaml_path = os.path.join(const_dir, sys_name, "solarsystem.yaml")
                    if os.path.exists(sys_yaml_path):
                        system_tasks.append((sys_yaml_path, sys_name, region_id, const_id, is_jspace))

    log(f"  {len(region_rows)} regions, {len(const_rows)} constellations, {len(system_tasks)} systems to parse...")

    sys_rows = []
    univ_rows = []
    with ThreadPoolExecutor(max_workers=16) as ex:
        for result in ex.map(_parse_system, system_tasks):
            if result:
                sys_rows.append(result[0])
                univ_rows.append(result[1])

    conn.executemany("INSERT OR REPLACE INTO regions VALUES (?,?,?,?,?,?,?,?,?,?)", region_rows)
    conn.executemany("INSERT OR REPLACE INTO constellations VALUES (?,?,?,?,?,?,?,?,?,?)", const_rows)
    conn.executemany("INSERT OR REPLACE INTO solarsystems VALUES (?,?,?,?,?,?,?,?,?,?,?)", sys_rows)
    conn.executemany("INSERT OR REPLACE INTO universe VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", univ_rows)
    conn.commit()
    log(f"  {len(sys_rows)} solar systems inserted")


def insert_stations(conn: sqlite3.Connection, sde_dir: str):
    bsd_path = os.path.join(sde_dir, "bsd", "staStations.yaml")
    npc_path = fsd_path(sde_dir, "npcStations.yaml")
    rows = []
    if os.path.exists(bsd_path):
        log("Inserting stations from bsd/staStations.yaml...")
        data = load_yaml(bsd_path)
        for entry in data:
            rows.append((
                entry.get("stationID"),
                entry.get("stationTypeID"),
                entry.get("stationName"),
                entry.get("regionID"),
                entry.get("solarSystemID"),
                entry.get("security"),
            ))
    elif os.path.exists(npc_path):
        log("Inserting stations from npcStations.yaml...")
        data = load_yaml(npc_path)
        type_names = {}
        cur = conn.cursor()
        cur.execute("SELECT type_id, en_name FROM types")
        for r in cur.fetchall():
            type_names[r[0]] = r[1]
        for station_id, entry in data.items():
            type_id = entry.get("typeID")
            rows.append((
                int(station_id),
                type_id,
                type_names.get(type_id),
                None,
                entry.get("solarSystemID"),
                None,
            ))
    else:
        log("SKIP: no stations file found")
        return
    conn.executemany("INSERT OR REPLACE INTO stations VALUES (?,?,?,?,?,?)", rows)
    conn.commit()
    log(f"  {len(rows)} stations")

    # Update hasStation in universe
    conn.execute("""
        UPDATE universe SET hasStation = 1
        WHERE solarsystem_id IN (SELECT DISTINCT solarSystemID FROM stations)
    """)
    conn.commit()


def insert_factions(conn: sqlite3.Connection, sde_dir: str):
    path = fsd_path(sde_dir, "factions.yaml")
    if not os.path.exists(path):
        return
    log("Inserting factions...")
    data = load_yaml(path)
    rows = []
    for fact_id, entry in data.items():
        names = multiname(entry)
        rows.append((
            int(fact_id),
            names.get("en"), names.get("de"), names.get("en"),
            names.get("es"), names.get("fr"), names.get("ja"),
            names.get("ko"), names.get("ru"), names.get("zh"),
            None, None, entry.get("iconID"),
        ))
    conn.executemany("INSERT OR REPLACE INTO factions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    log(f"  {len(rows)} factions")


def insert_npc_corporations(conn: sqlite3.Connection, sde_dir: str):
    path = fsd_path(sde_dir, "npcCorporations.yaml")
    if not os.path.exists(path):
        return
    log("Inserting npcCorporations...")
    data = load_yaml(path)
    rows = []
    corp_lp_offers = []
    lp_outputs = []
    lp_requirements = []

    for corp_id, entry in data.items():
        names = multiname(entry)
        rows.append((
            int(corp_id),
            names.get("en"), names.get("de"), names.get("en"),
            names.get("es"), names.get("fr"), names.get("ja"),
            names.get("ko"), names.get("ru"), names.get("zh"),
            None, entry.get("factionID"), entry.get("militiaFactionID"),
            None,
        ))

        lp_raw = entry.get("loyaltyStoreOffers", []) or []
        for offer in lp_raw:
            offer_id = offer.get("offerID")
            if offer_id is None:
                continue
            corp_lp_offers.append((int(corp_id), offer_id))
            lp_outputs.append((
                offer_id,
                offer.get("typeID", 0),
                offer.get("quantity", 1),
                offer.get("iskCost", 0),
                offer.get("lpCost", 0),
                offer.get("akCost", 0),
            ))
            for req in offer.get("requiredItems", []) or []:
                lp_requirements.append((offer_id, req.get("typeID"), req.get("quantity", 0)))

    conn.executemany("INSERT OR REPLACE INTO npcCorporations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    if corp_lp_offers:
        conn.executemany("INSERT OR REPLACE INTO loyalty_offers VALUES (?,?)", corp_lp_offers)
        conn.executemany("INSERT OR REPLACE INTO loyalty_offer_outputs VALUES (?,?,?,?,?,?)", lp_outputs)
        if lp_requirements:
            conn.executemany("INSERT OR REPLACE INTO loyalty_offer_requirements VALUES (?,?,?)", lp_requirements)
    conn.commit()
    log(f"  {len(rows)} npcCorporations, {len(corp_lp_offers)} loyalty offers")


def insert_agents(conn: sqlite3.Connection, sde_dir: str):
    path = fsd_path(sde_dir, "agents.yaml")
    if not os.path.exists(path):
        return
    log("Inserting agents...")
    data = load_yaml(path)

    names_path = os.path.join(sde_dir, "bsd", "chrNPCCharacters.yaml")
    agent_names = {}
    if os.path.exists(names_path):
        name_data = load_yaml(names_path)
        for entry in name_data:
            agent_names[entry.get("characterID")] = entry.get("characterName")

    rows = []
    for agent_id, entry in data.items():
        aid = int(agent_id)
        rows.append((
            aid,
            entry.get("agentTypeID"), entry.get("corporationID"),
            entry.get("divisionID"), entry.get("isLocator", 0),
            entry.get("level"), entry.get("locationID"),
            entry.get("solarSystemID"),
            agent_names.get(aid),
        ))
    conn.executemany("INSERT OR REPLACE INTO agents VALUES (?,?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    log(f"  {len(rows)} agents")


def insert_planet_schematics(conn: sqlite3.Connection, sde_dir: str):
    path = fsd_path(sde_dir, "planetSchematics.yaml")
    if not os.path.exists(path):
        return
    log("Inserting planetSchematics...")
    data = load_yaml(path)
    rows = []
    for sch_id, entry in data.items():
        outputs = entry.get("types", {})
        out_type = None
        out_qty = 0
        in_types = []
        in_qtys = []
        for tid, tdata in outputs.items():
            if tdata.get("isInput", True) is False:
                out_type = int(tid)
                out_qty = tdata.get("quantity", 0)
            else:
                in_types.append(str(tid))
                in_qtys.append(str(tdata.get("quantity", 0)))

        if out_type is None:
            continue
        rows.append((
            int(sch_id), out_type,
            entry.get("nameID", {}).get("en") if isinstance(entry.get("nameID"), dict) else None,
            None,
            entry.get("cycleTime"),
            out_qty,
            ",".join(in_types), ",".join(in_qtys),
        ))
    conn.executemany("INSERT OR REPLACE INTO planetSchematics VALUES (?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    log(f"  {len(rows)} planetSchematics")


def insert_blueprints(conn: sqlite3.Connection, sde_dir: str):
    path = fsd_path(sde_dir, "blueprints.yaml")
    if not os.path.exists(path):
        return
    log("Inserting blueprints (loading YAML — may take 30s)...")
    data = load_yaml(path)

    type_info = {}
    cur = conn.cursor()
    cur.execute("SELECT type_id, en_name, icon_filename FROM types")
    for row in cur.fetchall():
        type_info[row[0]] = (row[1], row[2])

    process_rows = []
    mfg_mat_rows = []
    mfg_out_rows = []
    mfg_skill_rows = []
    rm_mat_rows = []
    rm_skill_rows = []
    rt_mat_rows = []
    rt_skill_rows = []
    copy_mat_rows = []
    copy_skill_rows = []
    inv_mat_rows = []
    inv_prod_rows = []
    inv_skill_rows = []

    def type_name(tid):
        ti = type_info.get(tid, (None, None))
        return ti[0]

    def type_icon(tid):
        ti = type_info.get(tid, (None, None))
        return ti[1]

    for bp_id_raw, entry in data.items():
        bp_id = int(bp_id_raw)
        bp_name = type_name(bp_id)
        bp_icon = type_icon(bp_id)
        acts = entry.get("activities", {}) or {}
        max_runs = entry.get("maxProductionLimit")

        mfg = acts.get("manufacturing") or {}
        rm = acts.get("research_material") or {}
        rt = acts.get("research_time") or {}
        copying = acts.get("copying") or {}
        invention = acts.get("invention") or {}

        process_rows.append((
            bp_id, bp_name, bp_icon,
            mfg.get("time"), rm.get("time"), rt.get("time"),
            copying.get("time"), invention.get("time"), max_runs,
        ))

        for mat in mfg.get("materials", []) or []:
            tid = mat.get("typeID")
            mfg_mat_rows.append((bp_id, bp_name, bp_icon, tid, type_name(tid), type_icon(tid), mat.get("quantity")))
        for prod in mfg.get("products", []) or []:
            tid = prod.get("typeID")
            mfg_out_rows.append((bp_id, bp_name, bp_icon, tid, type_name(tid), type_icon(tid), prod.get("quantity")))
        for sk in mfg.get("skills", []) or []:
            tid = sk.get("typeID")
            mfg_skill_rows.append((bp_id, bp_name, bp_icon, tid, type_name(tid), type_icon(tid), sk.get("level")))

        for mat in rm.get("materials", []) or []:
            tid = mat.get("typeID")
            rm_mat_rows.append((bp_id, bp_name, bp_icon, tid, type_name(tid), type_icon(tid), mat.get("quantity")))
        for sk in rm.get("skills", []) or []:
            tid = sk.get("typeID")
            rm_skill_rows.append((bp_id, bp_name, bp_icon, tid, type_name(tid), type_icon(tid), sk.get("level")))

        for mat in rt.get("materials", []) or []:
            tid = mat.get("typeID")
            rt_mat_rows.append((bp_id, bp_name, bp_icon, tid, type_name(tid), type_icon(tid), mat.get("quantity")))
        for sk in rt.get("skills", []) or []:
            tid = sk.get("typeID")
            rt_skill_rows.append((bp_id, bp_name, bp_icon, tid, type_name(tid), type_icon(tid), sk.get("level")))

        for mat in copying.get("materials", []) or []:
            tid = mat.get("typeID")
            copy_mat_rows.append((bp_id, bp_name, bp_icon, tid, type_name(tid), type_icon(tid), mat.get("quantity")))
        for sk in copying.get("skills", []) or []:
            tid = sk.get("typeID")
            copy_skill_rows.append((bp_id, bp_name, bp_icon, tid, type_name(tid), type_icon(tid), sk.get("level")))

        for mat in invention.get("materials", []) or []:
            tid = mat.get("typeID")
            inv_mat_rows.append((bp_id, bp_name, bp_icon, tid, type_name(tid), type_icon(tid), mat.get("quantity")))
        for prod in invention.get("products", []) or []:
            tid = prod.get("typeID")
            inv_prod_rows.append((bp_id, bp_name, bp_icon, tid, type_name(tid), type_icon(tid), prod.get("quantity"), prod.get("probability")))
        for sk in invention.get("skills", []) or []:
            tid = sk.get("typeID")
            inv_skill_rows.append((bp_id, bp_name, bp_icon, tid, type_name(tid), type_icon(tid), sk.get("level")))

    conn.executemany("INSERT OR REPLACE INTO blueprint_process_time VALUES (?,?,?,?,?,?,?,?,?)", process_rows)
    conn.executemany("INSERT OR REPLACE INTO blueprint_manufacturing_materials VALUES (?,?,?,?,?,?,?)", mfg_mat_rows)
    conn.executemany("INSERT OR REPLACE INTO blueprint_manufacturing_output VALUES (?,?,?,?,?,?,?)", mfg_out_rows)
    conn.executemany("INSERT OR REPLACE INTO blueprint_manufacturing_skills VALUES (?,?,?,?,?,?,?)", mfg_skill_rows)
    conn.executemany("INSERT OR REPLACE INTO blueprint_research_material_materials VALUES (?,?,?,?,?,?,?)", rm_mat_rows)
    conn.executemany("INSERT OR REPLACE INTO blueprint_research_material_skills VALUES (?,?,?,?,?,?,?)", rm_skill_rows)
    conn.executemany("INSERT OR REPLACE INTO blueprint_research_time_materials VALUES (?,?,?,?,?,?,?)", rt_mat_rows)
    conn.executemany("INSERT OR REPLACE INTO blueprint_research_time_skills VALUES (?,?,?,?,?,?,?)", rt_skill_rows)
    conn.executemany("INSERT OR REPLACE INTO blueprint_copying_materials VALUES (?,?,?,?,?,?,?)", copy_mat_rows)
    conn.executemany("INSERT OR REPLACE INTO blueprint_copying_skills VALUES (?,?,?,?,?,?,?)", copy_skill_rows)
    conn.executemany("INSERT OR REPLACE INTO blueprint_invention_materials VALUES (?,?,?,?,?,?,?)", inv_mat_rows)
    conn.executemany("INSERT OR REPLACE INTO blueprint_invention_products VALUES (?,?,?,?,?,?,?,?)", inv_prod_rows)
    conn.executemany("INSERT OR REPLACE INTO blueprint_invention_skills VALUES (?,?,?,?,?,?,?)", inv_skill_rows)
    conn.commit()
    log(f"  {len(process_rows)} blueprints processed")


def insert_version_info(conn: sqlite3.Connection):
    log("Fetching version info from ESI...")
    status = esi_get(f"{ESI_BASE}/status/")
    if status:
        build = status.get("server_version", 0)
        try:
            build_int = int(build)
        except (TypeError, ValueError):
            build_int = 0
        conn.execute(
            "INSERT OR REPLACE INTO version_info (id, build_number, patch_number, release_date, build_key) VALUES (1, ?, 0, ?, 'sde')",
            (build_int, status.get("start_time")),
        )
        conn.commit()
        log(f"  build_number = {build_int}")
    else:
        log("  ESI unreachable — version_info skipped")


def fetch_ru_descriptions(conn: sqlite3.Connection, workers: int):
    log("Fetching Russian descriptions from ESI...")
    cur = conn.cursor()
    cur.execute("SELECT type_id FROM types WHERE published = 1")
    type_ids = [row[0] for row in cur.fetchall()]
    log(f"  {len(type_ids)} published types to fetch")

    import ssl
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    def fetch_one(type_id):
        url = f"{ESI_BASE}/universe/types/{type_id}/?language=ru"
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, context=ctx, timeout=15) as r:
                d = json.loads(r.read())
                return type_id, d.get("description") or ""
        except Exception:
            return type_id, None

    results = {}
    failed = []
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(fetch_one, tid): tid for tid in type_ids}
        for future in as_completed(futures):
            tid, desc = future.result()
            done += 1
            if desc is not None:
                results[tid] = desc
            else:
                failed.append(tid)
            if done % 500 == 0:
                log(f"  {done}/{len(type_ids)} done...")

    for tid, desc in results.items():
        conn.execute("UPDATE types SET description_ru = ? WHERE type_id = ?", (desc, tid))
    conn.commit()
    log(f"  {len(results)} OK, {len(failed)} failed")


def create_indexes(conn: sqlite3.Connection):
    log("Creating indexes...")
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_types_groupID ON types(groupID);
        CREATE INDEX IF NOT EXISTS idx_types_categoryID ON types(categoryID);
        CREATE INDEX IF NOT EXISTS idx_types_published ON types(published);
        CREATE INDEX IF NOT EXISTS idx_types_marketGroupID ON types(marketGroupID);
        CREATE INDEX IF NOT EXISTS idx_marketGroups_parentgroup_id ON marketGroups(parentgroup_id);
        CREATE INDEX IF NOT EXISTS idx_typeAttributes_type_id ON typeAttributes(type_id);
        CREATE INDEX IF NOT EXISTS idx_typeAttributes_attr_id ON typeAttributes(attribute_id);
        CREATE INDEX IF NOT EXISTS idx_typeSkillRequirement_typeid ON typeSkillRequirement(typeid);
        CREATE INDEX IF NOT EXISTS idx_typeSkillRequirement_skill ON typeSkillRequirement(required_skill_id);
        CREATE INDEX IF NOT EXISTS idx_stations_solarSystemID ON stations(solarSystemID);
        CREATE INDEX IF NOT EXISTS idx_npcCorporations_faction_id ON npcCorporations(faction_id);
        CREATE INDEX IF NOT EXISTS idx_loyalty_offers_corporation_id ON loyalty_offers(corporation_id);
        CREATE INDEX IF NOT EXISTS idx_loyalty_offer_outputs_type_id ON loyalty_offer_outputs(type_id);
        CREATE INDEX IF NOT EXISTS idx_loyalty_offer_outputs_lp_cost ON loyalty_offer_outputs(lp_cost);
        CREATE INDEX IF NOT EXISTS idx_loyalty_offer_requirements_offer_id ON loyalty_offer_requirements(offer_id);
        CREATE INDEX IF NOT EXISTS idx_loyalty_offer_requirements_type_id ON loyalty_offer_requirements(required_type_id);
        CREATE INDEX IF NOT EXISTS idx_agents_solarSystemID ON agents(solarSystemID);
        CREATE INDEX IF NOT EXISTS idx_agents_locationID ON agents(locationID);
        CREATE INDEX IF NOT EXISTS idx_agents_corporationID ON agents(corporationID);
    """)
    conn.commit()
    log("  Indexes created")


def main():
    parser = argparse.ArgumentParser(description="Generate item_db_en.sqlite from CCP SDE")
    parser.add_argument("--sde-zip", help="Path to sde.zip (skip download)")
    parser.add_argument("--sde-dir", help="Path to extracted sde/ dir (skip download+extract)")
    parser.add_argument("--out", default=DEFAULT_OUT, help=f"Output path (default: {DEFAULT_OUT})")
    parser.add_argument("--ru-descriptions", action="store_true", help="Fetch Russian descriptions from ESI")
    parser.add_argument("--workers", type=int, default=30, help="Worker threads for ESI fetching")
    args = parser.parse_args()

    sde_dir = args.sde_dir

    if sde_dir is None:
        zip_path = args.sde_zip
        if zip_path is None:
            zip_path = "/tmp/sde.zip"
            if not os.path.exists(zip_path):
                download_sde(zip_path)
            else:
                log(f"Reusing existing {zip_path}")

        extract_dir = "/tmp/sde_extracted"
        os.makedirs(extract_dir, exist_ok=True)

        sde_dir = os.path.join(extract_dir, "sde")
        if not os.path.exists(sde_dir):
            extract_sde(zip_path, extract_dir)

        if not os.path.isdir(sde_dir):
            top = os.listdir(extract_dir)
            log(f"Contents of {extract_dir}: {top[:20]}")
            if len(top) == 1 and os.path.isdir(os.path.join(extract_dir, top[0])):
                sde_dir = os.path.join(extract_dir, top[0])
                log(f"Using {sde_dir} as SDE root")
            elif os.path.isdir(os.path.join(extract_dir, "fsd")):
                sde_dir = extract_dir
                log(f"ZIP extracted flat with fsd/, using {sde_dir} as SDE root")
            elif any(f.endswith(".yaml") for f in top):
                sde_dir = extract_dir
                log(f"ZIP extracted flat (no fsd/), using {sde_dir} as SDE root")

    if not os.path.isdir(sde_dir):
        print(f"ERROR: SDE directory not found: {sde_dir}")
        sys.exit(1)

    fsd_dir = os.path.join(sde_dir, "fsd")
    if os.path.isdir(fsd_dir):
        fsd_files = sorted(os.listdir(fsd_dir))
        log(f"fsd/ contents ({len(fsd_files)} files): {fsd_files[:30]}")
    else:
        root_files = sorted(f for f in os.listdir(sde_dir) if f.endswith(".yaml"))
        log(f"SDE root YAML files ({len(root_files)}): {root_files[:30]}")

    out_path = args.out
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    if os.path.exists(out_path):
        os.remove(out_path)
        log(f"Removed existing {out_path}")

    log(f"Creating {out_path}")
    conn = sqlite3.connect(out_path)
    conn.execute("PRAGMA foreign_keys = OFF")

    t0 = time.time()
    create_schema(conn)
    fsd_strings = load_fsd_strings(sde_dir)
    icon_filenames = load_icon_filenames(sde_dir)
    insert_categories(conn, sde_dir, icon_filenames)
    insert_groups(conn, sde_dir, icon_filenames)
    insert_meta_groups(conn, sde_dir, fsd_strings)
    insert_market_groups(conn, sde_dir, fsd_strings, icon_filenames)
    insert_types(conn, sde_dir, icon_filenames)
    populate_representative_types(conn)
    insert_dogma_attribute_categories(conn)
    insert_dogma_attributes(conn, sde_dir, icon_filenames)
    insert_dogma_effects(conn, sde_dir)
    insert_types_dogma(conn, sde_dir)
    insert_universe(conn, sde_dir)
    insert_stations(conn, sde_dir)
    insert_factions(conn, sde_dir)
    insert_npc_corporations(conn, sde_dir)
    insert_agents(conn, sde_dir)
    insert_planet_schematics(conn, sde_dir)
    insert_blueprints(conn, sde_dir)
    insert_version_info(conn)

    if args.ru_descriptions:
        fetch_ru_descriptions(conn, args.workers)

    create_indexes(conn)

    log("Running VACUUM + ANALYZE...")
    conn.execute("ANALYZE")
    conn.execute("VACUUM")
    conn.close()

    size_mb = os.path.getsize(out_path) / (1024 * 1024)
    elapsed = time.time() - t0
    log(f"Done in {elapsed:.0f}s — {size_mb:.1f} MB → {out_path}")

    bundle_path = os.path.splitext(out_path)[0] + ".zip"
    log(f"Creating bundle {bundle_path} (SQLite only — icons bundled with app)")
    with zipfile.ZipFile(bundle_path, "w") as zf:
        zf.write(out_path, "item_db_en.sqlite", compress_type=zipfile.ZIP_DEFLATED)
    log(f"Bundle: {os.path.getsize(bundle_path) / 1024 / 1024:.1f} MB → {bundle_path}")


if __name__ == "__main__":
    main()
