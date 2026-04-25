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

        CREATE TABLE IF NOT EXISTS stargates (
            from_system_id INTEGER NOT NULL,
            to_system_id INTEGER NOT NULL,
            PRIMARY KEY (from_system_id, to_system_id)
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
            name TEXT,
            en_name TEXT,
            ru_name TEXT
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

        CREATE TABLE IF NOT EXISTS typeMaterials (
            typeid INTEGER NOT NULL,
            process_size INTEGER NOT NULL,
            output_material INTEGER NOT NULL,
            output_quantity INTEGER NOT NULL,
            output_material_name TEXT,
            output_material_icon TEXT,
            categoryid INTEGER,
            PRIMARY KEY (typeid, output_material)
        );

        CREATE TABLE IF NOT EXISTS version_info (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            build_number INTEGER NOT NULL,
            patch_number INTEGER DEFAULT 0,
            release_date TEXT,
            build_key TEXT,
            description TEXT DEFAULT 'EVE SDE Database Version Information'
        );

        CREATE TABLE IF NOT EXISTS ore_colors (
            type_id INTEGER NOT NULL PRIMARY KEY,
            hex_color TEXT NOT NULL
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
        if not icon_name:
            icon_name = f"category_{int(cat_id)}"
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
            entry.get("portionSize"),
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
    populate_npc_ship_data(conn)


ENTITY_CATEGORY_ID = 11

SCENE_PREFIXES = [
    ("Asteroid ", "Asteroid"),
    ("Deadspace ", "Deadspace"),
    ("Mission ", "Mission"),
    ("Storyline ", "Storyline"),
    ("Storyline Mission ", "Storyline"),
    ("Incursion ", "Incursion"),
    ("Abyssal ", "Abyssal"),
    ("Faction Warfare ", "Faction Warfare"),
    ("FW ", "Faction Warfare"),
]

KNOWN_FACTIONS = [
    "Angel Cartel", "Blood Raider", "Blood Raiders", "Guristas", "Sansha",
    "Serpentis", "Rogue Drone", "Sleeper", "Drifter", "CONCORD", "Mordu",
    "Amarr Empire", "Caldari State", "Gallente Federation", "Minmatar Republic",
    "Khanid", "Thukker", "Generic", "Faction", "Overseer",
]

KNOWN_SHIP_TYPES = [
    "Titan", "Supercarrier", "Carrier", "Dreadnought",
    "Battleship", "Battlecruiser", "BattleCruiser",
    "Cruiser", "Destroyer", "Frigate", "Hauler", "Drone",
]

DEFAULT_FACTION_ICON = "faction_500021.png"

FACTION_ICON_MAP = {
    "Angel Cartel": "faction_500011.png",
    "Blood Raider": "faction_500012.png",
    "Blood Raiders": "faction_500012.png",
    "Guristas": "faction_500010.png",
    "Sansha": "faction_500019.png",
    "Serpentis": "faction_500020.png",
    "Rogue Drone": "faction_500021.png",
    "Sleeper": "faction_500021.png",
    "Drifter": "faction_500021.png",
    "CONCORD": "faction_500006.png",
    "Mordu": "faction_500018.png",
    "Amarr Empire": "faction_500003.png",
    "Caldari State": "faction_500001.png",
    "Gallente Federation": "faction_500004.png",
    "Minmatar Republic": "faction_500002.png",
    "Khanid": "faction_500008.png",
    "Thukker": "faction_500015.png",
}


def _derive_npc_from_group(group_name: str):
    if not group_name:
        return "Other", "Other", "Other", DEFAULT_FACTION_ICON

    scene = "Other"
    remainder = group_name
    for prefix, scene_name in SCENE_PREFIXES:
        if group_name.startswith(prefix):
            scene = scene_name
            remainder = group_name[len(prefix):]
            break

    faction = "Other"
    for f in sorted(KNOWN_FACTIONS, key=len, reverse=True):
        if f in remainder:
            faction = f
            remainder = remainder.replace(f, "").strip()
            break

    ship_type = "Other"
    for t in KNOWN_SHIP_TYPES:
        if t.lower() in remainder.lower():
            ship_type = t
            if ship_type == "BattleCruiser":
                ship_type = "Cruiser"
            break

    icon = FACTION_ICON_MAP.get(faction, DEFAULT_FACTION_ICON)
    return scene, faction, ship_type, icon


def populate_npc_ship_data(conn: sqlite3.Connection):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(script_dir, "npc_ship_data.json")

    npc_map = {}
    if os.path.exists(json_path):
        with open(json_path, "r", encoding="utf-8") as f:
            entries = json.load(f)
        for e in entries:
            npc_map[e["type_id"]] = (
                e["npc_ship_scene"],
                e["npc_ship_faction"],
                e["npc_ship_type"],
                e["npc_ship_faction_icon"],
            )
        log(f"  Loaded {len(npc_map)} NPC ship mappings from npc_ship_data.json")
    else:
        log("  npc_ship_data.json not found — will derive NPC data from group names only")

    cursor = conn.execute(
        "SELECT type_id, group_name FROM types WHERE category_name = 'Entity'"
    )
    entity_types = cursor.fetchall()

    rows = []
    derived_count = 0
    for type_id, group_name in entity_types:
        if type_id in npc_map:
            scene, faction, ship_type, icon = npc_map[type_id]
        else:
            scene, faction, ship_type, icon = _derive_npc_from_group(group_name)
            derived_count += 1
        rows.append((scene, faction, ship_type, icon, type_id))

    if rows:
        conn.executemany(
            "UPDATE types SET npc_ship_scene=?, npc_ship_faction=?, npc_ship_type=?, npc_ship_faction_icon=? WHERE type_id=?",
            rows
        )
        conn.commit()

    log(f"  {len(rows)} NPC ships classified ({len(rows) - derived_count} from JSON, {derived_count} derived from group names)")


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
    stargates_data = sys_data.get("stargates", {}) or {}
    has_gate = 1 if stargates_data else 0

    stargate_entries = []
    for gate_id, gate_info in stargates_data.items():
        dest_gate = gate_info.get("destination")
        if dest_gate:
            stargate_entries.append((int(gate_id), int(sys_id), int(dest_gate)))

    planets = sys_data.get("planets", {}) or {}
    planet_counts = {col: 0 for col in ["temperate", "barren", "oceanic", "ice", "gas", "lava", "storm", "plasma"]}
    celestial_rows = []
    for planet_id, planet_data in planets.items():
        col = PLANET_TYPE_TO_COLUMN.get(planet_data.get("typeID"))
        if col:
            planet_counts[col] += 1
        cel_idx = planet_data.get("celestialIndex")
        if cel_idx is not None:
            planet_name = f"{sys_name} {_roman(int(cel_idx))}"
            celestial_rows.append((int(planet_id), planet_name))
            for moon_id, moon_data in (planet_data.get("moons", {}) or {}).items():
                moon_idx = moon_data.get("moonIndex") or moon_data.get("orbitIndex")
                if moon_idx is not None:
                    celestial_rows.append((int(moon_id), f"{planet_name} - Moon {moon_idx}"))

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
    return sys_row, univ_row, celestial_rows, stargate_entries


def _localized_name(name_obj, default: str = "") -> tuple:
    """Extract (en, de, es, fr, ja, ko, ru, zh) from a name dict or string."""
    if isinstance(name_obj, dict):
        en = name_obj.get("en", default)
        return (en, name_obj.get("de", en), en, name_obj.get("es", en),
                name_obj.get("fr", en), name_obj.get("ja", en),
                name_obj.get("ko", en), name_obj.get("ru", en),
                name_obj.get("zh", en))
    s = str(name_obj) if name_obj else default
    return (s, s, s, s, s, s, s, s, s)


def _insert_universe_from_map_files(conn: sqlite3.Connection, sde_dir: str):
    """Populate regions, constellations, solarsystems, universe from mapXxx.yaml files."""
    regions_path = fsd_path(sde_dir, "mapRegions.yaml")
    consts_path = fsd_path(sde_dir, "mapConstellations.yaml")
    systems_path = fsd_path(sde_dir, "mapSolarSystems.yaml")

    if not os.path.exists(systems_path):
        log("SKIP: mapSolarSystems.yaml not found")
        return

    log("Inserting universe data from mapXxx.yaml files...")
    region_rows = []
    const_rows = []
    sys_rows = []
    univ_rows = []

    sys_to_const = {}
    const_to_region = {}

    if os.path.exists(regions_path):
        regions_data = load_yaml(regions_path)
        for region_id, entry in regions_data.items():
            n = _localized_name(entry.get("name", ""))
            region_rows.append((int(region_id), n[0], n[1], n[2], n[3], n[4], n[5], n[6], n[7], n[8]))

    if os.path.exists(consts_path):
        consts_data = load_yaml(consts_path)
        for const_id, entry in consts_data.items():
            n = _localized_name(entry.get("name", ""))
            const_rows.append((int(const_id), n[0], n[1], n[2], n[3], n[4], n[5], n[6], n[7], n[8]))
            rid = entry.get("regionID")
            if rid:
                const_to_region[int(const_id)] = int(rid)

    systems_data = load_yaml(systems_path)
    celestial_rows = []
    for sys_id, entry in systems_data.items():
        n = _localized_name(entry.get("name", ""))
        sys_name = n[0] or ""
        sec = entry.get("securityStatus") or entry.get("security") or 0.0
        sys_rows.append((int(sys_id), n[0], n[1], n[2], n[3], n[4], n[5], n[6], n[7], n[8], sec))

        const_id = entry.get("constellationID", 0)
        region_id = const_to_region.get(const_id, 0)
        pos = entry.get("position", {})
        x = pos.get("x", 0.0)
        y = pos.get("y", 0.0)
        z = pos.get("z", 0.0)
        is_jspace = 1 if (n[0] or "").startswith("J") and any(c.isdigit() for c in (n[0] or "")) else 0
        univ_rows.append((
            region_id, const_id, int(sys_id), sec, 0,
            x, y, z,
            0, 0, is_jspace, 0,
            0, 0, 0, 0, 0, 0, 0, 0
        ))

        stargates_map = entry.get("stargates", {}) or {}
        has_gate = 1 if stargates_map else 0
        if has_gate:
            univ_rows[-1] = (
                region_id, const_id, int(sys_id), sec, 0,
                x, y, z,
                0, has_gate, is_jspace, 0,
                0, 0, 0, 0, 0, 0, 0, 0
            )

        planets = entry.get("planets", {}) or {}
        for planet_id, planet_data in planets.items():
            cel_idx = planet_data.get("celestialIndex")
            if cel_idx is not None and sys_name:
                planet_name = f"{sys_name} {_roman(int(cel_idx))}"
                celestial_rows.append((int(planet_id), planet_name))

    conn.executemany("INSERT OR REPLACE INTO regions VALUES (?,?,?,?,?,?,?,?,?,?)", region_rows)
    conn.executemany("INSERT OR REPLACE INTO constellations VALUES (?,?,?,?,?,?,?,?,?,?)", const_rows)
    conn.executemany("INSERT OR REPLACE INTO solarsystems VALUES (?,?,?,?,?,?,?,?,?,?,?)", sys_rows)
    conn.executemany("INSERT OR REPLACE INTO universe VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", univ_rows)
    conn.executemany("INSERT OR REPLACE INTO celestialNames VALUES (?,?)", celestial_rows)
    conn.commit()
    log(f"  {len(region_rows)} regions, {len(const_rows)} constellations, {len(sys_rows)} solar systems, {len(celestial_rows)} celestials inserted")

    # Parse stargates from mapStargates.yaml (flat format)
    stargates_path = fsd_path(sde_dir, "mapStargates.yaml")
    if os.path.exists(stargates_path):
        log("Inserting stargates from mapStargates.yaml...")
        stargates_data = load_yaml(stargates_path)
        stargate_rows = []
        systems_with_gates = set()
        for gate_id, gate_info in stargates_data.items():
            from_sys = gate_info.get("solarSystemID")
            dest = gate_info.get("destination", {})
            to_sys = dest.get("solarSystemID") if dest else None
            if from_sys and to_sys and from_sys != to_sys:
                stargate_rows.append((int(from_sys), int(to_sys)))
                systems_with_gates.add(int(from_sys))
                systems_with_gates.add(int(to_sys))
        conn.executemany("INSERT OR REPLACE INTO stargates VALUES (?,?)", stargate_rows)
        if systems_with_gates:
            placeholders = ",".join(str(s) for s in systems_with_gates)
            conn.execute(f"UPDATE universe SET hasJumpGate = 1 WHERE solarsystem_id IN ({placeholders})")
        conn.commit()
        log(f"  {len(stargate_rows)} stargate connections, {len(systems_with_gates)} systems with gates")


def insert_universe(conn: sqlite3.Connection, sde_dir: str):
    universe_root = os.path.join(sde_dir, "universe")
    if not os.path.exists(universe_root):
        _insert_universe_from_map_files(conn, sde_dir)
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
    celestial_rows = []
    all_gate_entries = []  # (gate_id, owner_system_id, dest_gate_id)
    with ThreadPoolExecutor(max_workers=16) as ex:
        for result in ex.map(_parse_system, system_tasks):
            if result:
                sys_rows.append(result[0])
                univ_rows.append(result[1])
                celestial_rows.extend(result[2])
                all_gate_entries.extend(result[3])

    # Build gate_id -> system_id mapping, then resolve dest_gate -> dest_system
    gate_to_system = {entry[0]: entry[1] for entry in all_gate_entries}
    stargate_rows = []
    for gate_id, from_sys, dest_gate in all_gate_entries:
        dest_sys = gate_to_system.get(dest_gate)
        if dest_sys and from_sys != dest_sys:
            stargate_rows.append((from_sys, dest_sys))

    conn.executemany("INSERT OR REPLACE INTO regions VALUES (?,?,?,?,?,?,?,?,?,?)", region_rows)
    conn.executemany("INSERT OR REPLACE INTO constellations VALUES (?,?,?,?,?,?,?,?,?,?)", const_rows)
    conn.executemany("INSERT OR REPLACE INTO solarsystems VALUES (?,?,?,?,?,?,?,?,?,?,?)", sys_rows)
    conn.executemany("INSERT OR REPLACE INTO universe VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", univ_rows)
    conn.executemany("INSERT OR REPLACE INTO celestialNames VALUES (?,?)", celestial_rows)
    conn.executemany("INSERT OR REPLACE INTO stargates VALUES (?,?)", stargate_rows)
    conn.commit()
    log(f"  {len(sys_rows)} solar systems, {len(celestial_rows)} celestials, {len(stargate_rows)} stargate connections inserted")


def insert_celestial_names(conn: sqlite3.Connection, sde_dir: str):
    """Populate celestialNames from mapPlanets.yaml + mapMoons.yaml + mapSolarSystems.yaml."""
    planets_path = fsd_path(sde_dir, "mapPlanets.yaml")
    moons_path = fsd_path(sde_dir, "mapMoons.yaml")
    systems_path = fsd_path(sde_dir, "mapSolarSystems.yaml")

    if not os.path.exists(planets_path):
        log("SKIP insert_celestial_names: mapPlanets.yaml not found")
        return

    log("Inserting celestial names from mapPlanets/mapMoons/mapSolarSystems...")

    sys_names: dict = {}
    if os.path.exists(systems_path):
        sys_data = load_yaml(systems_path)
        for sys_id, entry in sys_data.items():
            name = entry.get("solarSystemNameID") or entry.get("solarSystemName") or entry.get("name", "")
            if isinstance(name, dict):
                name = name.get("en") or ""
            if not name and isinstance(entry, dict):
                n = _localized_name(entry.get("name", ""))
                name = n[0] or ""
            sys_names[int(sys_id)] = str(name)

    planet_names: dict = {}
    planets_data = load_yaml(planets_path)
    rows = []
    for planet_id, entry in planets_data.items():
        cel_idx = entry.get("celestialIndex")
        sys_id = entry.get("solarSystemID")
        if cel_idx is not None and sys_id is not None:
            sys_name = sys_names.get(int(sys_id), "")
            if sys_name:
                name = f"{sys_name} {_roman(int(cel_idx))}"
                planet_names[int(planet_id)] = name
                rows.append((int(planet_id), name))

    if os.path.exists(moons_path):
        moons_data = load_yaml(moons_path)
        for moon_id, entry in moons_data.items():
            orbit_id = entry.get("orbitID")
            orbit_idx = entry.get("orbitIndex")
            if orbit_id is not None and orbit_idx is not None:
                planet_name = planet_names.get(int(orbit_id), "")
                if planet_name:
                    rows.append((int(moon_id), f"{planet_name} - Moon {orbit_idx}"))

    conn.executemany("INSERT OR REPLACE INTO celestialNames VALUES (?,?)", rows)
    conn.commit()
    log(f"  {len(rows)} celestial names inserted")


def _roman(n: int) -> str:
    vals = [(1000,'M'),(900,'CM'),(500,'D'),(400,'CD'),(100,'C'),(90,'XC'),
            (50,'L'),(40,'XL'),(10,'X'),(9,'IX'),(5,'V'),(4,'IV'),(1,'I')]
    r = ""
    for v, s in vals:
        while n >= v:
            r += s; n -= v
    return r


def _build_station_names(sde_dir: str) -> dict:
    """Build stationID -> name map from npcStations + supporting YAML files."""
    log("  Building station names (loading YAML in parallel)...")
    npc_path = fsd_path(sde_dir, "npcStations.yaml")

    paths = {
        "stations": npc_path,
        "systems": fsd_path(sde_dir, "mapSolarSystems.yaml"),
        "moons": fsd_path(sde_dir, "mapMoons.yaml"),
        "planets": fsd_path(sde_dir, "mapPlanets.yaml"),
        "corps": fsd_path(sde_dir, "npcCorporations.yaml"),
        "ops": fsd_path(sde_dir, "stationOperations.yaml"),
    }

    def _load(key):
        p = paths[key]
        return (key, load_yaml(p) if os.path.exists(p) else {})

    loaded = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        for key, data in ex.map(_load, paths.keys()):
            loaded[key] = data

    stations = loaded["stations"]
    systems = loaded["systems"]
    moons = loaded["moons"]
    planets = loaded["planets"]
    corps = loaded["corps"]
    ops = loaded["ops"]

    result = {}
    for station_id, entry in stations.items():
        solar_sys = systems.get(entry.get("solarSystemID"), {})
        sys_name_obj = solar_sys.get("name", {})
        sys_name = sys_name_obj.get("en", "") if isinstance(sys_name_obj, dict) else str(sys_name_obj)

        orbit_id = entry.get("orbitID")
        moon_data = moons.get(orbit_id, {})
        moon_orbit_index = moon_data.get("orbitIndex")

        planet_id = moon_data.get("orbitID")
        planet_data = planets.get(planet_id, {})
        planet_index = planet_data.get("celestialIndex") or entry.get("celestialIndex")

        owner_id = entry.get("ownerID")
        corp_data = corps.get(owner_id, {})
        corp_name_obj = corp_data.get("name", {})
        corp_name = corp_name_obj.get("en", "") if isinstance(corp_name_obj, dict) else str(corp_name_obj)

        op_id = entry.get("operationID")
        op_data = ops.get(op_id, {})
        op_name_obj = op_data.get("operationName", {})
        op_name = op_name_obj.get("en", "") if isinstance(op_name_obj, dict) else str(op_name_obj)

        if sys_name and planet_index and moon_orbit_index is not None:
            name = f"{sys_name} {_roman(planet_index)} - Moon {moon_orbit_index} - {corp_name} {op_name}"
        elif sys_name and planet_index:
            name = f"{sys_name} {_roman(planet_index)} - {corp_name} {op_name}"
        else:
            name = f"{sys_name} - {corp_name} {op_name}"
        security = solar_sys.get("securityStatus") or solar_sys.get("security")
        result[int(station_id)] = (name.strip(), security)
    return result


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
        station_info = _build_station_names(sde_dir)
        for station_id, entry in data.items():
            type_id = entry.get("typeID")
            info = station_info.get(int(station_id))
            name = info[0] if info else f"Station {station_id}"
            security = info[1] if info else None
            rows.append((
                int(station_id),
                type_id,
                name,
                None,
                entry.get("solarSystemID"),
                security,
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


def insert_factions(conn: sqlite3.Connection, sde_dir: str) -> dict:
    path = fsd_path(sde_dir, "factions.yaml")
    if not os.path.exists(path):
        return {}
    log("Inserting factions...")
    data = load_yaml(path)
    rows = []
    militia_corp_to_faction = {}
    for fact_id, entry in data.items():
        names = multiname(entry)
        rows.append((
            int(fact_id),
            names.get("en"), names.get("de"), names.get("en"),
            names.get("es"), names.get("fr"), names.get("ja"),
            names.get("ko"), names.get("ru"), names.get("zh"),
            None, None, f"faction_{int(fact_id)}.png",
        ))
        militia_corp_id = entry.get("militiaCorporationID")
        if militia_corp_id:
            militia_corp_to_faction[int(militia_corp_id)] = int(fact_id)
    conn.executemany("INSERT OR REPLACE INTO factions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    log(f"  {len(rows)} factions")
    return militia_corp_to_faction


def insert_npc_corporations(conn: sqlite3.Connection, sde_dir: str, militia_corp_to_faction: dict = None):
    path = fsd_path(sde_dir, "npcCorporations.yaml")
    if not os.path.exists(path):
        return
    log("Inserting npcCorporations...")
    if militia_corp_to_faction is None:
        militia_corp_to_faction = {}
    data = load_yaml(path)
    rows = []
    corp_lp_offers = []
    lp_outputs = []
    lp_requirements = []

    for corp_id, entry in data.items():
        cid = int(corp_id)
        names = multiname(entry)
        militia_faction = militia_corp_to_faction.get(cid) or entry.get("militiaFactionID")
        rows.append((
            cid,
            names.get("en"), names.get("de"), names.get("en"),
            names.get("es"), names.get("fr"), names.get("ja"),
            names.get("ko"), names.get("ru"), names.get("zh"),
            None, entry.get("factionID"), militia_faction,
            f"corperation_{cid}_128.png",
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

    lp_db = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lp_data.sqlite")
    if os.path.exists(lp_db):
        log("  Importing loyalty data from lp_data.sqlite...")
        lp_conn = sqlite3.connect(lp_db)
        for tbl in ["loyalty_offers", "loyalty_offer_outputs", "loyalty_offer_requirements"]:
            lp_rows = lp_conn.execute(f"SELECT * FROM {tbl}").fetchall()
            if lp_rows:
                ph = ",".join(["?"] * len(lp_rows[0]))
                conn.executemany(f"INSERT OR REPLACE INTO {tbl} VALUES ({ph})", lp_rows)
                if tbl == "loyalty_offers":
                    corp_lp_offers = lp_rows
                log(f"    {tbl}: {len(lp_rows)} rows")
        lp_conn.close()
    elif corp_lp_offers:
        conn.executemany("INSERT OR REPLACE INTO loyalty_offers VALUES (?,?)", corp_lp_offers)
        conn.executemany("INSERT OR REPLACE INTO loyalty_offer_outputs VALUES (?,?,?,?,?,?)", lp_outputs)
        if lp_requirements:
            conn.executemany("INSERT OR REPLACE INTO loyalty_offer_requirements VALUES (?,?,?)", lp_requirements)
    conn.commit()
    log(f"  {len(rows)} npcCorporations, {len(corp_lp_offers)} loyalty offers")


def insert_agents(conn: sqlite3.Connection, sde_dir: str):
    npc_path = fsd_path(sde_dir, "npcCharacters.yaml")
    old_path = fsd_path(sde_dir, "agents.yaml")

    if os.path.exists(npc_path):
        log("Inserting agents from npcCharacters.yaml...")
        data = load_yaml(npc_path)

        space_agents = {}
        space_path = fsd_path(sde_dir, "agentsInSpace.yaml")
        if os.path.exists(space_path):
            space_data = load_yaml(space_path)
            for aid, entry in space_data.items():
                space_agents[int(aid)] = entry.get("solarSystemID")

        rows = []
        for char_id, entry in data.items():
            if "agent" not in entry:
                continue
            aid = int(char_id)
            agent_info = entry["agent"]
            name_dict = entry.get("name", {})
            agent_name = name_dict.get("en") if isinstance(name_dict, dict) else None
            rows.append((
                aid,
                agent_info.get("agentTypeID"),
                entry.get("corporationID"),
                agent_info.get("divisionID"),
                1 if agent_info.get("isLocator") else 0,
                agent_info.get("level"),
                entry.get("locationID"),
                space_agents.get(aid),
                agent_name,
            ))
        conn.executemany("INSERT OR REPLACE INTO agents VALUES (?,?,?,?,?,?,?,?,?)", rows)
        conn.commit()
        log(f"  {len(rows)} agents")

    elif os.path.exists(old_path):
        log("Inserting agents from agents.yaml (legacy)...")
        data = load_yaml(old_path)

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
    else:
        log("SKIP: no agents data found (neither npcCharacters.yaml nor agents.yaml)")


def insert_divisions(conn: sqlite3.Connection, sde_dir: str):
    path = fsd_path(sde_dir, "npcCorporationDivisions.yaml")
    if not os.path.exists(path):
        return
    log("Inserting divisions...")
    data = load_yaml(path)
    rows = []
    for div_id, entry in data.items():
        name_dict = entry.get("name", {})
        en_name = name_dict.get("en", "") if isinstance(name_dict, dict) else ""
        ru_name = name_dict.get("ru") if isinstance(name_dict, dict) else None
        rows.append((int(div_id), en_name, en_name, ru_name))
    conn.executemany("INSERT OR REPLACE INTO divisions VALUES (?,?,?,?)", rows)
    conn.commit()
    log(f"  {len(rows)} divisions")


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
        pins = entry.get("pins", [])
        facilitys = ",".join(str(p) for p in pins) if pins else None
        rows.append((
            int(sch_id), out_type,
            entry.get("nameID", {}).get("en") if isinstance(entry.get("nameID"), dict) else None,
            facilitys,
            entry.get("cycleTime"),
            out_qty,
            ",".join(in_types), ",".join(in_qtys),
        ))
    conn.executemany("INSERT OR REPLACE INTO planetSchematics VALUES (?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    log(f"  {len(rows)} planetSchematics")


def insert_type_materials(conn: sqlite3.Connection, sde_dir: str):
    path = fsd_path(sde_dir, "typeMaterials.yaml")
    if not os.path.exists(path):
        log("SKIP: fsd/typeMaterials.yaml not found")
        return
    log("Inserting typeMaterials...")
    data = load_yaml(path)

    type_info = {}
    cur = conn.cursor()
    cur.execute("SELECT type_id, en_name, icon_filename, categoryID FROM types")
    for row in cur.fetchall():
        type_info[row[0]] = (row[1], row[2], row[3])

    portion_sizes = {}
    cur.execute("SELECT type_id, process_size FROM types WHERE process_size IS NOT NULL AND process_size > 0")
    for row in cur.fetchall():
        portion_sizes[row[0]] = row[1]

    rows = []
    for type_id_raw, entry in data.items():
        type_id = int(type_id_raw)
        materials = entry.get("materials", []) or []
        portion = portion_sizes.get(type_id, 1)
        ti = type_info.get(type_id, (None, None, None))
        cat_id = ti[2]
        for mat in materials:
            mat_tid = mat.get("materialTypeID")
            mat_qty = mat.get("quantity", 0)
            mi = type_info.get(mat_tid, (None, None, None))
            rows.append((type_id, portion, mat_tid, mat_qty, mi[0], mi[1], cat_id))

    conn.executemany("INSERT OR REPLACE INTO typeMaterials VALUES (?,?,?,?,?,?,?)", rows)
    conn.commit()
    log(f"  {len(rows)} typeMaterials rows")


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


def insert_dynamic_items(conn: sqlite3.Connection, sde_dir: str):
    path = fsd_path(sde_dir, "dynamicitemattributes.yaml", "dynamicItemAttributes.yaml")
    if not os.path.exists(path):
        log("SKIP: dynamicitemattributes.yaml not found")
        return
    log("Inserting dynamic item attributes...")
    data = load_yaml(path)
    if not data:
        return

    attr_rows = []
    mapping_rows = []

    for type_id, entry in data.items():
        dyn_attrs = entry.get("dynamicAttributes")
        if dyn_attrs:
            for attr_id, vals in dyn_attrs.items():
                attr_rows.append((
                    int(type_id),
                    int(attr_id),
                    round(vals.get("min", 0.0), 4),
                    round(vals.get("max", 0.0), 4),
                ))

        mappings = entry.get("inputOutputMapping")
        if mappings:
            for mapping in mappings:
                resulting_type = mapping.get("resultingType")
                if resulting_type is None:
                    continue
                for applicable_type in mapping.get("applicableTypes", []):
                    mapping_rows.append((
                        int(type_id),
                        int(applicable_type),
                        int(resulting_type),
                    ))

    conn.execute("DELETE FROM dynamic_item_attributes")
    conn.execute("DELETE FROM dynamic_item_mappings")
    conn.executemany(
        "INSERT OR REPLACE INTO dynamic_item_attributes (type_id, attribute_id, min_value, max_value) VALUES (?,?,?,?)",
        attr_rows,
    )
    conn.executemany(
        "INSERT OR REPLACE INTO dynamic_item_mappings (type_id, applicable_type, resulting_type) VALUES (?,?,?)",
        mapping_rows,
    )
    conn.commit()
    log(f"  {len(attr_rows)} attribute rows, {len(mapping_rows)} mapping rows")


def insert_ore_colors(conn: sqlite3.Connection):
    log("Inserting ore_colors...")
    rows = [
        (18,"#8a6847"),(19,"#9f886a"),(20,"#d39f73"),(21,"#2ce3e8"),(22,"#cd370c"),
        (1223,"#589751"),(1224,"#c9bb87"),(1225,"#ecc05f"),(1226,"#878162"),(1227,"#ecdd86"),
        (1228,"#8d785d"),(1229,"#13bb41"),(1230,"#d8b181"),(1231,"#785a39"),(1232,"#8e8769"),
        (11396,"#83260d"),(16263,"#6aa5d5"),(16264,"#185b79"),(16265,"#58758d"),
        (16266,"#82abc0"),(16267,"#5a7d9c"),(16268,"#99ddef"),(16269,"#6d9dbc"),
        (17425,"#cd370c"),(17426,"#cd370c"),(17428,"#589751"),(17429,"#589751"),
        (17432,"#ecc05f"),(17433,"#ecc05f"),(17436,"#8e8769"),(17437,"#8e8769"),
        (17440,"#2ce3e8"),(17441,"#2ce3e8"),(17444,"#785a39"),(17445,"#785a39"),
        (17448,"#878162"),(17449,"#878162"),(17452,"#d39f73"),(17453,"#d39f73"),
        (17455,"#8a6847"),(17456,"#8a6847"),(17459,"#c9bb87"),(17460,"#c9bb87"),
        (17463,"#8d785d"),(17464,"#8d785d"),(17466,"#9f886a"),(17467,"#9f886a"),
        (17470,"#d8b181"),(17471,"#d8b181"),(17865,"#13bb41"),(17866,"#13bb41"),
        (17867,"#ecdd86"),(17868,"#ecdd86"),(17869,"#83260d"),(17870,"#83260d"),
        (17975,"#185b79"),(17976,"#58758d"),(17977,"#6aa5d5"),
        # Cytoserocin (icon-extracted)
        (25268,"#f0d3af"),(25273,"#e5cb92"),(25274,"#a9eced"),(25275,"#bc7b9f"),
        (25276,"#a4bc7e"),(25277,"#adab63"),(25278,"#eed3d3"),(25279,"#7d95d1"),
        (28617,"#d8b181"),(28618,"#c9bb87"),(28619,"#ecdd86"),(28620,"#d39f73"),
        (28621,"#878162"),(28622,"#13bb41"),(28623,"#8e8769"),(28624,"#ecc05f"),
        (28625,"#cd370c"),(28626,"#83260d"),(28627,"#185b79"),
        (28629,"#f0d3af"),(28630,"#bc7b9f"),
        # Mykoserocin (icon-extracted)
        (28694,"#f0d3af"),(28695,"#7d95d1"),(28696,"#bc7b9f"),(28697,"#e5cb92"),
        (28698,"#adab63"),(28699,"#a4bc7e"),(28700,"#eed3d3"),(28701,"#a9eced"),
        # Fullerite (icon-extracted)
        (30370,"#eed3d3"),(30371,"#eed3d3"),(30372,"#7d95d1"),(30373,"#a4bc7e"),
        (30374,"#adab63"),(30375,"#adab63"),(30376,"#f0d3af"),(30377,"#e5cb92"),
        (30378,"#e5cb92"),
        (45490,"#b69f78"),(45491,"#b69f78"),(45492,"#b69f78"),(45493,"#b69f78"),
        (45494,"#688f95"),(45495,"#688f95"),(45496,"#688f95"),(45497,"#688f95"),
        (45498,"#7a4221"),(45499,"#7a4221"),(45500,"#7a4221"),(45501,"#7a4221"),
        (45502,"#365283"),(45503,"#365283"),(45504,"#365283"),(45506,"#365283"),
        (45510,"#c0837e"),(45511,"#c0837e"),(45512,"#c0837e"),(45513,"#c0837e"),
        (46280,"#b69f78"),(46281,"#c77835"),(46282,"#b69f78"),(46283,"#c77835"),
        (46284,"#b69f78"),(46285,"#c77835"),(46286,"#b69f78"),(46287,"#c77835"),
        (46288,"#688f95"),(46289,"#91d3dd"),(46290,"#688f95"),(46291,"#91d3dd"),
        (46292,"#688f95"),(46293,"#91d3dd"),(46294,"#688f95"),(46295,"#91d3dd"),
        (46296,"#7a4221"),(46297,"#ef8d56"),(46298,"#7a4221"),(46299,"#ef8d56"),
        (46300,"#7a4221"),(46301,"#ef8d56"),(46302,"#7a4221"),(46303,"#ef8d56"),
        (46304,"#365283"),(46305,"#61a7e2"),(46306,"#365283"),(46307,"#61a7e2"),
        (46308,"#365283"),(46309,"#61a7e2"),(46310,"#365283"),(46311,"#61a7e2"),
        (46312,"#c0837e"),(46313,"#6c2118"),(46314,"#c0837e"),(46315,"#6c2118"),
        (46316,"#c0837e"),(46317,"#6c2118"),(46318,"#c0837e"),(46319,"#6c2118"),
        (46675,"#8e8769"),(46676,"#589751"),(46677,"#ecc05f"),(46678,"#cd370c"),
        (46679,"#13bb41"),(46680,"#2ce3e8"),(46681,"#785a39"),(46682,"#878162"),
        (46683,"#d39f73"),(46684,"#ecdd86"),(46685,"#8a6847"),(46686,"#c9bb87"),
        (46687,"#8d785d"),(46688,"#9f886a"),(46689,"#d8b181"),
        (49787,"#296175"),(49789,"#185b79"),(50015,"#664c37"),
        (52306,"#a2552b"),(52315,"#476f75"),(52316,"#c1a25d"),
        (56625,"#a2552b"),(56626,"#a2552b"),(56627,"#c1a25d"),(56628,"#c1a25d"),
        (56629,"#476f75"),(56630,"#476f75"),(60771,"#8e301b"),(72935,"#d8b181"),
        (74521,"#d8b181"),(74522,"#d8b181"),(74523,"#d8b181"),(74524,"#d8b181"),
        (74525,"#ecdd86"),(74526,"#ecdd86"),(74527,"#ecdd86"),(74528,"#ecdd86"),
        (74529,"#cd370c"),(74530,"#cd370c"),(74531,"#cd370c"),(74532,"#cd370c"),
        (74533,"#9f886a"),(74534,"#9f886a"),(74535,"#9f886a"),(74536,"#9f886a"),
        (76373,"#476f75"),(77118,"#296175"),(77418,"#8f3d10"),(77419,"#83250d"),
        (77420,"#8f5d27"),(77421,"#83260d"),(77524,"#dcc280"),
        (81900,"#13bb41"),(81901,"#13bb41"),(81902,"#13bb41"),(81903,"#13bb41"),
        (81975,"#ecdd86"),(81976,"#ecdd86"),(81977,"#ecdd86"),(81978,"#ecdd86"),
        (82016,"#2ce3e8"),(82017,"#2ce3e8"),(82018,"#2ce3e8"),(82019,"#2ce3e8"),
        (82163,"#9f886a"),(82164,"#9f886a"),(82165,"#9f886a"),(82166,"#9f886a"),
        (82205,"#cd370c"),(82206,"#cd370c"),(82207,"#cd370c"),(82208,"#cd370c"),
        (88105,"#0ae9d1"),
        # Compressed gas (match uncompressed icon-extracted colors)
        (62377,"#f0d3af"),(62379,"#7d95d1"),(62380,"#bc7b9f"),(62381,"#e5cb92"),
        (62382,"#adab63"),(62383,"#a4bc7e"),(62384,"#eed3d3"),(62385,"#a9eced"),
        (62396,"#f0d3af"),(62386,"#7d95d1"),(62387,"#bc7b9f"),(62390,"#e5cb92"),
        (62391,"#adab63"),(62392,"#a4bc7e"),(62393,"#eed3d3"),(62394,"#a9eced"),
        (62388,"#bc7b9f"),(62389,"#f0d3af"),
        (62399,"#eed3d3"),(62397,"#eed3d3"),(62398,"#7d95d1"),(62403,"#a4bc7e"),
        (62400,"#adab63"),(62402,"#adab63"),(62404,"#f0d3af"),(62406,"#e5cb92"),
        (62405,"#e5cb92"),
    ]
    conn.executemany("INSERT OR REPLACE INTO ore_colors VALUES (?,?)", rows)
    conn.commit()
    log(f"  {len(rows)} ore colors")


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
        CREATE INDEX IF NOT EXISTS idx_typeMaterials_typeid ON typeMaterials(typeid);
        CREATE INDEX IF NOT EXISTS idx_typeMaterials_output ON typeMaterials(output_material);
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
    insert_celestial_names(conn, sde_dir)
    insert_stations(conn, sde_dir)
    militia_map = insert_factions(conn, sde_dir)
    insert_npc_corporations(conn, sde_dir, militia_map)
    insert_agents(conn, sde_dir)
    insert_divisions(conn, sde_dir)
    insert_planet_schematics(conn, sde_dir)
    insert_type_materials(conn, sde_dir)
    insert_blueprints(conn, sde_dir)
    insert_dynamic_items(conn, sde_dir)
    insert_ore_colors(conn)
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
