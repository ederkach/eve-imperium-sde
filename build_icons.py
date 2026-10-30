#!/usr/bin/env python3
"""Build icons.zip (type, blueprint-copy, item and category icons) from the EVE client resources."""

import argparse
import hashlib
import io
import json
import os
import shutil
import sys
import tempfile
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import requests
from PIL import Image

BINARIES = "https://binaries.eveonline.com"
RESOURCES = "https://resources.eveonline.com"
SDE_URL = "https://developers.eveonline.com/static-data/eve-online-static-data-latest-jsonl.zip"
USER_AGENT = "EveImperium-SDE/1.0 (icon-builder)"
PNG_MAGIC = b"\x89PNG\r\n\x1a\n"

SKIN_GROUPS = {1950, 1951, 1952, 1953, 1954, 1955, 4040}
SKIN_CATEGORIES = {91, 30, 2118}
BLUEPRINT_CATEGORIES = {9, 34}
RELIC_CATEGORY = 34
ICON_GROUPS = {12, 340, 448, 479, 548, 649, 711, 4168}
REACTION_GROUPS = {1888, 1889, 1890, 4097}

TECH_OVERLAYS = {
    2: "res:/ui/texture/icons/73_16_242.png",
    3: "res:/ui/texture/icons/73_16_245.png",
    4: "res:/ui/texture/icons/73_16_246.png",
    5: "res:/ui/texture/icons/73_16_248.png",
    6: "res:/ui/texture/icons/73_16_247.png",
    14: "res:/ui/texture/icons/73_16_243.png",
    15: "res:/ui/texture/icons/itemoverlay/abyssal.png",
    17: "res:/ui/texture/icons/itemoverlay/nes.png",
    19: "res:/ui/texture/icons/itemoverlay/timelimited.png",
    52: "res:/ui/texture/shared/structureoverlayfaction.png",
    53: "res:/ui/texture/shared/structureoverlayt2.png",
    54: "res:/ui/texture/shared/structureoverlay.png",
}

FRAME = "res:/ui/texture/icons/"
RELIC_FRAME = (FRAME + "relic.png", FRAME + "relic_overlay.png")
REACTION_FRAME = (FRAME + "reaction.png", FRAME + "bpo_overlay.png")
BPO_FRAME = (FRAME + "bpo.png", FRAME + "bpo_overlay.png")
BPC_FRAME = (FRAME + "bpc.png", FRAME + "bpc_overlay.png")

CATEGORY_SOURCES = {
    0: "res:/ui/texture/icons/7_64_4.png", 1: "res:/ui/texture/icons/70_128_11.png", 2: 6, 3: 1932, 4: 34,
    5: 29668, 6: "res:/ui/texture/icons/26_64_2.png", 7: "res:/ui/texture/icons/2_64_11.png",
    8: "res:/ui/texture/icons/5_64_2.png", 9: 1002, 10: "res:/ui/texture/icons/6_64_3.png",
    11: "res:/ui/texture/icons/26_64_10.png", 14: "res:/ui/texture/icons/modules/fleetboost_infobase.png",
    16: 2403, 17: 11068, 18: 2454, 20: "res:/ui/texture/icons/40_64_16.png", 22: 33475, 23: 17174,
    24: "res:/ui/texture/icons/comprfuel_amarr.png", 25: "res:/ui/texture/icons/inventory/moonasteroid_r4.png",
    30: "res:/ui/texture/icons/inventory/cratexvishirt.png", 32: "res:/ui/texture/icons/76_64_7.png",
    34: 30752, 35: "res:/ui/texture/icons/55_64_11.png", 39: "res:/ui/texture/icons/95_64_6.png", 40: 32458,
    41: 2409, 42: "res:/ui/texture/icons/97_64_10.png", 43: "res:/ui/texture/icons/99_64_8.png", 46: 2233,
    63: 19658, 65: 40340, 66: 35923, 87: 23061, 91: "res:/ui/texture/icons/rewardtrack/crateskincontainer.png",
    2100: 57203, 2118: 83291, 2143: 81143,
}


def norm(path):
    return path.replace("\\", "/").lower()


def parse_index(text):
    index = {}
    for line in text.splitlines():
        parts = line.strip().split(",")
        if len(parts) >= 3:
            index[norm(parts[0])] = (parts[1], parts[2])
    return index


class Client:
    def __init__(self, cache_dir):
        self.cache_dir = cache_dir
        self.session = requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT
        self.downloads = 0

    def get(self, url):
        for attempt in range(5):
            try:
                r = self.session.get(url, timeout=30)
                r.raise_for_status()
                return r.content
            except requests.RequestException as e:
                if attempt == 4:
                    raise
                print(f"  retry {url}: {e}")
                time.sleep(3)

    def cached(self, base_url, rel):
        path = os.path.join(self.cache_dir, rel)
        if not os.path.exists(path):
            data = self.get(f"{base_url}/{rel}")
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            tmp = path + ".part"
            with open(tmp, "wb") as f:
                f.write(data)
            os.replace(tmp, path)
            self.downloads += 1
        return path


class Resources:
    def __init__(self, client):
        self.client = client
        info = json.loads(client.get(f"{BINARIES}/eveclient_TQ.json"))
        if info.get("protected"):
            sys.exit("eveclient_TQ.json is protected: no public client build available")
        self.build = info.get("build_number") or info.get("buildNumber")
        self.app_index_name = f"eveonline_{self.build}.txt"
        with open(client.cached(BINARIES, self.app_index_name), encoding="utf-8") as f:
            self.app = parse_index(f.read())
        rel = self.app[norm("app:/resfileindex.txt")][0]
        with open(client.cached(BINARIES, rel), encoding="utf-8") as f:
            self.res = parse_index(f.read())

    def has(self, path):
        return norm(path) in self.res

    def hash(self, path):
        return self.res[norm(path)][1]

    def file(self, path):
        return self.client.cached(RESOURCES, self.res[norm(path)][0])

    def referenced(self):
        refs = {self.app_index_name}
        refs.update(rel for rel, _ in self.app.values())
        refs.update(rel for rel, _ in self.res.values())
        return {os.path.normpath(r) for r in refs}


def load_sde(zip_path):
    def rows(name):
        with zf.open(name) as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)

    with zipfile.ZipFile(zip_path) as zf:
        sde = {
            "build": next((r.get("buildNumber") for r in rows("_sde.jsonl") if r["_key"] == "sde"), None),
            "types": {r["_key"]: r for r in rows("types.jsonl")},
            "groups": {r["_key"]: r.get("categoryID") for r in rows("groups.jsonl")},
            "icons": {r["_key"]: r["iconFile"] for r in rows("icons.jsonl") if r.get("iconFile")},
            "folders": {r["_key"]: r["iconFolder"].rstrip("/") for r in rows("graphics.jsonl") if r.get("iconFolder")},
        }
        skins = {r["_key"]: r.get("skinMaterialID") for r in rows("skins.jsonl")}
        sde["skin_materials"] = {r["_key"]: skins.get(r.get("skinID")) for r in rows("skinLicenses.jsonl")}
    return sde


def plain_or_tech(base, meta, has):
    overlay = TECH_OVERLAYS.get(meta or 1)
    if overlay and has(overlay):
        return ("tech", base, overlay)
    return ("copy", base)


def frame(frame_pair, art, meta, has):
    overlay = TECH_OVERLAYS.get(meta or 1)
    return ("frame", frame_pair[0], art, frame_pair[1], overlay if overlay and has(overlay) else None)


def plan_types(sde, has, skip_skins, log=print):
    """Return ({entry_name: job}, [blueprint type IDs that fell through from graphic art to icon art])."""
    jobs, fallthrough = {}, []
    for tid, t in sorted(sde["types"].items()):
        gid, grp, iid = t.get("graphicID"), t.get("groupID"), t.get("iconID")
        if not (gid or iid or grp in SKIN_GROUPS):
            continue
        cat = sde["groups"].get(grp)
        if cat is None:
            continue
        if not (iid or gid) and cat != 91:
            continue
        if skip_skins and cat in SKIN_CATEGORIES:
            continue
        meta = t.get("metaGroupID")
        folder = sde["folders"].get(gid)
        icon = sde["icons"].get(iid)
        main = f"type_{tid}_64.png"
        copy = f"type_{tid}_bpc_64.png"

        if cat in BLUEPRINT_CATEGORIES:
            if folder:
                bp, bpc = f"{folder}/{gid}_64_bp.png", f"{folder}/{gid}_64_bpc.png"
                if has(bp) and grp not in ICON_GROUPS:
                    jobs[main] = plain_or_tech(bp, meta, has)
                    if has(bpc):
                        jobs[copy] = plain_or_tech(bpc, meta, has)
                    continue
            if icon and has(icon):
                if folder:
                    fallthrough.append(tid)
                if cat == RELIC_CATEGORY:
                    jobs[main] = frame(RELIC_FRAME, icon, meta, has)
                elif grp in REACTION_GROUPS:
                    jobs[main] = frame(REACTION_FRAME, icon, meta, has)
                else:
                    jobs[main] = frame(BPO_FRAME, icon, meta, has)
                    jobs[copy] = frame(BPC_FRAME, icon, meta, has)
            else:
                log(f"  missing blueprint art: type {tid}")
            continue

        base = None
        if folder:
            art = f"{folder}/{gid}_64.png"
            base = art if has(art) and grp not in ICON_GROUPS else icon
        elif icon:
            base = icon
        elif cat == 91:
            material = sde["skin_materials"].get(tid)
            if material is not None:
                base = f"res:/ui/texture/classes/skins/icons/{material}.png"
        if base and has(base):
            jobs[main] = plain_or_tech(base, meta, has)
        elif base:
            log(f"  missing art {base}: type {tid}")
    return jobs, fallthrough


def plan_items(sde, has):
    jobs = {}
    for _, path in sorted(sde["icons"].items()):
        if path.lower().endswith(".png") and has(path):
            jobs.setdefault("Icons/items/" + path.replace("\\", "/").rsplit("/", 1)[-1], ("copy", path))
    return jobs


def open_rgba(path, size=None):
    with Image.open(path) as im:
        im = im.convert("RGBA")
    return im.resize(size, Image.LANCZOS) if size and im.size != size else im


def compose_tech(base, overlay):
    im = open_rgba(base, (64, 64))
    ov = open_rgba(overlay, (16, 16))
    im.paste(ov, (0, 0), ov)
    return im


def add_overlay(im, overlay):
    a = np.asarray(im, dtype=np.float64)
    b = np.asarray(overlay, dtype=np.float64)
    a1, a2 = a[..., 3:] / 255.0, b[..., 3:] / 255.0
    out = np.empty_like(a)
    out[..., :3] = a[..., :3] + b[..., :3] * a2
    out[..., 3:] = 255.0 * (a1 + a2 * (1.0 - a1))
    return Image.fromarray(np.clip(out, 0, 255).astype(np.uint8), "RGBA")


def compose_frame(background, art, overlay, tech):
    im = open_rgba(background)
    item = open_rgba(art, (64, 64))
    im.paste(item, (0, 0), item)
    im = add_overlay(im, open_rgba(overlay, im.size))
    if tech:
        ov = open_rgba(tech, (16, 16))
        im.paste(ov, (0, 0), ov)
    return im


def png_bytes(path):
    with open(path, "rb") as f:
        data = f.read()
    if data.startswith(PNG_MAGIC):
        return data
    buf = io.BytesIO()
    open_rgba(path).save(buf, "PNG")
    return buf.getvalue()


def work_name(job, res):
    key = "|".join([job[0]] + [res.hash(p) if p else "-" for p in job[1:]])
    return hashlib.sha1(key.encode()).hexdigest() + ".png"


def remove_unreferenced(root, keep):
    removed = 0
    for dirpath, _, files in os.walk(root, topdown=False):
        for name in files:
            path = os.path.join(dirpath, name)
            if os.path.normpath(os.path.relpath(path, root)) not in keep:
                os.remove(path)
                removed += 1
        if dirpath != root and not os.listdir(dirpath):
            os.rmdir(dirpath)
    return removed


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default="icons.zip")
    ap.add_argument("--cache-dir", default="./icon_cache")
    ap.add_argument("--icon-dir", default="./icon_work")
    ap.add_argument("--sde-zip", help="already downloaded SDE JSONL zip (downloaded if omitted)")
    ap.add_argument("--skip-skins", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--workers", type=int, default=16)
    args = ap.parse_args()

    os.makedirs(args.cache_dir, exist_ok=True)
    os.makedirs(args.icon_dir, exist_ok=True)
    if any(n.lower() in ("updater.exe", "tq") for n in os.listdir(args.cache_dir)):
        sys.exit(f"{args.cache_dir} looks like an EVE game install; refusing to use it as a cache")

    client = Client(args.cache_dir)
    res = Resources(client)
    print(f"EVE client build {res.build}: {len(res.res)} resources")

    tmp_dir = None
    sde_zip = args.sde_zip
    if not sde_zip:
        tmp_dir = tempfile.mkdtemp()
        sde_zip = os.path.join(tmp_dir, "sde.zip")
        print("Downloading SDE")
        with open(sde_zip, "wb") as f:
            f.write(client.get(SDE_URL))
    sde = load_sde(sde_zip)
    if tmp_dir:
        shutil.rmtree(tmp_dir)
    print(f"SDE build {sde['build']}: {len(sde['types'])} types")

    type_jobs, fallthrough = plan_types(sde, res.has, args.skip_skins)
    item_jobs = plan_items(sde, res.has)
    if fallthrough:
        print(f"{len(fallthrough)} blueprints use icon art because their graphic has no _64_bp.png: "
              + " ".join(map(str, fallthrough)))

    manifest_path = os.path.join(args.icon_dir, "manifest.json")
    previous = set()
    if os.path.exists(manifest_path) and not args.force:
        with open(manifest_path) as f:
            previous = set(json.load(f))

    built = {}
    to_build = {}
    for job in type_jobs.values():
        if job[0] != "copy":
            name = work_name(job, res)
            built[job] = name
            if name not in previous or not os.path.exists(os.path.join(args.icon_dir, name)):
                to_build[name] = job

    needed = {p for job in to_build.values() for p in job[1:] if p}
    needed.update(job[1] for job in list(type_jobs.values()) + list(item_jobs.values()) if job[0] == "copy")
    needed.update(p for p in CATEGORY_SOURCES.values() if isinstance(p, str) and res.has(p))
    with ThreadPoolExecutor(args.workers) as pool:
        list(pool.map(res.file, sorted(needed)))
    print(f"Downloaded {client.downloads} files, building {len(to_build)} of {len(built)} composites")

    for name, job in to_build.items():
        if job[0] == "tech":
            im = compose_tech(res.file(job[1]), res.file(job[2]))
        else:
            im = compose_frame(*(res.file(p) if p else None for p in job[1:]))
        path = os.path.join(args.icon_dir, name)
        im.save(path + ".part", "PNG")
        os.replace(path + ".part", path)

    def job_bytes(job):
        if job[0] == "copy":
            return png_bytes(res.file(job[1]))
        with open(os.path.join(args.icon_dir, built[job]), "rb") as f:
            return f.read()

    tmp_out = args.out + ".part"
    with zipfile.ZipFile(tmp_out, "w", zipfile.ZIP_STORED) as zf:
        for name, job in list(type_jobs.items()) + list(item_jobs.items()):
            zf.writestr(name, job_bytes(job))
        for cat, src in sorted(CATEGORY_SOURCES.items()):
            if isinstance(src, int):
                job = type_jobs.get(f"type_{src}_64.png")
                if job is None:
                    print(f"  warning: category {cat} icon source type {src} has no icon")
                    continue
                data = job_bytes(job)
            elif res.has(src):
                data = png_bytes(res.file(src))
            else:
                print(f"  warning: category {cat} icon {src} not in resources")
                continue
            zf.writestr(f"category_{cat}.png", data)
        count = len(zf.infolist())
    os.replace(tmp_out, args.out)

    if "type_10000_64.png" not in type_jobs:
        sys.exit("type_10000_64.png missing: the app uses it as the icons-installed marker")

    names = set(built.values())
    with open(manifest_path, "w") as f:
        json.dump(sorted(names), f)
    stale = remove_unreferenced(args.icon_dir, names | {"manifest.json"})
    pruned = remove_unreferenced(args.cache_dir, res.referenced())
    print(f"Wrote {args.out}: {count} entries, {os.path.getsize(args.out)} bytes; "
          f"removed {stale} stale icons and {pruned} stale cache files")


if __name__ == "__main__":
    main()
