import hashlib
import json
import os
import shutil
from enum import Enum
from pathlib import Path
from typing import Dict, Set, Optional, Tuple
from zipfile import ZipFile, ZIP_STORED
from PIL import Image, ImageChops
import numpy as np

from .cache import SharedCache
from .sde import TypeInfo


REACTION_GROUPS = [1888, 1889, 1890, 4097]

USE_ICON_INSTEAD_OF_GRAPHIC_GROUPS = [12, 340, 448, 479, 548, 649, 711, 4168]


class IconKind(Enum):
    ICON = "icon"
    BLUEPRINT = "bp"
    BLUEPRINT_COPY = "bpc"
    REACTION = "reaction"
    RELIC = "relic"
    RENDER = "render"


class IconError(Exception):
    pass


def techicon_resource_for_metagroup(metagroup_id: int) -> Optional[str]:
    mapping = {
        1: None,
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
    return mapping.get(metagroup_id)


def composite_tech(icon_path: Path, tech_icon_path: Path, out_path: Path):
    image = Image.open(icon_path).convert('RGBA')
    image = image.resize((64, 64), Image.Resampling.LANCZOS)

    tech_overlay = Image.open(tech_icon_path).convert('RGBA')
    tech_overlay = tech_overlay.resize((16, 16), Image.Resampling.LANCZOS)

    image.paste(tech_overlay, (0, 0), tech_overlay)
    image.save(out_path, 'PNG')


def composite_blueprint(background_path: Path, overlay_path: Path, icon_path: Path,
                       tech_icon_path: Optional[Path], out_path: Path):
    background = Image.open(background_path).convert('RGBA')

    icon = Image.open(icon_path).convert('RGBA')
    icon = icon.resize((64, 64), Image.Resampling.LANCZOS)

    background.paste(icon, (0, 0), icon)

    overlay = Image.open(overlay_path).convert('RGBA')
    background = image_add(background, overlay)

    if tech_icon_path:
        tech_overlay = Image.open(tech_icon_path).convert('RGBA')
        tech_overlay = tech_overlay.resize((16, 16), Image.Resampling.LANCZOS)
        background.paste(tech_overlay, (0, 0), tech_overlay)

    background.save(out_path, 'PNG')


def image_add(img1: Image.Image, img2: Image.Image) -> Image.Image:
    """Additive blend (like Photoshop Add mode).
    Only adds RGB where img2 has opacity.
    """
    arr1 = np.array(img1, dtype=np.float32)
    arr2 = np.array(img2, dtype=np.float32)

    rgb1 = arr1[:, :, :3]
    alpha1 = arr1[:, :, 3]
    rgb2 = arr2[:, :, :3]
    alpha2 = arr2[:, :, 3]

    alpha1_norm = alpha1 / 255.0
    alpha2_norm = alpha2 / 255.0

    result_rgb = rgb1 + rgb2 * (alpha2_norm[:, :, np.newaxis])
    result_rgb = np.clip(result_rgb, 0, 255)

    result_alpha_norm = alpha1_norm + alpha2_norm * (1.0 - alpha1_norm)
    result_alpha = np.clip(result_alpha_norm * 255.0, 0, 255)

    result = np.dstack([result_rgb, result_alpha]).astype(np.uint8)

    return Image.fromarray(result, 'RGBA')


def copy_or_convert(from_path: Path, to_path: Path, resource: str, extension: str):
    if resource.endswith(extension):
        shutil.copy(from_path, to_path)
    else:
        img = Image.open(from_path)
        if extension == '.png':
            img.save(to_path, 'PNG')
        elif extension in ['.jpg', '.jpeg']:
            img.save(to_path, 'JPEG')
        else:
            raise ValueError(f"Unknown image extension: {extension}")


class IconBuildData:
    def __init__(self, types: Dict[int, TypeInfo], group_categories: Dict[int, int],
                 icon_files: Dict[int, str], graphics_folders: Dict[int, str],
                 skin_materials: Dict[int, int]):
        self.types = types
        self.group_categories = group_categories
        self.icon_files = icon_files
        self.graphics_folders = graphics_folders
        self.skin_materials = skin_materials


def build_icon_export(output_mode: str, skip_output_if_fresh: bool, data: IconBuildData,
                     cache: SharedCache, icon_dir: Path, force_rebuild: bool,
                     silent_mode: bool, log_file=None, out=None, show_progress: bool = True,
                     skip_skins: bool = False, test_type_id: Optional[int] = None,
                     skip_renders: bool = False, skip_bpc: bool = False,
                     **output_params) -> Tuple[int, int]:
    icon_dir.mkdir(parents=True, exist_ok=True)

    old_index = set()
    index_path = icon_dir / "cache.csv"
    if index_path.exists():
        content = index_path.read_bytes()
        for entry in content.split(b'\x1E'):
            if entry:
                old_index.add(entry.decode('utf-8'))

    service_metadata: Dict[int, Dict[IconKind, str]] = {}
    new_index: Set[str] = set()

    def is_up_to_date(filename: str) -> bool:
        new_index.add(filename)
        return filename in old_index and not force_rebuild

    processable_types = []
    for type_id, type_info in data.types.items():
        if test_type_id is not None and type_id != test_type_id:
            continue

        category_id = data.group_categories.get(type_info.group_id)
        if category_id is None:
            if not silent_mode:
                print(f"\t[!] Group has no category: {type_info.group_id}")
            continue

        if type_info.icon_id is None and type_info.graphic_id is None and category_id != 91:
            continue

        if skip_skins and category_id in [91, 30, 2118]:
            continue

        processable_types.append((type_id, type_info, category_id))

    total_count = len(processable_types)
    processed_count = 0

    for type_id, type_info, category_id in processable_types:
        processed_count += 1

        if show_progress and not silent_mode and processed_count % 500 == 0:
            percentage = (processed_count / total_count) * 100
            print(f"\tProgress: {processed_count}/{total_count} ({percentage:.1f}%)")
            if log_file:
                log_file.write(f"\tProgress: {processed_count}/{total_count} ({percentage:.1f}%)\n")

        if category_id == 9 or category_id == 34:
            _process_blueprint(type_id, type_info, category_id, data, cache, icon_dir,
                             is_up_to_date, service_metadata, silent_mode, log_file)
        else:
            _process_regular_item(type_id, type_info, category_id, data, cache, icon_dir,
                                is_up_to_date, service_metadata, silent_mode, log_file)

    if show_progress and not silent_mode:
        print(f"\tComplete: {processed_count}/{total_count} (100.0%)")
        if log_file:
            log_file.write(f"\tComplete: {processed_count}/{total_count} (100.0%)\n")

    index_bytes = b'\x1E'.join(filename.encode('utf-8') for filename in sorted(new_index))
    index_path.write_bytes(index_bytes)

    to_remove = [f for f in old_index if f not in new_index]
    to_add = [f for f in new_index if f not in old_index]

    if len(to_add) == 0 and len(to_remove) == 0 and skip_output_if_fresh:
        if not silent_mode:
            print("Icons unchanged, skipping output...")
        if log_file:
            log_file.write("Icons unchanged, skipping output...\n")
    else:
        if not silent_mode:
            print("Icons built, generating output...")
        if log_file:
            log_file.write("Icons built, generating output...\n")

        if out and 'out' not in output_params:
            output_params['out'] = out

        _generate_output(output_mode, output_params, icon_dir, new_index,
                        service_metadata, old_index, force_rebuild, silent_mode, log_file, cache, data,
                        skip_renders=skip_renders, skip_bpc=skip_bpc)

    for filename in to_remove:
        try:
            (icon_dir / filename).unlink()
        except Exception:
            pass

    return len(to_add), len(to_remove)


def _process_blueprint(type_id: int, type_info: TypeInfo, category_id: int,
                      data: IconBuildData, cache: SharedCache, icon_dir: Path,
                      is_up_to_date, service_metadata: Dict, silent_mode: bool, log_file):
    if type_info.graphic_id and type_info.graphic_id in data.graphics_folders:
        folder = data.graphics_folders[type_info.graphic_id].rstrip('/')
        icon_resource_bp = f"{folder}/{type_info.graphic_id}_64_bp.png"
        icon_resource_bpc = f"{folder}/{type_info.graphic_id}_64_bpc.png"

        if cache.has_resource(icon_resource_bp) and type_info.group_id not in USE_ICON_INSTEAD_OF_GRAPHIC_GROUPS:
            techicon = techicon_resource_for_metagroup(type_info.meta_group_id or 1)

            if techicon:
                filename = f"bp;{cache.hash_of(icon_resource_bp)};{cache.hash_of(techicon)}.png"
                service_metadata.setdefault(type_id, {})[IconKind.ICON] = filename
                service_metadata[type_id][IconKind.BLUEPRINT] = filename

                if not is_up_to_date(filename):
                    composite_tech(cache.path_of(icon_resource_bp),
                                 cache.path_of(techicon),
                                 icon_dir / filename)

                if cache.has_resource(icon_resource_bpc):
                    filename = f"bpc;{cache.hash_of(icon_resource_bpc)};{cache.hash_of(techicon)}.png"
                    service_metadata[type_id][IconKind.BLUEPRINT_COPY] = filename

                    if not is_up_to_date(filename):
                        composite_tech(cache.path_of(icon_resource_bpc),
                                     cache.path_of(techicon),
                                     icon_dir / filename)
            else:
                filename = f"bp;{cache.hash_of(icon_resource_bp)}.png"
                service_metadata.setdefault(type_id, {})[IconKind.ICON] = filename
                service_metadata[type_id][IconKind.BLUEPRINT] = filename

                if not is_up_to_date(filename):
                    copy_or_convert(cache.path_of(icon_resource_bp), icon_dir / filename,
                                  icon_resource_bp, '.png')

                if cache.has_resource(icon_resource_bpc):
                    filename = f"bpc;{cache.hash_of(icon_resource_bpc)}.png"
                    service_metadata[type_id][IconKind.BLUEPRINT_COPY] = filename

                    if not is_up_to_date(filename):
                        copy_or_convert(cache.path_of(icon_resource_bpc), icon_dir / filename,
                                      icon_resource_bpc, '.png')

    elif type_info.icon_id and type_info.icon_id in data.icon_files:
        icon_resource = data.icon_files[type_info.icon_id]

        if cache.has_resource(icon_resource):
            tech_overlay = techicon_resource_for_metagroup(type_info.meta_group_id or 1)
            tech_hash = cache.hash_of(tech_overlay) if tech_overlay else ""

            if category_id == 34:
                filename = f"relic;{cache.hash_of(icon_resource)};{tech_hash}.png"
                service_metadata.setdefault(type_id, {})[IconKind.ICON] = filename
                service_metadata[type_id][IconKind.RELIC] = filename

                if not is_up_to_date(filename):
                    composite_blueprint(
                        cache.path_of("res:/ui/texture/icons/relic.png"),
                        cache.path_of("res:/ui/texture/icons/relic_overlay.png"),
                        cache.path_of(icon_resource),
                        cache.path_of(tech_overlay) if tech_overlay else None,
                        icon_dir / filename
                    )

            elif type_info.group_id in REACTION_GROUPS:
                filename = f"reaction;{cache.hash_of(icon_resource)};{tech_hash}.png"
                service_metadata.setdefault(type_id, {})[IconKind.ICON] = filename
                service_metadata[type_id][IconKind.REACTION] = filename
                service_metadata[type_id][IconKind.BLUEPRINT] = filename

                if not is_up_to_date(filename):
                    composite_blueprint(
                        cache.path_of("res:/ui/texture/icons/reaction.png"),
                        cache.path_of("res:/ui/texture/icons/bpo_overlay.png"),
                        cache.path_of(icon_resource),
                        cache.path_of(tech_overlay) if tech_overlay else None,
                        icon_dir / filename
                    )

            else:
                filename = f"bp;{cache.hash_of(icon_resource)};{tech_hash}.png"
                service_metadata.setdefault(type_id, {})[IconKind.ICON] = filename
                service_metadata[type_id][IconKind.BLUEPRINT] = filename

                if not is_up_to_date(filename):
                    composite_blueprint(
                        cache.path_of("res:/ui/texture/icons/bpo.png"),
                        cache.path_of("res:/ui/texture/icons/bpo_overlay.png"),
                        cache.path_of(icon_resource),
                        cache.path_of(tech_overlay) if tech_overlay else None,
                        icon_dir / filename
                    )

                filename = f"bpc;{cache.hash_of(icon_resource)};{tech_hash}.png"
                service_metadata[type_id][IconKind.BLUEPRINT_COPY] = filename

                if not is_up_to_date(filename):
                    composite_blueprint(
                        cache.path_of("res:/ui/texture/icons/bpc.png"),
                        cache.path_of("res:/ui/texture/icons/bpc_overlay.png"),
                        cache.path_of(icon_resource),
                        cache.path_of(tech_overlay) if tech_overlay else None,
                        icon_dir / filename
                    )
        else:
            if not silent_mode:
                print(f"\t[x] Missing icon: {type_id}")
            if log_file:
                log_file.write(f"\t[x] Missing icon: {type_id}\n")


def _process_regular_item(type_id: int, type_info: TypeInfo, category_id: int,
                         data: IconBuildData, cache: SharedCache, icon_dir: Path,
                         is_up_to_date, service_metadata: Dict, silent_mode: bool, log_file):
    icon_resource = None

    if type_info.graphic_id and type_info.graphic_id in data.graphics_folders:
        folder = data.graphics_folders[type_info.graphic_id].rstrip('/')
        icon_resource = f"{folder}/{type_info.graphic_id}_64.png"

        if not cache.has_resource(icon_resource) or type_info.group_id in USE_ICON_INSTEAD_OF_GRAPHIC_GROUPS:
            if type_info.icon_id and type_info.icon_id in data.icon_files:
                icon_resource = data.icon_files[type_info.icon_id]
            else:
                return

        render_resource = f"{folder}/{type_info.graphic_id}_512.jpg"
        if cache.has_resource(render_resource):
            filename = f"{cache.hash_of(render_resource)}.jpg"
            service_metadata.setdefault(type_id, {})[IconKind.RENDER] = filename

            if not is_up_to_date(filename):
                copy_or_convert(cache.path_of(render_resource), icon_dir / filename,
                              render_resource, '.jpg')

    elif type_info.icon_id and type_info.icon_id in data.icon_files:
        icon_resource = data.icon_files[type_info.icon_id]

    elif category_id == 91:
        if type_id in data.skin_materials:
            material_id = data.skin_materials[type_id]
            icon_resource = f"res:/ui/texture/classes/skins/icons/{material_id}.png"
        else:
            return
    else:
        return

    if icon_resource and cache.has_resource(icon_resource):
        techicon = techicon_resource_for_metagroup(type_info.meta_group_id or 1)

        if techicon:
            filename = f"{cache.hash_of(icon_resource)};{cache.hash_of(techicon)}.png"
            service_metadata.setdefault(type_id, {})[IconKind.ICON] = filename

            if not is_up_to_date(filename):
                composite_tech(cache.path_of(icon_resource),
                             cache.path_of(techicon),
                             icon_dir / filename)
        else:
            filename = f"{cache.hash_of(icon_resource)}.png"
            service_metadata.setdefault(type_id, {})[IconKind.ICON] = filename

            if not is_up_to_date(filename):
                copy_or_convert(cache.path_of(icon_resource), icon_dir / filename,
                              icon_resource, '.png')
    else:
        if not silent_mode:
            print(f"\t[x] Missing icon: {type_id}")
        if log_file:
            log_file.write(f"\t[x] Missing icon: {type_id}\n")


def _generate_output(output_mode: str, output_params: dict, icon_dir: Path,
                    new_index: Set[str], service_metadata: Dict,
                    old_index: Set[str], force_rebuild: bool, silent_mode: bool, log_file,
                    cache: SharedCache, data: IconBuildData,
                    skip_renders: bool = False, skip_bpc: bool = False):
    if output_mode == 'service_bundle':
        out_path = Path(output_params['out'])
        out_path.parent.mkdir(parents=True, exist_ok=True)

        with ZipFile(out_path, 'w', ZIP_STORED) as zf:
            for filename in new_index:
                zf.write(icon_dir / filename, filename)

            metadata_json = {}
            for type_id, icons in service_metadata.items():
                metadata_json[type_id] = {kind.value: filename for kind, filename in icons.items()}

            zf.writestr('service_metadata.json', json.dumps(metadata_json, indent=2))

    elif output_mode == 'iec':
        out_path = Path(output_params['out'])
        out_path.parent.mkdir(parents=True, exist_ok=True)

        with ZipFile(out_path, 'w', ZIP_STORED) as zf:
            for type_id, icons in service_metadata.items():
                for icon_kind, filename in icons.items():
                    if icon_kind == IconKind.ICON:
                        output_name = f"type_{type_id}_64.png"
                        zf.write(icon_dir / filename, output_name)
                    elif icon_kind == IconKind.BLUEPRINT_COPY and not skip_bpc:
                        output_name = f"type_{type_id}_bpc_64.png"
                        zf.write(icon_dir / filename, output_name)
                    elif icon_kind == IconKind.RENDER and not skip_renders:
                        output_name = f"type_{type_id}_512.jpg"
                        zf.write(icon_dir / filename, output_name)

    elif output_mode == 'web_dir':
        out_dir = Path(output_params['out'])
        out_dir.mkdir(parents=True, exist_ok=True)
        copy_files = output_params.get('copy_files', False)
        hard_link = output_params.get('hard_link', False)

        created_files = {}
        index_path = out_dir / 'index.json'

        old_links = {}
        if index_path.exists():
            with open(index_path, 'r') as f:
                old_links = json.load(f)

        for type_id, icons in service_metadata.items():
            json_name = f"{type_id}.json"
            json_content = json.dumps([kind.value for kind in icons.keys()])

            if force_rebuild or old_links.get(json_name) != json_content:
                (out_dir / json_name).write_text(json_content)

            created_files[json_name] = json_content

            for icon_kind, filename in icons.items():
                ext = 'jpg' if icon_kind == IconKind.RENDER else 'png'
                link_name = f"{type_id}_{icon_kind.value}.{ext}"
                link_source = (icon_dir / filename).resolve()
                link_file = (out_dir / link_name).resolve()

                if force_rebuild or old_links.get(link_name) != filename:
                    if copy_files:
                        shutil.copy(link_source, link_file)
                    elif hard_link:
                        if link_file.exists():
                            link_file.unlink()
                        os.link(link_source, link_file)
                    else:
                        if link_file.exists():
                            link_file.unlink()
                        os.symlink(link_source, link_file)

                created_files[link_name] = filename

        for entry in old_links.keys():
            if entry not in created_files:
                try:
                    (out_dir / entry).unlink()
                except Exception:
                    pass

        with open(index_path, 'w') as f:
            json.dump(created_files, f)

    elif output_mode == 'checksum':
        index_bytes = b'\x1E'.join(f.encode('utf-8') for f in sorted(new_index))
        checksum = hashlib.md5(index_bytes).hexdigest()

        out_path = output_params.get('out')
        if out_path:
            out_path = Path(out_path)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(checksum)
        else:
            print(checksum, end='')

    elif output_mode == 'aux_icons':
        out_path = Path(output_params['out'])
        out_path.parent.mkdir(parents=True, exist_ok=True)

        with ZipFile(out_path, 'w', ZIP_STORED) as zf:
            for icon_id, resource in data.icon_files.items():
                parts = resource.rsplit('.', 1)
                if len(parts) == 2:
                    extension = parts[1]
                else:
                    extension = resource.rsplit('/', 1)[-1]

                try:
                    resource_path = cache.path_of(resource)
                    zf.write(resource_path, f"{icon_id}.{extension}")
                except Exception:
                    pass

    elif output_mode == 'aux_all':
        out_path = Path(output_params['out'])
        out_path.parent.mkdir(parents=True, exist_ok=True)

        with ZipFile(out_path, 'w', ZIP_STORED) as zf:
            for resource in cache.iter_resources():
                if resource.endswith('png') or resource.endswith('jpg'):
                    parts = resource.split(':/', 1)
                    filename = parts[1] if len(parts) == 2 else resource

                    resource_path = cache.path_of(resource)
                    zf.write(resource_path, filename)
