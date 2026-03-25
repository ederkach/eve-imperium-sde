#!/usr/bin/env python3
import argparse
import sys
import time
from pathlib import Path
from datetime import datetime

from .cache import CacheDownloader, CacheError
from .sde import update_sde, read_types, read_group_categories, read_icons, read_graphics, read_skin_materials
from .icons import IconBuildData, build_icon_export, IconError


def main():
    parser = argparse.ArgumentParser(description='EVE Online icon generator')

    parser.add_argument('-u', '--user_agent', required=True, help='HTTP User-Agent')
    parser.add_argument('-c', '--cache_folder', default='./cache', help='Cache folder (default: ./cache)')
    parser.add_argument('-i', '--icon_folder', default='./icons', help='Icon output folder (default: ./icons)')
    parser.add_argument('-l', '--logfile', help='Log file path')
    parser.add_argument('--append_log', action='store_true', help='Append to log file')
    parser.add_argument('--silent', action='store_true', help='Silent mode')
    parser.add_argument('-f', '--force_rebuild', action='store_true', help='Force rebuild unchanged icons')
    parser.add_argument('-s', '--skip_if_fresh', action='store_true', help='Skip output if icons unchanged')
    parser.add_argument('--no_progress', action='store_true', help='Disable progress display')
    parser.add_argument('--skip_skins', action='store_true', help='Skip SKIN type icons')
    parser.add_argument('--test_type_id', type=int, help='Test mode: only build specified typeID')

    subparsers = parser.add_subparsers(dest='command', required=True, help='Output mode')

    service_bundle = subparsers.add_parser('service_bundle', help='Service hosting bundle (zip with metadata)')
    service_bundle.add_argument('-o', '--out', required=True, help='Output file')

    iec = subparsers.add_parser('iec', help='Image Export Collection (zip)')
    iec.add_argument('-o', '--out', required=True, help='Output file')

    web_dir = subparsers.add_parser('web_dir', help='Web hosting directory')
    web_dir.add_argument('-o', '--out', required=True, help='Output directory')
    web_dir.add_argument('--copy_files', action='store_true', help='Copy files instead of symlinks')
    web_dir.add_argument('--hardlink', action='store_true', help='Use hard links instead of symlinks')

    checksum = subparsers.add_parser('checksum', help='Print/write icon set checksum')
    checksum.add_argument('-o', '--out', help='Output file (prints to stdout if omitted)')

    aux_icons = subparsers.add_parser('aux_icons', help='Auxiliary icon dump (zip)')
    aux_icons.add_argument('-o', '--out', required=True, help='Output file')

    aux_all = subparsers.add_parser('aux_all', help='All images dump (zip)')
    aux_all.add_argument('-o', '--out', required=True, help='Output file')

    args = parser.parse_args()

    output_mode = args.command
    output_params = {}

    if output_mode == 'service_bundle':
        output_params['out'] = args.out
    elif output_mode == 'iec':
        output_params['out'] = args.out
    elif output_mode == 'web_dir':
        out_path = Path(args.out)
        if not out_path.exists():
            out_path.mkdir(parents=True, exist_ok=True)
        elif out_path.is_file():
            print(f"[x] Output must be a directory! ({args.out})")
            return 1
        output_params['out'] = args.out
        output_params['copy_files'] = args.copy_files
        output_params['hard_link'] = args.hardlink
    elif output_mode == 'checksum':
        output_params['out'] = args.out
    elif output_mode == 'aux_icons':
        output_params['out'] = args.out
    elif output_mode == 'aux_all':
        output_params['out'] = args.out

    silent_mode = args.silent or (output_mode == 'checksum' and not args.out)
    skip_if_fresh = args.skip_if_fresh and not (output_mode == 'checksum' and not args.out)

    log_file = None
    if args.logfile:
        mode = 'a' if args.append_log else 'w'
        log_file = open(args.logfile, mode, encoding='utf-8')
        log_file.write(f"Icon generation run, output: {output_mode} - {datetime.now()}\n")

    try:
        start_time = time.time()

        if not silent_mode:
            print(f"Initializing cache (UA:`{args.user_agent}`)")

        cache = CacheDownloader(Path(args.cache_folder), args.user_agent, use_macos_build=False)
        cache_init_duration = time.time() - start_time

        data_load_start = time.time()
        if not silent_mode:
            print("Loading SDE...")

        sde = update_sde(silent_mode)

        icon_build_data = IconBuildData(
            types=read_types(sde, silent_mode),
            group_categories=read_group_categories(sde, silent_mode),
            icon_files=read_icons(sde, silent_mode),
            graphics_folders=read_graphics(sde, silent_mode),
            skin_materials=read_skin_materials(sde, silent_mode)
        )

        sde.close()
        data_load_duration = time.time() - data_load_start

        if not silent_mode:
            print("Building icons...")

        build_start = time.time()
        added, removed = build_icon_export(
            output_mode=output_mode,
            skip_output_if_fresh=skip_if_fresh,
            data=icon_build_data,
            cache=cache,
            icon_dir=Path(args.icon_folder),
            force_rebuild=args.force_rebuild,
            silent_mode=silent_mode,
            log_file=log_file,
            show_progress=not args.no_progress,
            skip_skins=args.skip_skins,
            test_type_id=args.test_type_id,
            **output_params
        )
        build_duration = time.time() - build_start

        total_duration = time.time() - start_time

        s1 = f"Done in: {total_duration:.1f}s"
        s2 = f"\tCache init: {cache_init_duration:.1f}s"
        s3 = f"\tData load: {data_load_duration:.1f}s"
        s4 = f"\tImage build: {build_duration:.1f}s ({added} added, {removed} removed)"

        if not silent_mode:
            print(s1)
            print(s2)
            print(s3)
            print(s4)

        cache.purge(['sde.zip', 'checksum.txt'])

        return 0

    except (CacheError, IconError, Exception) as e:
        print(f"[x] Error: {e}", file=sys.stderr)
        if log_file:
            log_file.write(f"[x] Error: {e}\n")
        return 1
    finally:
        if log_file:
            log_file.close()


if __name__ == '__main__':
    sys.exit(main())
