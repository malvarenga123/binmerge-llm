#!/usr/bin/env python3
#
#  binmerge
#
#  Takes a cue sheet with multiple binary track files and merges them together,
#  generating a corrected cue sheet in the process.
#
#  Copyright (C) 2024 Chris Putnam
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#
#
#  Original GitHub: https://github.com/putnam/binmerge
#
import argparse
import re
import os
import sys
import textwrap
import traceback
import shlex
from pathlib import Path
from typing import List, Optional, Union

# Python 3.8+ Compatible
VERBOSE = False
VERSION_STRING = "1.1.0-LLM"

# Fields that MUST be quoted according to spec/convention
QUOTED_FIELDS = {
    'TITLE', 'PERFORMER', 'SONGWRITER', 'ARRANGER', 'COMPOSER', 
    'MESSAGE', 'UPC_EAN', 'CDTEXTFILE', 'FILE'
}

class ZeroBinFilesException(Exception): pass
class BinFilesMissingException(Exception): pass

def print_license():
    print(textwrap.dedent(f"""
    binmerge {VERSION_STRING}
    Copyright (C) 2024 Chris Putnam

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    Source code available at: https://github.com/putnam/binmerge
    """))

def log_debug(s: str):
    if VERBOSE: print(f"[DEBUG]\t{s}")

def log_error(s: str):
    print(f"[ERROR]\t{s}")

def log_warn(s: str):
    print(f"[WARN]\t{s}")

def log_info(s: str):
    print(f"[INFO]\t{s}")

# ==========================================
# Progress Bar Class
# ==========================================
class ProgressBar:
    def __init__(self, total_bytes: int, prefix: str = '', length: int = 30):
        self.total = total_bytes
        self.prefix = prefix
        self.length = length
        self.current = 0

    def update(self, inc: int):
        self.current += inc
        if self.total > 0:
            percent = 100 * (self.current / float(self.total))
            filled_length = int(self.length * self.current // self.total)
        else:
            percent = 100
            filled_length = self.length
            
        bar = '#' * filled_length + '-' * (self.length - filled_length)
        sys.stdout.write(f'\r{self.prefix} |{bar}| {percent:.1f}% ({self.current // (1024*1024)}/{self.total // (1024*1024)} MB)')
        sys.stdout.flush()

    def finish(self):
        sys.stdout.write('\n')

# ==========================================
# Data Classes
# ==========================================
class CueLine:
    """Represents a generic line in the CUE file."""
    def __init__(self, cmd: str, value: str):
        self.cmd = cmd
        self.value = value

    def to_string(self, indent: str = "") -> str:
        # Determine quoting strategy
        if self.cmd in QUOTED_FIELDS:
            return f'{indent}{self.cmd} "{self.value}"\n'
        elif self.cmd == 'REM':
            return f'{indent}REM {self.value}\n'
        else:
            return f'{indent}{self.cmd} {self.value}\n'

class IndexLine:
    """Represents an INDEX line, which needs offset calculation."""
    def __init__(self, idx_id: int, stamp: str, sectors: int):
        self.id = idx_id
        self.stamp = stamp
        self.file_offset = sectors

class Track:
    def __init__(self, num: int, track_type: str):
        self.num: int = num
        self.track_type: str = track_type
        self.lines: List[Union[CueLine, IndexLine]] = []
        self.sectors: Optional[int] = None
        self.blocksize: int = 0

        # Determine blocksize for this specific track
        if track_type in ['AUDIO', 'MODE1/2352', 'MODE2/2352', 'CDI/2352']:
            self.blocksize = 2352
        elif track_type == 'CDG':
            self.blocksize = 2448
        elif track_type == 'MODE1/2048':
            self.blocksize = 2048
        elif track_type in ['MODE2/2336', 'CDI/2336']:
            self.blocksize = 2336

class File:
    def __init__(self, filename: Path):
        self.filename: Path = filename
        self.tracks: List[Track] = []
        try:
            self.size: int = filename.stat().st_size
        except FileNotFoundError as exc:
            log_error(f"Critical IO Error: File disappeared before sizing: {filename}")
            raise exc

class CueSheet:
    def __init__(self):
        self.header_lines: List[CueLine] = [] 
        self.files: List[File] = []
        self.blocksize: Optional[int] = None

# ==========================================
# Parsing & logic
# ==========================================
def sectors_to_cuestamp(sectors: int) -> str:
    minutes = sectors // 4500
    remainder = sectors % 4500
    seconds = remainder // 75
    fields = remainder % 75
    return '%02d:%02d:%02d' % (minutes, seconds, fields)

def cuestamp_to_sectors(stamp: str) -> int:
    m = re.match(r"(\d+):(\d+):(\d+)", stamp)
    if not m:
        return 0
    minutes = int(m.group(1))
    seconds = int(m.group(2))
    fields = int(m.group(3))
    return fields + (seconds * 75) + (minutes * 60 * 75)

def parse_line_tokens(line: str) -> List[str]:
    try:
        return shlex.split(line, posix=True)
    except ValueError:
        return line.strip().split()

def read_cue_file(cue_path: Path) -> CueSheet:
    sheet = CueSheet()
    this_track: Optional[Track] = None
    this_file: Optional[File] = None
    bin_files_missing = False
    
    log_debug(f"Parsing CUE: {cue_path}")

    with cue_path.open('r', encoding='utf-8', errors='replace') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
                
            tokens = parse_line_tokens(line)
            if not tokens:
                continue

            cmd = tokens[0].upper()

            if cmd == 'FILE':
                if len(tokens) < 3: continue
                fname = tokens[1]
                this_path = cue_path.parent / fname
                log_debug(f"Found FILE entry: {fname}")
                
                if not this_path.is_file():
                    log_error(f"Bin file not found or not readable: {this_path}")
                    bin_files_missing = True
                else:
                    this_file = File(this_path)
                    sheet.files.append(this_file)
                continue

            elif cmd == 'TRACK':
                if len(tokens) < 3: continue
                if this_file:
                    t_num = int(tokens[1])
                    t_type = tokens[2]
                    this_track = Track(t_num, t_type)
                    
                    # Safely validate blocksize at the sheet level
                    if this_track.blocksize > 0:
                        if sheet.blocksize is None:
                            sheet.blocksize = this_track.blocksize
                            log_debug(f"Blocksize Lock: Set to {sheet.blocksize} bytes based on Track {t_num}")
                        elif sheet.blocksize != this_track.blocksize:
                            raise ValueError(
                                f"Inconsistent Blocksize detected! Track {t_num} ({t_type}) uses {this_track.blocksize} "
                                f"bytes, but previous tracks used {sheet.blocksize} bytes."
                            )
                            
                    this_file.tracks.append(this_track)
                continue

            elif cmd == 'INDEX':
                if len(tokens) < 3: continue
                if this_track:
                    idx_id = int(tokens[1])
                    stamp = tokens[2]
                    idx_obj = IndexLine(idx_id, stamp, cuestamp_to_sectors(stamp))
                    this_track.lines.append(idx_obj)
                continue

            else:
                value = ""
                if cmd == 'REM':
                    value = line[3:].strip()
                elif len(tokens) > 1:
                    if cmd == 'FLAGS':
                        value = " ".join(tokens[1:])
                    else:
                        value = tokens[1]

                line_obj = CueLine(cmd, value)
                if this_track:
                    this_track.lines.append(line_obj)
                else:
                    sheet.header_lines.append(line_obj)

    if bin_files_missing:
        raise BinFilesMissingException

    if not sheet.files:
        raise ZeroBinFilesException

    # Calculation logic for single-file split cases
    if len(sheet.files) == 1 and sheet.blocksize:
        log_debug("Single-file detected. Calculating track sectors based on indices...")
        next_item_offset = sheet.files[0].size // sheet.blocksize
        
        for t in reversed(sheet.files[0].tracks):
            indices = [x for x in t.lines if isinstance(x, IndexLine)]
            if not indices:
                log_debug(f"Track {t.num} has no indices?")
                continue

            # The first index listed in the CUE is the start of the block in the binary
            start_index = indices[0]
            t.sectors = next_item_offset - int(start_index.file_offset)
            log_debug(f"Track {t.num}: Start {start_index.file_offset}, End {next_item_offset}, Length {t.sectors}")
            
            next_item_offset = int(start_index.file_offset)

    return sheet

def track_filename(prefix: str, track_num: int, track_count: int) -> str:
    # Follows standard Redump naming conventions
    if track_count == 1:
        return f"{prefix}.bin"
    if track_count > 9:
        return f"{prefix} (Track {track_num:02d}).bin"
    return f"{prefix} (Track {track_num}).bin"

# ==========================================
# CUE Generation
# ==========================================
def gen_merged_cuesheet(basename: str, sheet: CueSheet) -> str:
    lines = []
    for line in sheet.header_lines:
        lines.append(line.to_string())
    
    lines.append(f'FILE "{basename}.bin" BINARY\n')
    
    sector_pos = 0
    if not sheet.blocksize:
        raise ValueError("Could not determine Blocksize from tracks.")

    for f in sheet.files:
        for t in f.tracks:
            lines.append(f'  TRACK {t.num:02d} {t.track_type}\n')
            for item in t.lines:
                if isinstance(item, CueLine):
                    lines.append(item.to_string(indent="    "))
                elif isinstance(item, IndexLine):
                    new_stamp = sectors_to_cuestamp(sector_pos + item.file_offset)
                    lines.append(f'    INDEX {item.id:02d} {new_stamp}\n')
        
        sector_pos += int(f.size // sheet.blocksize)
        
    return "".join(lines)

def gen_split_cuesheet(basename: str, merged_file: File, header_lines: List[CueLine]) -> str:
    lines = []
    for line in header_lines:
        lines.append(line.to_string())
    
    for t in merged_file.tracks:
        track_fn = track_filename(basename, t.num, len(merged_file.tracks))
        lines.append(f'FILE "{track_fn}" BINARY\n')
        lines.append(f'  TRACK {t.num:02d} {t.track_type}\n')
        
        first_index_offset = 0
        indices = [x for x in t.lines if isinstance(x, IndexLine)]
        if indices:
            first_index_offset = indices[0].file_offset

        for item in t.lines:
            if isinstance(item, CueLine):
                lines.append(item.to_string(indent="    "))
            elif isinstance(item, IndexLine):
                rel_pos = item.file_offset - first_index_offset
                new_stamp = sectors_to_cuestamp(rel_pos)
                lines.append(f'    INDEX {item.id:02d} {new_stamp}\n')
            
    return "".join(lines)

# ==========================================
# IO Operations
# ==========================================
def merge_files(merged_filename: Path, files: List[File], force: bool = False, dry_run: bool = False) -> bool:
    if merged_filename.exists():
        if force:
            log_warn(f"Overwriting existing file: {merged_filename}")
        else:
            log_error(f'Target merged bin path already exists: {merged_filename}')
            log_error('Use --force to overwrite.')
            return False

    total_size = sum(f.size for f in files)
    
    if dry_run:
        log_info(f"[DRY-RUN] Would create merged file: {merged_filename}")
        log_info(f"[DRY-RUN] Total size: {total_size} bytes")
        return True

    progress = ProgressBar(total_size, prefix='Merging')
    chunksize = 1024 * 1024
    try:
        with merged_filename.open('wb') as outfile:
            for f in files:
                log_debug(f"Merging part: {f.filename} ({f.size} bytes)")
                with f.filename.open('rb') as infile:
                    while True:
                        chunk = infile.read(chunksize)
                        if not chunk:
                            break
                        outfile.write(chunk)
                        progress.update(len(chunk))
        progress.finish()
        return True
    except IOError as exc:
        progress.finish()
        log_error(f"IO Error merging files: {exc}")
        return False

def split_files(new_basename: str, merged_file: File, outdir: Path, blocksize: int, force: bool = False, dry_run: bool = False) -> bool:
    if not blocksize:
        log_error("Cannot split files: Unknown Blocksize.")
        return False

    for t in merged_file.tracks:
        out_basename = track_filename(new_basename, t.num, len(merged_file.tracks))
        out_path = outdir / out_basename
        if out_path.exists():
            if force:
                log_warn(f"Overwriting existing file: {out_path}")
            else:
                log_error(f'Target bin path already exists: {out_path}')
                log_error('Use --force to overwrite.')
                return False

    if dry_run:
        log_info(f"[DRY-RUN] Would split source file: {merged_file.filename}")
        for t in merged_file.tracks:
            out_basename = track_filename(new_basename, t.num, len(merged_file.tracks))
            sz = (t.sectors * blocksize) if t.sectors else 0
            log_info(f"[DRY-RUN]   -> {out_basename} (~{sz // 1024} KB)")
        return True

    total_size = sum(t.sectors * blocksize for t in merged_file.tracks if t.sectors)
    progress = ProgressBar(total_size, prefix='Splitting')

    try:
        with merged_file.filename.open('rb') as infile:
            for t in merged_file.tracks:
                if t.sectors is None:
                    log_error(f"Track {t.num} has undefined sector count. Cannot split.")
                    return False
                    
                chunksize = 1024 * 1024
                out_basename = track_filename(new_basename, t.num, len(merged_file.tracks))
                out_path = outdir / out_basename
                
                tracksize = t.sectors * blocksize
                written = 0
                
                log_debug(f"Writing track {t.num} to {out_path} ({tracksize} bytes)")
                
                with out_path.open('wb') as outfile:
                    while True:
                        if chunksize + written > tracksize:
                            chunksize = tracksize - written
                        
                        if chunksize <= 0:
                            break
                            
                        chunk = infile.read(chunksize)
                        if not chunk:
                            if written < tracksize:
                                progress.finish()
                                log_error(f"Unexpected End of File reading track {t.num}")
                                return False
                            break
                        
                        outfile.write(chunk)
                        written += len(chunk)
                        progress.update(len(chunk))
                        
                        if written == tracksize:
                            break
        progress.finish()
        return True
    except IOError as exc:
        progress.finish()
        log_error(f"IO Error splitting files: {exc}")
        return False

def setup_output_dir(cuefile: Path, outdir_arg: Optional[str]) -> Optional[Path]:
    if outdir_arg:
        outdir = Path(outdir_arg).resolve()
        log_info(f"Output directory: {outdir}")
        if not outdir.exists():
            try:
                log_info("Output directory did not exist; creating it.")
                outdir.mkdir(parents=True, exist_ok=True)
            except OSError:
                log_error("Could not create output directory (permissions?)")
                traceback.print_exc()
                return None
    else:
        outdir = cuefile.parent
        log_info(f"Output directory: {outdir}")

    if not outdir.is_dir():
        log_error(f"Output directory is not a directory: {outdir}")
        return None

    if not os.access(outdir, os.W_OK):
        log_error(f"Output directory is not writable: {outdir}")
        return None
        
    return outdir

# ==========================================
# Main Runners
# ==========================================
def run_merge(args) -> bool:
    cuefile = Path(args.cuefile).resolve()
    if not cuefile.exists():
        log_error(f"Cue file does not exist: {cuefile}")
        return False

    outdir = setup_output_dir(cuefile, args.outdir)
    if not outdir:
        return False

    log_info(f"Opening cue: {cuefile}")
    try:
        sheet = read_cue_file(cuefile)
    except (BinFilesMissingException, ZeroBinFilesException, ValueError) as exc:
        log_error(str(exc))
        return False
    except Exception:
        traceback.print_exc()
        return False
    
    all_tracks = []
    for f in sheet.files:
        all_tracks.extend(f.tracks)

    log_info(f"Merging {len(all_tracks)} tracks...")
    
    try:
        cuesheet = gen_merged_cuesheet(args.basename, sheet)
    except ValueError as err:
        log_error(str(err))
        return False
    
    out_path = outdir / (args.basename + '.bin')
    
    if not merge_files(out_path, sheet.files, force=args.force, dry_run=args.dry_run):
        return False

    if args.dry_run:
        log_info(f"[DRY-RUN] Not writing CUE sheet.")
        print("--- PREVIEW OF CUE SHEET ---")
        print(cuesheet, end="")
        print("--- END PREVIEW ---")
        return True

    new_cue_fn = outdir / (args.basename + '.cue')
    if new_cue_fn.exists():
        if args.force:
            log_warn(f"Overwriting existing cue: {new_cue_fn}")
        else:
            log_error(f"Output cue file already exists: {new_cue_fn}")
            log_error("Use --force to overwrite.")
            return False

    with new_cue_fn.open('w', newline='\r\n', encoding='utf-8') as f:
        f.write(cuesheet)
    log_info(f"Wrote new cue: {new_cue_fn}")
    return True

def run_split(args) -> bool:
    cuefile = Path(args.cuefile).resolve()
    if not cuefile.exists():
        log_error(f"Cue file does not exist: {cuefile}")
        return False

    outdir = setup_output_dir(cuefile, args.outdir)
    if not outdir:
        return False

    log_info(f"Opening cue: {cuefile}")
    try:
        sheet = read_cue_file(cuefile)
    except (BinFilesMissingException, ZeroBinFilesException, ValueError) as exc:
        log_error(str(exc))
        return False
    except Exception:
        traceback.print_exc()
        return False

    if len(sheet.files) > 1:
        log_error("Cannot split: The cuesheet references multiple files. It seems they are not merged yet.")
        return False

    log_warn("Splitting tracks may result in lost hidden audio (Pregap/Postgap) depending on the source CUE format.")
    log_info("Splitting files...")
    
    if not split_files(args.basename, sheet.files[0], outdir, sheet.blocksize, force=args.force, dry_run=args.dry_run):
        return False

    cuesheet = gen_split_cuesheet(args.basename, sheet.files[0], sheet.header_lines)
    
    if args.dry_run:
        log_info(f"[DRY-RUN] Not writing CUE sheet.")
        print("--- PREVIEW OF CUE SHEET ---")
        print(cuesheet, end="")
        print("--- END PREVIEW ---")
        return True

    new_cue_fn = outdir / (args.basename + '.cue')
    if new_cue_fn.exists():
        if args.force:
            log_warn(f"Overwriting existing cue: {new_cue_fn}")
        else:
            log_error(f"Output cue file already exists: {new_cue_fn}")
            log_error("Use --force to overwrite.")
            return False

    with new_cue_fn.open('w', newline='\r\n', encoding='utf-8') as f:
        f.write(cuesheet)
    log_info(f"Wrote new cue: {new_cue_fn}")
    return True

def main() -> int:
    base_parser = argparse.ArgumentParser(add_help=False)
    base_parser.add_argument('-v', '--verbose', action='store_true', help='print more verbose messages')
    base_parser.add_argument('-n', '--dry-run', action='store_true', help='simulate operation without writing files')

    parser = argparse.ArgumentParser(
        description="Merges or splits binary track files using a cuesheet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=[base_parser]
    )

    class LicenseAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            print_license()
            parser.exit()
            
    parser.add_argument('-l', '--license', nargs=0, action=LicenseAction, help='prints license info and exits')
    parser.add_argument('--version', action='version', version=f'%(prog)s {VERSION_STRING}')

    subparsers = parser.add_subparsers(dest='command', required=True, help='Action to perform')

    parser_merge = subparsers.add_parser('merge', parents=[base_parser], help='Merge multiple bin files into one')
    parser_merge.add_argument('cuefile', help='path to source cue file')
    parser_merge.add_argument('basename', help='output filename (without extension)')
    parser_merge.add_argument('-o', '--outdir', help='output directory')
    parser_merge.add_argument('-f', '--force', action='store_true', help='overwrite existing files without asking')
    parser_merge.set_defaults(func=run_merge)

    parser_split = subparsers.add_parser('split', parents=[base_parser], help='Split a single bin file into multiple tracks')
    parser_split.add_argument('cuefile', help='path to source cue file')
    parser_split.add_argument('basename', help='output filename (without extension)')
    parser_split.add_argument('-o', '--outdir', help='output directory')
    parser_split.add_argument('-f', '--force', action='store_true', help='overwrite existing files without asking')
    parser_split.set_defaults(func=run_split)

    args = parser.parse_args()

    global VERBOSE
    if args.verbose:
        VERBOSE = True

    success = args.func(args)
    return 0 if success else 1

if __name__ == '__main__':
    sys.exit(main())
