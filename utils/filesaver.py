#!/usr/bin/env python3
import argparse
import os
import glob
import re
import sys

def natural_sort_key(s):
    """Generates a key for natural sorting (e.g., file.part1, file.part2, file.part10)."""
    return [int(text) if text.isdigit() else text.lower() for text in re.split('(\d+)', s)]

def main():
    parser = argparse.ArgumentParser(description="Merge .part files into a full file.")
    parser.add_argument("--dir", "-d", default=".", help="Directory containing the .part files (default: current directory)")
    parser.add_argument("output_prefix", nargs="?", help="Output filename prefix. If provided, only .part files with this prefix will be merged.")
    args = parser.parse_args()

    directory = args.dir
    if not os.path.isdir(directory):
        print(f"Error: The directory '{directory}' does not exist or is not a directory.")
        sys.exit(1)

    if args.output_prefix:
        # Look for files starting with the specified prefix followed by .part (and optionally numbers)
        pattern = os.path.join(directory, args.output_prefix + ".part*")
        part_files = sorted(glob.glob(pattern), key=natural_sort_key)
        if not part_files:
            print(f"No .part files found with prefix '{args.output_prefix}' in directory '{directory}'.")
            sys.exit(1)
        output_filename = os.path.join(directory, args.output_prefix)
    else:
        # Find all .part files in the specified directory.
        pattern = os.path.join(directory, "*.part")
        part_files = sorted(glob.glob(pattern), key=natural_sort_key)
        if not part_files:
            print(f"No .part files found in directory '{directory}'.")
            sys.exit(1)
        # Derive output filename from the first file found by stripping the '.part' suffix and any trailing digits.
        first_file = os.path.basename(part_files[0])
        output_prefix = re.sub(r'\.part\d*$', '', first_file)
        output_filename = os.path.join(directory, output_prefix)

    print("Merging the following parts:")
    for pf in part_files:
        print("  ", pf)

    try:
        with open(output_filename, "wb") as outfile:
            for part in part_files:
                with open(part, "rb") as infile:
                    outfile.write(infile.read())
        print(f"Successfully merged {len(part_files)} parts into '{output_filename}'.")
    except Exception as e:
        print("An error occurred while merging files:", e)

if __name__ == "__main__":
    main()
