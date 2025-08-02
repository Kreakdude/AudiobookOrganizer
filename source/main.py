import os
import subprocess
import json
import shutil
import re
import sys
import time
import argparse
from multiprocessing import Pool, cpu_count, Lock
import xml.etree.ElementTree as ET
from collections import defaultdict

# Import functions from other modules
from metadata_utils import get_audio_metadata_and_embedded_image_status, parse_opf_metadata, extract_series_info, extract_internal_part_info, strip_series_info_from_title, strip_part_info_from_title, normalize_publisher_name
from file_system_utils import custom_print, sanitize_filename, hard_link_to_leftbehind, set_global_log_handles

# SCRIPT VERSION - Increment this each time the script is modified and sent
SCRIPT_VERSION = "1.0.40" # Updated version for ls_result generation, find_longest_common_substring, and cache debug

# Global lock for print statements to avoid interleaving output from multiple processes/threads
print_lock = Lock()

AUDIO_EXTENSIONS = ('.mp3', '.m4a', '.m4b')
IMAGE_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.gif', '.webp')
EBOOK_EXTENSIONS = ('.epub', '.mobi', '.azw3', '.pdf', '.lit', '.prc', '.fb2', '.txt', '.rtf')

def find_longest_common_substring(strings):
    """
    Finds the longest common substring among a list of strings.
    Args:
        strings (list): A list of strings.
    Returns:
        str: The longest common substring.
    """
    if not strings:
        return ""
    if len(strings) == 1:
        return strings[0]

    s1 = strings[0]
    longest_common = ""

    for i in range(len(s1)):
        for j in range(i + 1, len(s1) + 1):
            substring = s1[i:j]
            is_common = True
            for other_string in strings[1:]:
                if substring not in other_string:
                    is_common = False
                    break
            if is_common and len(substring) > len(longest_common):
                longest_common = substring
    return longest_common.strip()

def generate_ls_output(directory, output_file_path, custom_print_func):
    """
    Generates 'ls -R' output for a given directory and saves it to a file.
    Args:
        directory (str): The directory to run 'ls -R' on.
        output_file_path (str): The file path to save the output to.
        custom_print_func (function): The logging function.
    """
    custom_print_func(f"--- Generating ls -R output for '{directory}' ---", to_console=True)
    try:
        # Use subprocess.run with shell=True for 'ls -R' for simplicity
        # capture_output=True captures stdout and stderr
        # text=True decodes stdout/stderr as text
        result = subprocess.run(['ls', '-R', directory], capture_output=True, text=True, check=True)
        with open(output_file_path, 'w', encoding='utf-8') as f:
            f.write(result.stdout)
        custom_print_func(f"ls -R output for '{directory}' saved to: '{output_file_path}'", to_console=True)
    except FileNotFoundError:
        custom_print_func(f"Error: 'ls' command not found. Cannot generate ls -R output for '{directory}'.", level="ERROR", to_console=True)
    except subprocess.CalledProcessError as e:
        custom_print_func(f"Error generating ls -R output for '{directory}': {e.stderr}", level="ERROR", to_console=True)
    except IOError as e:
        custom_print_func(f"Error writing ls -R output to '{output_file_path}': {e}", level="ERROR", to_console=True)


def _get_physical_folder_metadata(args):
    """
    Worker function for multiprocessing pool to get metadata for a single physical folder.
    Args:
        args (tuple): A tuple containing (physical_folder_path, audiobook_cache).
    Returns:
        dict: A dictionary containing metadata and processing results for the folder.
    """
    physical_folder_path, audiobook_cache = args
    # Global log handles are set via Pool initializer, no need to pass them here

    folder_name = os.path.basename(physical_folder_path)
    custom_print(f"  Info: Pre-scanning folder: '{folder_name}'", to_console=False)

    combined_metadata = None
    book_has_embedded_image = False
    all_audio_files_details_in_folder = []
    worker_cache_updates = {} # Cache updates specific to this worker

    audio_files_in_folder = []
    opf_file = None

    # Walk through the physical folder to find audio and OPF files
    for root, _, files in os.walk(physical_folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            if file.lower().endswith(AUDIO_EXTENSIONS):
                audio_files_in_folder.append(file_path)
            elif file.lower().endswith('.opf'):
                opf_file = file_path # Assuming one OPF per folder for now
        if audio_files_in_folder or opf_file: # Stop after finding files in top level or first audio file
            break
    
    audio_files_in_folder.sort() # Ensure consistent order

    if not audio_files_in_folder:
        custom_print(f"  Warning: No audio files found in '{folder_name}'. Skipping metadata extraction for this folder.", level="WARNING", to_console=False)
        return {
            'physical_folder_path': physical_folder_path,
            'combined_metadata': None,
            'book_has_embedded_image': False,
            'all_audio_files_details_in_folder': [],
            'worker_cache_updates': worker_cache_updates
        }

    # Try to find an OPF file first
    opf_metadata = {}
    if opf_file:
        custom_print(f"  DEBUG: Found OPF file: '{os.path.basename(opf_file)}' in '{folder_name}'", level="DEBUG", to_console=False)
        opf_metadata = parse_opf_metadata(opf_file, custom_print)
        if opf_metadata:
            custom_print(f"  DEBUG: OPF metadata for '{folder_name}': {opf_metadata}", level="DEBUG", to_console=False)
            
    # Process each audio file for its metadata
    for audio_file_name in audio_files_in_folder:
        audio_file_path = os.path.join(physical_folder_path, audio_file_name) # Ensure full path
        
        # Check cache first
        if audio_file_path in audiobook_cache:
            file_metadata = audiobook_cache[audio_file_path]['metadata']
            has_embedded_image_for_file = audiobook_cache[audio_file_path]['has_embedded_image']
            custom_print(f"  Info: Using cached metadata for '{os.path.basename(audio_file_path)}'", to_console=False)
        else:
            file_metadata, has_embedded_image_for_file = get_audio_metadata_and_embedded_image_status(audio_file_path, custom_print)
            if file_metadata:
                worker_cache_updates[audio_file_path] = {
                    'metadata': file_metadata,
                    'has_embedded_image': has_embedded_image_for_file
                }
            custom_print(f"  DEBUG: Fresh metadata for '{os.path.basename(audio_file_path)}': {file_metadata}", level="DEBUG", to_console=False)

        if file_metadata:
            all_audio_files_details_in_folder.append({
                'file_path': audio_file_path,
                'metadata': file_metadata,
                'has_embedded_image': has_embedded_image_for_file
            })
            if has_embedded_image_for_file:
                book_has_embedded_image = True
        else:
            custom_print(f"  Warning: Could not extract metadata for audio file: '{os.path.basename(audio_file_path)}'. It will be treated as unorganized.", level="WARNING", to_console=True)

    # Combine OPF metadata with audio file metadata. OPF takes precedence for core book info.
    if opf_metadata:
        combined_metadata = opf_metadata.copy()
        # Overlay common audio file metadata if not present in OPF
        if all_audio_files_details_in_folder:
            first_audio_meta = all_audio_files_details_in_folder[0]['metadata']
            for key in ['artist', 'album', 'title', 'genre', 'comment', 'grouping', 'description', 'TIT3', 'TRACKTOTAL', 'copyright', 'publisher', 'performer', 'date']:
                if key not in combined_metadata and key in first_audio_meta:
                    combined_metadata[key] = first_audio_meta[key]
        custom_print(f"  DEBUG: Combined metadata (OPF prioritized) for '{folder_name}': {combined_metadata}", level="DEBUG", to_console=False)
    elif all_audio_files_details_in_folder:
        # If no OPF, use metadata from the first audio file as the primary source for the folder
        combined_metadata = all_audio_files_details_in_folder[0]['metadata'].copy()
        custom_print(f"  DEBUG: Combined metadata (first audio file) for '{folder_name}': {combined_metadata}", level="DEBUG", to_console=False)
    
    # Ensure publisher is normalized in combined_metadata
    if combined_metadata and 'publisher' in combined_metadata:
        combined_metadata['publisher'] = normalize_publisher_name(combined_metadata['publisher'])
        custom_print(f"  DEBUG: Publisher after normalization: '{combined_metadata['publisher']}' for '{folder_name}'", level="DEBUG", to_console=False)

    # Extract part info from the physical folder name itself, and add to combined_metadata
    part_designation, part_number, total_parts = extract_internal_part_info(folder_name)
    if combined_metadata:
        combined_metadata['extracted_part_designation'] = part_designation
        combined_metadata['extracted_part_number'] = part_number
        combined_metadata['extracted_total_parts'] = total_parts
    custom_print(f"  DEBUG: Extracted part info for '{folder_name}': designation='{part_designation}', num={part_number}, total={total_parts}", level="DEBUG", to_console=False)

    custom_print(f"  DEBUG: Worker cache updates for '{folder_name}': {worker_cache_updates}", level="DEBUG", to_console=False) # New debug print

    return {
        'physical_folder_path': physical_folder_path,
        'combined_metadata': combined_metadata,
        'book_has_embedded_image': book_has_embedded_image,
        'all_audio_files_details_in_folder': all_audio_files_details_in_folder,
        'worker_cache_updates': worker_cache_updates
    }

def process_single_logical_book_or_part(args):
    """
    Processes a single logical book or a part of a multi-part book.
    Args:
        args (tuple): A tuple containing (logical_book_info, source_root_dir, dest_base_dir,
                                         leftbehind_base_dir, series_max_numbers, ambiguous_base_names,
                                         parent_book_path).
    Returns:
        tuple: (linked_count, errors_count, final_book_path_relative_to_dest, audio_manual_logs, non_audio_manual_logs, successfully_linked_paths, associated_physical_folders)
    """
    logical_book_info, source_root_dir, dest_base_dir, leftbehind_base_dir, \
    series_max_numbers, ambiguous_base_names, parent_book_path = args

    # Global log handles are set via Pool initializer, no need to pass them here

    linked_count = 0
    errors_count = 0
    audio_manual_logs = []
    non_audio_manual_logs = []
    successfully_linked_paths = set()
    associated_physical_folders = set() # To track which physical folders were processed by this logical book/part

    sanitized_author = logical_book_info['author']
    sanitized_series_name = logical_book_info['series_name']
    series_book_num_for_folder = logical_book_info['series_book_num']
    sanitized_core_book_title = logical_book_info['core_book_title']
    publisher = logical_book_info['publisher']
    performer = logical_book_info['performer']
    book_has_embedded_image = logical_book_info['book_has_embedded_image']
    
    physical_folder_paths_for_this_part = logical_book_info['physical_folder_paths']
    all_audio_files_details_for_this_part = logical_book_info['all_audio_files_details']

    part_display_name = logical_book_info['part_display_name'] # Will be None for single books
    # part_number_int = logical_book_info.get('part_number_int') # Integer part number for sorting/padding
    # total_parts_int = logical_book_info.get('total_parts_int') # Integer total parts for padding

    # Construct the destination path
    dest_author_path = os.path.join(dest_base_dir, sanitized_author)
    
    final_book_folder_name_prefix = ""
    if sanitized_series_name and series_book_num_for_folder is not None:
        max_series_num = series_max_numbers.get(sanitized_series_name, series_book_num_for_folder)
        
        # Determine padding for series number: only pad if max_series_num is 10 or more
        # or if it's a float (e.g., 2.5) where the integer part is single digit but needs to align with multi-digit.
        # Based on user's goal, no leading zeros for single digits (e.g., "1 - The Way of Kings")
        padding_length_for_series_num = 1
        if isinstance(max_series_num, float):
            if int(max_series_num) >= 10:
                padding_length_for_series_num = len(str(int(max_series_num)))
        else: # Integer series number
            if int(max_series_num) >= 10:
                padding_length_for_series_num = len(str(int(max_series_num)))

        int_part = int(series_book_num_for_folder)
        frac_part_str = ""
        if isinstance(series_book_num_for_folder, float) and series_book_num_for_folder != int_part:
            str_series_number = str(series_book_num_for_folder)
            if '.' in str_series_number:
                frac_part_str = "." + str_series_number.split('.')[-1]
        
        # Apply padding only if padding_length_for_series_num is greater than 1
        if padding_length_for_series_num > 1:
            padded_series_number_str = f"{int_part:0{padding_length_for_series_num}d}{frac_part_str}"
        else:
            padded_series_number_str = f"{int_part}{frac_part_str}" # No padding for single digits

        final_book_folder_name_prefix = f"{padded_series_number_str} - "

    book_or_series_distinguisher_to_apply = ""
    check_base_name = sanitized_series_name if sanitized_series_name else sanitized_core_book_title
    if (sanitized_author, check_base_name) in ambiguous_base_names:
        if publisher:
            book_or_series_distinguisher_to_apply = f" (Published by {normalize_publisher_name(publisher)})" # Use normalized publisher directly
        elif performer:
            book_or_series_distinguisher_to_apply = f" (Narrated by {normalize_publisher_name(performer)})" # Use normalized performer directly

    # Determine the base path for the book (either directly under author or under series)
    if sanitized_series_name:
        # Series folder
        current_series_folder_name = f"{sanitized_series_name}{book_or_series_distinguisher_to_apply}"
        dest_series_path = os.path.join(dest_author_path, sanitize_filename(current_series_folder_name))
        os.makedirs(dest_series_path, exist_ok=True)
        custom_print(f"  Info: Created series folder: '{os.path.relpath(dest_series_path, dest_base_dir)}'", to_console=False)
        
        # Book folder goes under series folder
        base_book_folder_name = f"{final_book_folder_name_prefix}{sanitized_core_book_title}"
        dest_book_base_path = os.path.join(dest_series_path, sanitize_filename(base_book_folder_name))
    else:
        # No series, book folder directly under author
        base_book_folder_name = f"{final_book_folder_name_prefix}{sanitized_core_book_title}{book_or_series_distinguisher_to_apply}"
        dest_book_base_path = os.path.join(dest_author_path, sanitize_filename(base_book_folder_name))

    # Now, determine the final destination path for the current part/book
    dest_book_path = dest_book_base_path
    if logical_book_info.get('is_multi_part') and part_display_name:
        # For multi-part books, create a subfolder for each part
        clean_part_folder_name = part_display_name.strip('()') # e.g., "1 of 5"
        dest_book_path = os.path.join(dest_book_base_path, sanitize_filename(clean_part_folder_name))
        custom_print(f"  DEBUG: Multi-part book part path: '{os.path.relpath(dest_book_path, dest_base_dir)}'", level="DEBUG", to_console=False)
    
    os.makedirs(dest_book_path, exist_ok=True)
    custom_print(f"  Info: Created book/part folder: '{os.path.relpath(dest_book_path, dest_base_dir)}'", to_console=False)

    final_book_path_relative_to_dest = os.path.relpath(dest_book_path, dest_base_dir)

    # Process audio files
    num_audio_files_in_part = len(all_audio_files_details_for_this_part)
    for i, audio_file_detail in enumerate(all_audio_files_details_for_this_part): # Use files specific to THIS part
        src_audio_file_path = audio_file_detail['file_path']
        audio_metadata = audio_file_detail['metadata']
        
        # Determine the track number for file naming
        track_num_raw = audio_metadata.get('track')
        track_total_raw = audio_metadata.get('TRACKTOTAL')
        
        track_num = None
        track_total = None

        if track_num_raw:
            try:
                if '/' in track_num_raw:
                    track_num = int(track_num_raw.split('/')[0].strip())
                    track_total = int(track_num_raw.split('/')[1].strip()) if len(track_num_raw.split('/')) > 1 else None
                else:
                    track_num = int(track_num_raw.strip())
            except ValueError:
                custom_print(f"  Warning: Could not parse track number '{track_num_raw}' for '{os.path.basename(src_audio_file_path)}'.", level="WARNING", to_console=False)
        
        if track_total_raw and track_total is None: # Only use if not already set by track_num_raw
            try:
                track_total = int(track_total_raw.strip())
            except ValueError:
                pass

        # Determine padding for track number
        padding = 2 # Default padding for consistency if multiple tracks
        if track_total is not None:
            padding = max(padding, len(str(track_total)))
        elif num_audio_files_in_part > 1: # If it's a multi-file part, ensure padding for total files in THIS part
            padding = max(padding, len(str(num_audio_files_in_part)))

        track_info_for_filename = ""
        if num_audio_files_in_part > 1: # Only add track info if there's more than one audio file in this part
            if track_num is not None and track_total is not None:
                track_info_for_filename = f" Track {track_num:0{padding}d} of {track_total:0{padding}d}"
            elif track_num is not None:
                track_info_for_filename = f" Track {track_num:0{padding}d}"
            else: # Fallback to index if no track metadata
                # Sort files by name to ensure consistent indexing if track metadata is missing
                sorted_audio_files_paths = sorted([d['file_path'] for d in all_audio_files_details_for_this_part])
                current_file_index = sorted_audio_files_paths.index(src_audio_file_path) + 1
                track_info_for_filename = f" Track {current_file_index:0{padding}d} of {num_audio_files_in_part:0{padding}d}"
                custom_print(f"  DEBUG: Using generated track index {current_file_index} for '{os.path.basename(src_audio_file_path)}'", level="DEBUG", to_console=False)

        # Determine the file title for the audio file name
        # Always start with the core book title
        audio_file_base_name = sanitized_core_book_title

        # If it's a part of a multi-part book, add the part display name to the audio file name
        if logical_book_info.get('is_multi_part') and part_display_name:
            # Reconstruct the part info without the outer parentheses for the filename
            # e.g., "(Part 01 of 05)" -> "Part 1 of 5"
            part_info_for_audio_filename = part_display_name.strip('()')
            audio_file_base_name = f"{audio_file_base_name} - {part_info_for_audio_filename}"

        # Combine all parts for the final audio file name
        final_audio_file_name = f"{audio_file_base_name}{track_info_for_filename}{os.path.splitext(src_audio_file_path)[1]}"
        final_audio_file_name = sanitize_filename(final_audio_file_name) # Ensure final name is sanitized

        dest_audio_file_path = os.path.join(dest_book_path, final_audio_file_name)

        try:
            os.link(src_audio_file_path, dest_audio_file_path)
            linked_count += 1
            successfully_linked_paths.add(src_audio_file_path)
            custom_print(f"  Info: Hard-linked '{os.path.basename(src_audio_file_path)}' to '{os.path.relpath(dest_audio_file_path, dest_base_dir)}'", to_console=False)
        except OSError as e:
            errors_count += 1
            error_msg = f"  Error: Could not hard-link '{os.path.basename(src_audio_file_path)}' to '{os.path.relpath(dest_audio_file_path, dest_base_dir)}': {e}"
            custom_print(error_msg, level="ERROR", to_console=True)
            hard_link_to_leftbehind(src_audio_file_path, source_root_dir, leftbehind_base_dir, reason=f"Hard-link failed: {e}", manual_log_list=non_audio_manual_logs, level="ERROR")
    
    # Process non-audio files (e.g., cover art, PDFs, EPUBs) from the original physical folders
    for physical_folder_path in physical_folder_paths_for_this_part: # Use physical folders specific to THIS part
        associated_physical_folders.add(physical_folder_path) # Mark this folder as processed
        
        # Collect all potential extra files in the physical folder
        all_files_in_folder = [
            f for f in os.listdir(physical_folder_path)
            if os.path.isfile(os.path.join(physical_folder_path, f))
        ]
        
        primary_cover_linked = False
        
        # Determine the desired cover image name
        cover_image_base_name = sanitized_core_book_title
        if logical_book_info.get('is_multi_part') and part_display_name:
            # Cover image for a part should be named after the part, e.g., "The Way of Kings (Part 1 of 5) Cover.jpg"
            cover_image_base_name = f"{sanitized_core_book_title} {part_display_name.strip('()')}"
        
        # Prioritize linking a single "Cover.jpg" or similar if no embedded image
        # OR if an embedded image exists, but we still want a separate cover file.
        # The user's goal shows explicit Cover.jpg files, so we should always try to link them.
        for file_name in all_files_in_folder:
            src_file_path = os.path.join(physical_folder_path, file_name)

            if file_name.lower().endswith(IMAGE_EXTENSIONS):
                # Check if this image is a strong candidate for the main cover
                if any(kw in file_name.lower() for kw in ['cover', 'folder', 'front']) or \
                   (not primary_cover_linked and len([f for f in all_files_in_folder if f.lower().endswith(IMAGE_EXTENSIONS)]) == 1): # If only one image, assume it's the cover
                    
                    dest_image_path = os.path.join(dest_book_path, f"{sanitize_filename(cover_image_base_name)} Cover{os.path.splitext(file_name)[1]}")
                    
                    try:
                        os.link(src_file_path, dest_image_path)
                        linked_count += 1
                        successfully_linked_paths.add(src_file_path)
                        custom_print(f"  Info: Hard-linked '{file_name}' as primary cover to '{os.path.relpath(dest_image_path, dest_base_dir)}'", to_console=False)
                        primary_cover_linked = True
                    except OSError as e:
                        errors_count += 1
                        error_msg = f"  Error: Could not hard-link primary cover '{os.path.basename(src_file_path)}': {e}"
                        custom_print(error_msg, level="ERROR", to_console=True)
                        non_audio_manual_logs.append(f"[ERROR] {error_msg}")
                        hard_link_to_leftbehind(src_file_path, source_root_dir, leftbehind_base_dir, reason=f"Hard-link failed for primary cover: {e}", manual_log_list=non_audio_manual_logs, level="ERROR")
                
                # All other image files (including those not chosen as primary cover) are treated as extras
                # No separate "Artwork" folder, they go to "Extras" if not the main cover.
                elif src_file_path not in successfully_linked_paths: # If not already linked as primary cover
                    dest_extra_path = os.path.join(dest_book_path, "Extras", sanitize_filename(file_name))
                    os.makedirs(os.path.dirname(dest_extra_path), exist_ok=True)
                    try:
                        os.link(src_file_path, dest_extra_path)
                        linked_count += 1
                        successfully_linked_paths.add(src_file_path)
                        custom_print(f"  Info: Hard-linked extra image '{os.path.basename(src_file_path)}' to '{os.path.relpath(dest_extra_path, dest_base_dir)}'", to_console=False)
                    except OSError as e:
                        errors_count += 1
                        error_msg = f"  Error: Could not hard-link extra image '{os.path.basename(src_file_path)}': {e}"
                        custom_print(error_msg, level="ERROR", to_console=True)
                        non_audio_manual_logs.append(f"[ERROR] {error_msg}")
                        hard_link_to_leftbehind(src_file_path, source_root_dir, leftbehind_base_dir, reason=f"Hard-link failed for extra image: {e}", manual_log_list=non_audio_manual_logs, level="ERROR")
            
            elif file_name.lower() == 'playlist.ll':
                # Place playlist.ll directly in the book/part folder
                dest_playlist_path = os.path.join(dest_book_path, sanitize_filename(file_name))
                try:
                    os.link(src_file_path, dest_playlist_path)
                    linked_count += 1
                    successfully_linked_paths.add(src_file_path)
                    custom_print(f"  Info: Hard-linked 'playlist.ll' to '{os.path.relpath(dest_playlist_path, dest_base_dir)}'", to_console=False)
                except OSError as e:
                    errors_count += 1
                    error_msg = f"  Error: Could not hard-link 'playlist.ll' '{os.path.basename(src_file_path)}': {e}"
                    custom_print(error_msg, level="ERROR", to_console=True)
                    non_audio_manual_logs.append(f"[ERROR] {error_msg}")
                    hard_link_to_leftbehind(src_file_path, source_root_dir, leftbehind_base_dir, reason=f"Hard-link failed for playlist.ll: {e}", manual_log_list=non_audio_manual_logs, level="ERROR")
            
            elif file_name.lower().endswith(EBOOK_EXTENSIONS) and src_file_path not in successfully_linked_paths: # Handle EPUBs and PDFs explicitly
                dest_extra_path = os.path.join(dest_book_path, "Extras", sanitize_filename(file_name))
                os.makedirs(os.path.dirname(dest_extra_path), exist_ok=True)
                try:
                    os.link(src_file_path, dest_extra_path)
                    linked_count += 1
                    successfully_linked_paths.add(src_file_path)
                    custom_print(f"  Info: Hard-linked extra file '{os.path.basename(src_file_path)}' to '{os.path.relpath(dest_extra_path, dest_base_dir)}'", to_console=False)
                except OSError as e:
                    errors_count += 1
                    error_msg = f"  Error: Could not hard-link extra file '{os.path.basename(src_file_path)}': {e}"
                    custom_print(error_msg, level="ERROR", to_console=True)
                    non_audio_manual_logs.append(f"[ERROR] {error_msg}")
                    hard_link_to_leftbehind(src_file_path, source_root_dir, leftbehind_base_dir, reason=f"Hard-link failed for extra file: {e}", manual_log_list=non_audio_manual_logs, level="ERROR")

            elif not file_name.lower().endswith(AUDIO_EXTENSIONS + IMAGE_EXTENSIONS + ('.opf',) + EBOOK_EXTENSIONS) and src_file_path not in successfully_linked_paths: # Catch any other unhandled files
                # Hard-link other non-audio, non-image files to an "Extras" subfolder
                dest_extra_path = os.path.join(dest_book_path, "Extras", sanitize_filename(file_name))
                os.makedirs(os.path.dirname(dest_extra_path), exist_ok=True)
                try:
                    os.link(src_file_path, dest_extra_path)
                    linked_count += 1
                    successfully_linked_paths.add(src_file_path)
                    custom_print(f"  Info: Hard-linked extra file '{os.path.basename(src_file_path)}' to '{os.path.relpath(dest_extra_path, dest_base_dir)}'", to_console=False)
                except OSError as e:
                    errors_count += 1
                    error_msg = f"  Error: Could not hard-link extra file '{os.path.basename(src_file_path)}': {e}"
                    custom_print(error_msg, level="ERROR", to_console=True)
                    non_audio_manual_logs.append(f"[ERROR] {error_msg}")
                    hard_link_to_leftbehind(src_file_path, source_root_dir, leftbehind_base_dir, reason=f"Hard-link failed for extra file: {e}", manual_log_list=non_audio_manual_logs, level="ERROR")
    
    custom_print(f"  Organized logical book/part '{sanitized_core_book_title}' into '{os.path.relpath(dest_book_path, dest_base_dir)}'. Hard-linked {linked_count} files.", to_console=False)
    processed_book_info = os.path.relpath(dest_book_path, dest_base_dir)
    return (linked_count, errors_count, processed_book_info, audio_manual_logs, non_audio_manual_logs, successfully_linked_paths, associated_physical_folders)


def group_physical_folders_into_logical_books(physical_folder_metadata_results, custom_print_func):
    """
    Groups physical folders into logical books based on author, series, book number, and publisher.
    Args:
        physical_folder_metadata_results (list): List of results from _get_physical_folder_metadata.
        custom_print_func (function): The logging function.
    Returns:
        tuple: (list of logical book info dicts, dict of series max numbers, set of ambiguous base names)
    """
    logical_books_grouped_by_key = {} 
    
    for physical_folder_result in physical_folder_metadata_results:
        physical_folder_path = physical_folder_result['physical_folder_path']
        combined_metadata = physical_folder_result['combined_metadata']
        book_has_embedded_image_from_folder = physical_folder_result['book_has_embedded_image']
        all_audio_files_details_in_folder = physical_folder_result['all_audio_files_details_in_folder']

        if combined_metadata is None:
            # This physical folder had no audio files, or metadata extraction failed.
            # It should have been handled by hard_link_to_leftbehind in main loop.
            continue

        artist = combined_metadata.get('artist')
        album = combined_metadata.get('album')
        title = combined_metadata.get('title')
        grouping = combined_metadata.get('grouping')
        performer = combined_metadata.get('performer')
        publisher = combined_metadata.get('publisher') # This is already normalized

        sanitized_author = sanitize_filename(re.split(r'[;,/&]| and ', artist)[0].strip()) if artist else "Unknown Author"
        
        custom_print_func(f"  DEBUG: Before extract_series_info - grouping: '{grouping}', album: '{album}', title: '{title}'", level="DEBUG", to_console=False)
        # Extract series info (cleaned series name and book number)
        sanitized_series_name, series_number_from_extracted_patterns = extract_series_info(grouping, album, title)
        custom_print_func(f"  DEBUG: After extract_series_info - sanitized_series_name: '{sanitized_series_name}', series_number_from_extracted_patterns: {series_number_from_extracted_patterns}", level="DEBUG", to_console=False)

        # Prioritize series_book_num if it came directly from OPF parsing
        series_book_num_for_logic = None
        if 'series_book_num' in combined_metadata and combined_metadata['series_book_num'] is not None:
            series_book_num_for_logic = combined_metadata['series_book_num']
            custom_print_func(f"  DEBUG: Using OPF-derived series_book_num: {series_book_num_for_logic}", level="DEBUG", to_console=False)
            # If OPF gave a series_book_num, ensure series_name is also consistent from grouping if available
            if not sanitized_series_name and 'grouping' in combined_metadata:
                sanitized_series_name = sanitize_filename(combined_metadata['grouping'])
                custom_print_func(f"  DEBUG: OPF series_book_num present, but no series_name. Using grouping: '{sanitized_series_name}'", level="DEBUG", to_console=False)
                # Re-clean the series name from grouping after setting it
                if sanitized_series_name:
                    # Apply generic cleaning to the series name after setting it from grouping
                    sanitized_series_name = re.sub(r'\[Dramatized Adaptation\]', '', sanitized_series_name, flags=re.IGNORECASE).strip() # Specific cleanup
                    sanitized_series_name = re.sub(r'\s*(?:#\d+(?:\.\d+)?(?:[ -].*)?|\((?:book|part|disc|volume|vol)\s*\d+(?:\.\d+)?(?:\s+of\s*\d+(?:\.\d+)?)?\)|\[.*?\])$', '', sanitized_series_name, flags=re.IGNORECASE).strip()
                    sanitized_series_name = re.sub(r'\s*(?:Part|Disc|Volume|Vol)\s*\d+(?:\s+of\s*\d+)?', '', sanitized_series_name, flags=re.IGNORECASE).strip()
                    sanitized_series_name = re.sub(r'#\d+(?:\.\d+)?(?:,\s*(?:Part|Disc|Volume|Vol)\s*\d+(?:\s+of\s*\d+)?)?', '', sanitized_series_name, flags=re.IGNORECASE).strip()
                    sanitized_series_name = re.sub(r'\s*#\d+(?:\.\d+)?$', '', sanitized_series_name, flags=re.IGNORECASE).strip()
                    sanitized_series_name = re.sub(r'\s+', ' ', sanitized_series_name).strip()
                    custom_print_func(f"  DEBUG: Sanitized series name after OPF-driven cleanup: '{sanitized_series_name}'", level="DEBUG", to_console=False)
        else:
            series_book_num_for_logic = series_number_from_extracted_patterns
            custom_print_func(f"  DEBUG: Using extracted series_book_num: {series_book_num_for_logic}", level="DEBUG", to_console=False)
        
        # Determine the core book title, stripping *both* series and part info for the logical grouping key
        overall_book_title_raw = title.strip() if title else album.strip() if album else os.path.basename(physical_folder_path)
        custom_print_func(f"  DEBUG: overall_book_title_raw: '{overall_book_title_raw}'", level="DEBUG", to_console=False)

        # First, strip series info to get a cleaner base title
        temp_core_title = strip_series_info_from_title(overall_book_title_raw, sanitized_series_name, series_book_num_for_logic)
        custom_print_func(f"  DEBUG: temp_core_title (after series strip): '{temp_core_title}'", level="DEBUG", to_console=False)
        # Then, strip part info from that cleaner title
        final_core_book_title_for_grouping = strip_part_info_from_title(temp_core_title)
        custom_print_func(f"  DEBUG: final_core_book_title_for_grouping (after part strip): '{final_core_book_title_for_grouping}'", level="DEBUG", to_console=False)

        if not final_core_book_title_for_grouping: # Fallback if stripping left it empty
            final_core_book_title_for_grouping = temp_core_title if temp_core_title else overall_book_title_raw
            custom_print_func(f"  DEBUG: final_core_book_title_for_grouping was empty, using fallback: '{final_core_book_title_for_grouping}'", level="DEBUG", to_console=False)

        sanitized_core_book_title_for_grouping = sanitize_filename(final_core_book_title_for_grouping)
        custom_print_func(f"  DEBUG: sanitized_core_book_title_for_grouping: '{sanitized_core_book_title_for_grouping}'", level="DEBUG", to_console=False)

        # Define the key for the logical book (author, series, book_num, publisher)
        logical_book_key = (sanitized_author, sanitized_series_name, series_book_num_for_logic, publisher)
        custom_print_func(f"  DEBUG: Generated logical_book_key for '{os.path.basename(physical_folder_path)}': {logical_book_key}", level="DEBUG", to_console=False)

        if logical_book_key not in logical_books_grouped_by_key:
            logical_books_grouped_by_key[logical_book_key] = []
        
        logical_books_grouped_by_key[logical_book_key].append({
            'physical_folder_path': physical_folder_path,
            'combined_metadata': combined_metadata,
            'book_has_embedded_image': book_has_embedded_image_from_folder,
            'all_audio_files_details_in_folder': all_audio_files_details_in_folder,
            'sanitized_core_book_title_for_grouping': sanitized_core_book_title_for_grouping, # Add this for later use
            'publisher': publisher, # Add publisher for sub-grouping
            'performer': performer # Add performer for ambiguity
        })

    custom_print_func(f"Grouped {len(physical_folder_metadata_results)} physical folders into {len(logical_books_grouped_by_key)} initial logical book keys (including publisher).", to_console=True)

    # --- Refine logical books (handle multi-part books and generate final structure) ---
    final_logical_books_for_processing = [] 

    for logical_book_key, physical_folder_results_list in logical_books_grouped_by_key.items():
        if len(physical_folder_results_list) == 1:
            # This is a unique book (by author, series, book_num, and publisher)
            res = physical_folder_results_list[0]
            sanitized_author = logical_book_key[0]
            sanitized_series_name = logical_book_key[1]
            series_book_num_for_logic = logical_book_key[2]
            publisher = logical_book_key[3] # Get publisher from the key
            sanitized_core_book_title = res['sanitized_core_book_title_for_grouping']

            final_logical_books_for_processing.append({
                'author': sanitized_author,
                'series_name': sanitized_series_name,
                'series_book_num': series_book_num_for_logic,
                'core_book_title': sanitized_core_book_title,
                'publisher': publisher,
                'performer': res['performer'],
                'book_has_embedded_image': res['book_has_embedded_image'],
                'physical_folder_paths': [res['physical_folder_path']],
                'all_audio_files_details': res['all_audio_files_details_in_folder'],
                'is_multi_part': False, # This is a single-part book
                'part_display_name': None 
            })
        else:
            # Multiple physical folders for the same (author, series, book_num, publisher)
            # Assume these are parts of the same book.
            sanitized_author = logical_book_key[0]
            sanitized_series_name = logical_book_key[1]
            series_book_num_for_logic = logical_book_key[2]
            publisher = logical_book_key[3] # Get publisher from the key

            custom_print_func(f"  Info: Detected multi-part book for key {logical_book_key}. Grouping by common title substring.", to_console=False)
            
            # Collect all titles/albums for common substring calculation
            titles_for_common_substring = []
            for res in physical_folder_results_list:
                raw_title = res['combined_metadata'].get('title') or res['combined_metadata'].get('album') or os.path.basename(res['physical_folder_path'])
                # Strip part info *before* finding common substring, so "Book Title Part 1" and "Book Title Part 2" yield "Book Title"
                cleaned_title_for_common = strip_part_info_from_title(raw_title)
                titles_for_common_substring.append(cleaned_title_for_common)
            
            custom_print_func(f"  DEBUG: Titles for common substring detection: {titles_for_common_substring}", level="DEBUG", to_console=False)
            shared_book_title = find_longest_common_substring(titles_for_common_substring)
            custom_print_func(f"  DEBUG: Shared book title found: '{shared_book_title}'", level="DEBUG", to_console=False)

            if not shared_book_title: # Fallback if no significant common substring
                shared_book_title = f"{sanitized_series_name if sanitized_series_name else 'Book'} {series_book_num_for_logic if series_book_num_for_logic is not None else ''}".strip() + " (Multi-Part)"
                custom_print_func(f"  Warning: No significant common substring found for multi-part book. Using fallback title: '{shared_book_title}'", level="WARNING", to_console=True)

            # Create individual logical book entries for each part
            # Sort parts by their extracted part number for consistent ordering
            physical_folder_results_list.sort(key=lambda x: x['combined_metadata'].get('extracted_part_number') if x['combined_metadata'].get('extracted_part_number') is not None else 0)

            for i, res in enumerate(physical_folder_results_list):
                original_physical_folder_name = os.path.basename(res['physical_folder_path'])
                custom_print_func(f"  DEBUG: Processing part of multi-part book. Original physical folder name: '{original_physical_folder_name}'", level="DEBUG", to_console=False)
                
                part_designation = res['combined_metadata'].get('extracted_part_designation')
                part_num = res['combined_metadata'].get('extracted_part_number')
                part_total = res['combined_metadata'].get('extracted_total_parts')
                
                # Determine part_display_name based on user's desired format
                part_display_name = ""
                if part_num is not None and part_total is not None:
                    # Pad part number and total parts for display
                    part_padding = max(len(str(int(part_num))), len(str(int(part_total))))
                    part_display_name = f"({part_designation if part_designation else 'Part'} {int(part_num):0{part_padding}d} of {int(part_total):0{part_padding}d})"
                elif part_num is not None: # Fallback if total isn't known
                    part_display_name = f"({part_designation if part_designation else 'Part'} {int(part_num)})"
                else: # Fallback to generic if no part info at all
                    part_display_name = f"(Part {i+1})" # Generic fallback for part index
                
                custom_print_func(f"  DEBUG: Calculated part_display_name (before sanitization): '{part_display_name}'", level="DEBUG", to_console=False)

                part_info = {
                    'author': sanitized_author,
                    'series_name': sanitized_series_name, # Inherit series name
                    'series_book_num': series_book_num_for_logic, # Inherit book num
                    'core_book_title': sanitize_filename(shared_book_title), # Use the shared title for the part's core title
                    'publisher': res['publisher'],
                    'performer': res['performer'],
                    'book_has_embedded_image': res['book_has_embedded_image'],
                    'physical_folder_paths': [res['physical_folder_path']],
                    'all_audio_files_details': res['all_audio_files_details_in_folder'],
                    'is_multi_part': True, # This is an individual part of a multi-part book
                    'part_display_name': part_display_name, # Name for its sub-folder, including parentheses
                    'part_index_for_multi_part_parent': i+1 # Pass index for fallback if needed
                }
                final_logical_books_for_processing.append(part_info)
    
    # Now, calculate ambiguous_base_names based on the *final* book titles/series names
    ambiguous_base_names = set()
    temp_base_name_tracker = {} # (author, base_name) -> set of (publisher, performer)
    for logical_book_info in final_logical_books_for_processing:
        # For multi-part books, the 'core_book_title' is the shared title for all parts.
        # For single books, it's their own core_book_title.
        # The ambiguity check should be at the level of (author, series_name) if series exists,
        # otherwise (author, core_book_title).
        
        base_name_for_ambiguity_check = logical_book_info['series_name'] if logical_book_info['series_name'] else logical_book_info['core_book_title']
        
        author = logical_book_info['author']
        key = (author, base_name_for_ambiguity_check)
        if key not in temp_base_name_tracker:
            temp_base_name_tracker[key] = set()
        
        normalized_publisher = logical_book_info['publisher'] if logical_book_info['publisher'] else None
        normalized_performer = logical_book_info['performer'] if logical_book_info['performer'] else None
        temp_base_name_tracker[key].add((normalized_publisher, normalized_performer))

    for key, variations in temp_base_name_tracker.items():
        if len(variations) > 1:
            ambiguous_base_names.add(key)
    custom_print_func(f"Identified {len(ambiguous_base_names)} ambiguous (author, name) pairs for final folder naming.", to_console=True)
    custom_print_func(f"  Ambiguous pairs: {ambiguous_base_names}", level="DEBUG", to_console=False)

    # Recalculate series_max_numbers based on the final logical books
    series_max_numbers = {}
    for logical_book_info in final_logical_books_for_processing:
        if logical_book_info['series_name'] and logical_book_info['series_book_num'] is not None:
            sanitized_series_name = logical_book_info['series_name']
            series_number_to_use = logical_book_info['series_book_num']
            if sanitized_series_name not in series_max_numbers:
                series_max_numbers[sanitized_series_name] = series_number_to_use
            else:
                series_max_numbers[sanitized_series_name] = max(series_max_numbers[sanitized_series_name], series_number_to_use)
    
    custom_print_func(f"  DEBUG: series_max_numbers after final logical grouping: {series_max_numbers}", level="DEBUG", to_console=False)

    return final_logical_books_for_processing, series_max_numbers, ambiguous_base_names


def organize_audiobooks_main(source_root_dir, dest_base_dir, log_path, manual_log_path, cache_file_path, target_author=None, target_series=None, force_empty=False):
    global log_file_handle, console_verbose
    start_time = time.time()
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    try:
        log_file_handle = open(log_path, 'w', encoding='utf-8')
        manual_log_file_handle = open(manual_log_path, 'w', encoding='utf-8') # Open manual log here
    except IOError as e:
        print(f"Error: Could not open log files: {e}", file=sys.stderr)
        return
    
    # Set global log handles for the main process and initial setup
    set_global_log_handles(log_file_handle, manual_log_file_handle)

    custom_print(f"--- Starting Audiobook Organization (Script Version: {SCRIPT_VERSION}) ---", to_console=True)
    custom_print(f"Source Root Directory: '{source_root_dir}'", to_console=True)
    custom_print(f"Destination Base Directory: '{dest_base_dir}'", to_console=True)
    parent_of_source = os.path.dirname(source_root_dir)
    source_folder_name = os.path.basename(source_root_dir)
    LEFTBEHIND_BASE_DIR = os.path.join(parent_of_source, f"{source_folder_name}_leftbehind") # Corrected typo
    custom_print(f"Files not organized will be hard-linked to: '{LEFTBEHIND_BASE_DIR}'", to_console=True)
    custom_print(f"Script data (logs, cache) located in: '{os.path.dirname(log_path)}'", to_console=True)
    custom_print("Important: Files will be hard-linked, original files will remain untouched.", to_console=True)
    custom_print("Hard links do not duplicate disk space, but require source and destination to be on the same filesystem.", to_console=True)
    if target_author:
        custom_print(f"Focusing on Author: '{target_author}'", to_console=True)
    if target_series:
        custom_print(f"Focusing on Series: '{target_series}'", to_console=True)
    custom_print("-" * 60, to_console=True)
    try:
        subprocess.run(['ffprobe', '-h'], capture_output=True, check=True)
    except FileNotFoundError:
        error_msg = "Error: 'ffprobe' command not found. Please ensure FFmpeg is installed and ffprobe is in your system's PATH."
        custom_print(error_msg, level="ERROR", to_console=True)
        manual_log_file_handle.write(f"[ERROR] {error_msg}\n")
        manual_log_file_handle.write(f"[ERROR] Organization aborted.\n")
        manual_log_file_handle.flush()
        custom_print("Organization aborted.", level="ERROR", to_console=True)
        log_file_handle.close()
        manual_log_file_handle.close()
        return
    except subprocess.CalledProcessError as e:
        error_msg = f"Warning: ffprobe found but returned an error on help command. It might still work. Error: {e.stderr.strip()}"
        custom_print(error_msg, level="WARNING", to_console=True)
        manual_log_file_handle.write(f"[WARNING] {error_msg}\n")
        manual_log_file_handle.flush()
    if not os.path.isdir(source_root_dir):
        error_msg = f"Error: Source root directory '{source_root_dir}' does not exist or is not a directory."
        custom_print(error_msg, level="ERROR", to_console=True)
        manual_log_file_handle.write(f"[ERROR] {error_msg}\n")
        manual_log_file_handle.write(f"[ERROR] Organization aborted.\n")
        manual_log_file_handle.flush()
        custom_print("Organization aborted.", level="ERROR", to_console=True)
        log_file_handle.close()
        manual_log_file_handle.close()
        return
    if os.path.exists(dest_base_dir):
        custom_print(f"Warning: Destination directory '{dest_base_dir}' already exists.", level="WARNING", to_console=True)
        if not force_empty:
            print("Waiting for your input to proceed...", file=sys.stderr)
            response = input("Do you want to empty its contents before proceeding? (y/N): ").strip().lower()
        else:
            response = 'y'
            custom_print("Proceeding with emptying destination directory due to --force-empty flag.", level="INFO", to_console=True)
        if response == 'y':
            try:
                custom_print(f"Attempting to remove and recreate '{dest_base_dir}'...", to_console=True)
                if os.path.exists(dest_base_dir):
                    shutil.rmtree(dest_base_dir)
                os.makedirs(dest_base_dir, exist_ok=True)
                custom_print(f"Directory '{dest_base_dir}' emptied and recreated.", to_console=True)
            except OSError as e:
                error_msg = f"Error removing or recreating destination directory '{dest_base_dir}': {e}"
                custom_print(error_msg, level="ERROR", to_console=True)
                manual_log_file_handle.write(f"[ERROR] {error_msg}\n")
                manual_log_file_handle.write(f"[ERROR] This might be due to files being in use by another process. Please ensure no other applications are accessing this directory.\n")
                manual_log_file_handle.write(f"[ERROR] Organization aborted.\n")
                manual_log_file_handle.flush()
                custom_print("This might be due to files being in use by another process. Please ensure no other applications are accessing this directory.", level="ERROR", to_console=True)
                custom_print("Organization aborted.", level="ERROR", to_console=True)
                log_file_handle.close()
                manual_log_file_handle.close()
                return
        else:
            info_msg = "Aborting: Destination directory not emptied. Please clear it manually or confirm to proceed."
            custom_print(info_msg, level="INFO", to_console=True)
            manual_log_file_handle.write(f"[INFO] {info_msg}\n")
            manual_log_file_handle.flush()
            log_file_handle.close()
            manual_log_file_handle.close()
            return
    else:
        try:
            os.makedirs(dest_base_dir, exist_ok=True)
        except OSError as e:
            error_msg = f"Error creating destination directory '{dest_base_dir}': {e}"
            custom_print(error_msg, level="ERROR", to_console=True)
            manual_log_file_handle.write(f"[ERROR] {error_msg}\n")
            manual_log_file_handle.write(f"[ERROR] Organization aborted.\n")
            manual_log_file_handle.flush()
            custom_print("Organization aborted.", level="ERROR", to_console=True)
            log_file_handle.close()
            manual_log_file_handle.close()
            return
    if os.path.exists(LEFTBEHIND_BASE_DIR):
        custom_print(f"Warning: Leftbehind directory '{LEFTBEHIND_BASE_DIR}' already exists. Its contents will be cleared for this run.", level="WARNING", to_console=True)
        try:
            custom_print(f"Attempting to remove and recreate '{LEFTBEHIND_BASE_DIR}'...", to_console=True) # Corrected typo
            shutil.rmtree(LEFTBEHIND_BASE_DIR)
            os.makedirs(LEFTBEHIND_BASE_DIR, exist_ok=True)
            custom_print(f"Directory '{LEFTBEHIND_BASE_DIR}' emptied and recreated.", to_console=True)
        except OSError as e:
            error_msg = f"Error removing or recreating leftbehind directory '{LEFTBEHIND_BASE_DIR}': {e}"
            custom_print(error_msg, level="ERROR", to_console=True)
            manual_log_file_handle.write(f"[ERROR] {error_msg}\n")
            manual_log_file_handle.write(f"[ERROR] This might be due to files being in use by another process. Please ensure no other applications are accessing this directory.\n")
            manual_log_file_handle.write(f"[ERROR] Organization aborted.\n")
            manual_log_file_handle.flush()
            custom_print("This might be due to files being in use by another process. Please ensure no other applications are accessing this directory.", level="ERROR", to_console=True)
            custom_print("Organization aborted.", level="ERROR", to_console=True)
            log_file_handle.close()
            manual_log_file_handle.close()
            return
    else:
        try:
            os.makedirs(LEFTBEHIND_BASE_DIR, exist_ok=True)
        except OSError as e:
            error_msg = f"Error creating leftbehind directory '{LEFTBEHIND_BASE_DIR}': {e}"
            custom_print(error_msg, level="ERROR", to_console=True)
            manual_log_file_handle.write(f"[ERROR] {error_msg}\n")
            manual_log_file_handle.write(f"[ERROR] Organization aborted.\n")
            manual_log_file_handle.flush()
            custom_print("Organization aborted.", level="ERROR", to_console=True)
            log_file_handle.close()
            manual_log_file_handle.close()
            return
    try:
        source_stat = os.stat(source_root_dir)
        dest_stat = os.stat(dest_base_dir)
        leftbehind_stat = os.stat(LEFTBEHIND_BASE_DIR)
        if source_stat.st_dev != dest_stat.st_dev or source_stat.st_dev != leftbehind_stat.st_dev:
            error_msg = f"Error: Source root directory '{source_root_dir}', destination directory '{dest_base_dir}', and leftbehind directory '{LEFTBEHIND_BASE_DIR}' are not all on the same filesystem."
            custom_print(error_msg, level="ERROR", to_console=True)
            manual_log_file_handle.write(f"[ERROR] {error_msg}\n")
            manual_log_file_handle.write(f"[ERROR] Hard linking is not possible across different filesystems. Organization aborted.\n")
            manual_log_file_handle.flush()
            custom_print("Hard linking is not possible across different filesystems. Organization aborted.", level="ERROR", to_console=True)
            log_file_handle.close()
            manual_log_file_handle.close()
            return
    except OSError as e:
        error_msg = f"Error checking filesystem for directories: {e}"
        custom_print(error_msg, level="ERROR", to_console=True)
        manual_log_file_handle.write(f"[ERROR] {error_msg}\n")
        manual_log_file_handle.write(f"[ERROR] Organization aborted.\n")
        manual_log_file_handle.flush()
        custom_print("Organization aborted.", level="ERROR", to_console=True)
        log_file_handle.close()
        manual_log_file_handle.close()
        return
    all_audio_manual_logs = []
    all_non_audio_manual_logs = []
    audiobook_cache = {}
    try:
        if os.path.exists(cache_file_path):
            with open(cache_file_path, 'r', encoding='utf-8') as f:
                audiobook_cache = json.load(f)
            custom_print(f"Loaded metadata cache from '{cache_file_path}'.", to_console=False)
    except json.JSONDecodeError as e:
        custom_print(f"Warning: Could not load metadata cache from '{cache_file_path}' (corrupted JSON?): {e}. Starting with empty cache.", level="WARNING", to_console=True)
        audiobook_cache = {}
    except Exception as e:
        custom_print(f"Warning: Unexpected error loading metadata cache from '{cache_file_path}': {e}. Starting with empty cache.", level="WARNING", to_console=True)
        audiobook_cache = {}
    
    # Identify all top-level physical book folders
    all_physical_book_folder_paths = []
    dirs_with_audio = set()
    for root, _, files in os.walk(source_root_dir):
        if os.path.basename(root) == ".audiobook_organizer_data" or os.path.basename(root) == os.path.basename(dest_base_dir) or os.path.basename(root) == os.path.basename(LEFTBEHIND_BASE_DIR):
            continue
        for file in files:
            if file.lower().endswith(AUDIO_EXTENSIONS):
                dirs_with_audio.add(root)
                break
    for d in sorted(list(dirs_with_audio)):
        is_top_level_book = True
        current_dir = d
        while current_dir != source_root_dir and os.path.dirname(current_dir) != source_root_dir:
            parent_dir = os.path.dirname(current_dir)
            if parent_dir in dirs_with_audio:
                is_top_level_book = False
                break
            current_dir = parent_dir
        if is_top_level_book:
            all_physical_book_folder_paths.append(d)
    
    custom_print("Pre-scanning physical book folders to collect initial metadata (parallelized)...", to_console=True)
    physical_folder_metadata_results = [] # Stores results from _get_physical_folder_metadata
    total_folders_to_prescan = len(all_physical_book_folder_paths)
    
    # Pass log file handles to worker processes using initializer and initargs
    # The actual arguments to _get_physical_folder_metadata will NOT include the file handles directly
    pool_args_prescan = [(path, audiobook_cache) for path in all_physical_book_folder_paths]
    num_processes = cpu_count()
    
    with Pool(processes=num_processes, initializer=set_global_log_handles, initargs=(log_file_handle, manual_log_file_handle)) as pool:
        for i, result in enumerate(pool.imap_unordered(_get_physical_folder_metadata, pool_args_prescan)):
            physical_folder_metadata_results.append(result)
            # Update main cache from worker updates
            if result['worker_cache_updates']: # worker_cache_updates
                audiobook_cache.update(result['worker_cache_updates'])
            with print_lock:
                print(f"\rPre-scan Progress: {i+1}/{total_folders_to_prescan} folders scanned ({(((i+1) / total_folders_to_prescan) * 100):.2f}%)", end="")
                sys.stdout.flush()
    print()
    custom_print("Pre-scan complete.", to_console=True)

    custom_print("Grouping physical folders into logical books...", to_console=True)
    logical_books_for_processing, series_max_numbers, ambiguous_base_names = group_physical_folders_into_logical_books(physical_folder_metadata_results, custom_print)
    
    custom_print(f"Found {len(all_physical_book_folder_paths)} unique physical book folders in source. {len(logical_books_for_processing)} logical books (including multi-part parents) will be processed.", to_console=True)
    custom_print(f"Using {num_processes} processes for parallel organization.", to_console=True)
    
    total_linked_to_organized_final = 0
    total_errors_final = 0
    all_successfully_linked_to_organized_source_paths = set()
    unique_books_processed = set() # This will track the *final* top-level book folders created

    # Collect all source files to track which ones are not linked
    all_source_files_found_during_scan = set()
    for physical_folder_path in all_physical_book_folder_paths:
        for root, _, files in os.walk(physical_folder_path):
            for file in files:
                all_source_files_found_during_scan.add(os.path.join(root, file))


    processed_books_count = 0
    total_books_to_process = len(logical_books_for_processing)

    pool_args_process = []
    for logical_book_info in logical_books_for_processing:
        # If it's a multi-part logical book, we need to handle its parent folder creation first
        if logical_book_info.get('is_multi_part'):
            # Calculate parent_book_path for multi-part books here
            sanitized_author = logical_book_info['author']
            sanitized_series_name = logical_book_info['series_name']
            series_book_num_for_folder = logical_book_info['series_book_num']
            sanitized_core_book_title = logical_book_info['core_book_title']
            publisher = logical_book_info['publisher']
            performer = logical_book_info['performer']

            book_or_series_distinguisher_to_apply = ""
            check_base_name = sanitized_series_name if sanitized_series_name else sanitized_core_book_title
            if (sanitized_author, check_base_name) in ambiguous_base_names:
                if publisher:
                    book_or_series_distinguisher_to_apply = f" (Published by {normalize_publisher_name(publisher)})"
                elif performer:
                    book_or_series_distinguisher_to_apply = f" (Narrated by {normalize_publisher_name(performer)})"
            
            dest_author_path = os.path.join(dest_base_dir, sanitized_author)
            
            final_book_folder_name_prefix = ""
            if sanitized_series_name and series_book_num_for_folder is not None:
                max_series_num = series_max_numbers.get(sanitized_series_name, series_book_num_for_folder)
                padding_length_for_series_num = 1
                if isinstance(max_series_num, float):
                    if int(max_series_num) >= 10:
                        padding_length_for_series_num = len(str(int(max_series_num)))
                else:
                    if int(max_series_num) >= 10:
                        padding_length_for_series_num = len(str(int(max_series_num)))

                int_part = int(series_book_num_for_folder)
                frac_part_str = ""
                if isinstance(series_book_num_for_folder, float) and series_book_num_for_folder != int_part:
                    str_series_number = str(series_book_num_for_folder)
                    if '.' in str_series_number:
                        frac_part_str = "." + str_series_number.split('.')[-1]
                
                if padding_length_for_series_num > 1:
                    padded_series_number_str = f"{int_part:0{padding_length_for_series_num}d}{frac_part_str}"
                else:
                    padded_series_number_str = f"{int_part}{frac_part_str}"

                final_book_folder_name_prefix = f"{padded_series_number_str} - "

            current_series_folder_name = sanitized_series_name
            if current_series_folder_name:
                current_series_folder_name = f"{current_series_folder_name}{book_or_series_distinguisher_to_apply}"
            
            if sanitized_series_name:
                dest_series_path = os.path.join(dest_author_path, sanitize_filename(current_series_folder_name))
                parent_book_path_for_parts = os.path.join(dest_series_path, sanitize_filename(f"{final_book_folder_name_prefix}{sanitized_core_book_title}"))
            else:
                parent_book_path_for_parts = os.path.join(dest_author_path, sanitize_filename(f"{final_book_folder_name_prefix}{sanitized_core_book_title}{book_or_series_distinguisher_to_apply}"))
            
            os.makedirs(parent_book_path_for_parts, exist_ok=True)
            custom_print(f"  Created parent folder for multi-part book: '{os.path.relpath(parent_book_path_for_parts, dest_base_dir)}'", to_console=False)
            unique_books_processed.add(os.path.relpath(parent_book_path_for_parts, dest_base_dir))

            # Add each part to the pool_args_process
            for part_info in logical_book_info['parts']:
                pool_args_process.append((part_info, source_root_dir, dest_base_dir, LEFTBEHIND_BASE_DIR, series_max_numbers, ambiguous_base_names, parent_book_path_for_parts))
        else:
            # Add single logical book to the pool_args_process
            pool_args_process.append((logical_book_info, source_root_dir, dest_base_dir, LEFTBEHIND_BASE_DIR, series_max_numbers, ambiguous_base_names, None))

    # Execute processing in parallel
    with Pool(processes=num_processes, initializer=set_global_log_handles, initargs=(log_file_handle, manual_log_file_handle)) as pool:
        for i, result in enumerate(pool.imap_unordered(process_single_logical_book_or_part, pool_args_process)):
            linked_count, errors_count, book_info, audio_logs, non_audio_logs, successfully_linked_paths_from_worker, associated_physical_folders = result
            
            total_linked_to_organized_final += linked_count
            total_errors_final += errors_count
            all_successfully_linked_to_organized_source_paths.update(successfully_linked_paths_from_worker)
            if book_info and not any(book_info.startswith(parent_path) for parent_path in unique_books_processed): # Only add if it's a top-level book, not a sub-part
                unique_books_processed.add(book_info)
            all_audio_manual_logs.extend(audio_logs)
            all_non_audio_manual_logs.extend(non_audio_logs)
            
            # Remove processed files from all_source_files_found_during_scan
            for folder_path in associated_physical_folders:
                for root_inner, _, files_inner in os.walk(folder_path):
                    for file_name_inner in files_inner:
                        file_path_inner = os.path.join(root_inner, file_name_inner)
                        if file_path_inner in all_source_files_found_during_scan:
                            all_source_files_found_during_scan.remove(file_path_inner)

            with print_lock:
                progress_percent = (i+1 / total_books_to_process) * 100 # Adjusted progress for each part/book processed
                print(f"\rOverall Progress: {i+1}/{total_books_to_process} logical books/parts processed ({progress_percent:.2f}%)", end="")
                sys.stdout.flush()
    print() # Newline after progress bar

    custom_print("\n--- Sweeping for unorganized files to move to _leftbehind ---", to_console=True)
    total_unlinked_found_in_sweep = 0
    
    # Any files remaining in all_source_files_found_during_scan were not part of any logical book processed
    for src_file_path in all_source_files_found_during_scan:
        reason = "File not organized into main structure."
        if hard_link_to_leftbehind(src_file_path, source_root_dir, LEFTBEHIND_BASE_DIR, reason=reason, manual_log_list=all_non_audio_manual_logs, level="INFO"):
            total_unlinked_found_in_sweep += 1
        else:
            total_errors_final += 1
            
    custom_print(f"Sweep complete. Found and hard-linked {total_unlinked_found_in_sweep} files to '{LEFTBEHIND_BASE_DIR}'.", to_console=True)
    end_time = time.time()
    total_runtime_seconds = end_time - start_time
    hours, remainder = divmod(total_runtime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    custom_print("-" * 60, to_console=True)
    custom_print(f"Organization complete. Unique books organized: {len(unique_books_processed)}", to_console=True)
    custom_print(f"Total files hard-linked to organized directory: {total_linked_to_organized_final}", to_console=True)
    custom_print(f"Total files hard-linked to leftbehind directory: {total_unlinked_found_in_sweep}", to_console=True)
    custom_print(f"Total errors encountered during processing: {total_errors_final}", to_console=True)
    custom_print(f"New organized library root is: '{dest_base_dir}'", to_console=True)
    custom_print(f"Files not hard-linked to the organized directory can be found in: '{LEFTBEHIND_BASE_DIR}'", to_console=True)
    custom_print(f"Total execution time: {int(hours)}h {int(minutes)}m {seconds:.2f}s", to_console=True)
    custom_print("Please verify the new structure. Remember that hard links share data with originals.", to_console=True)
    
    # Generate ls -R outputs
    organized_ls_output_path = os.path.join(os.path.dirname(log_path), "organized_ls_result.txt")
    leftbehind_ls_output_path = os.path.join(os.path.dirname(log_path), "leftbehind_ls_result.txt")
    generate_ls_output(dest_base_dir, organized_ls_output_path, custom_print)
    generate_ls_output(LEFTBEHIND_BASE_DIR, leftbehind_ls_output_path, custom_print)

    try:
        with open(manual_log_path, 'w', encoding='utf-8') as f:
            f.write("--- Audio-Related Manual Actions ---\n")
            if not all_audio_manual_logs:
                f.write("[INFO] No audio-related manual actions required.\n")
            for log_entry in all_audio_manual_logs:
                f.write(log_entry + "\n")
            f.write("\n--- Non-Audio/Image File Manual Actions (Including Unorganized Files) ---\n")
            if not all_non_audio_manual_logs:
                f.write("[INFO] No non-audio/image file manual actions required.\n")
            for log_entry in all_non_audio_manual_logs:
                f.write(log_entry + "\n")
    except IOError as e:
        custom_print(f"Error: Could not write to manual log file '{manual_log_path}': {e}", level="ERROR", to_console=True)
    try:
        with open(cache_file_path, 'w', encoding='utf-8') as f:
            json.dump(audiobook_cache, f, indent=4)
        custom_print(f"Saved updated metadata cache to '{cache_file_path}'.", level="INFO", to_console=False)
    except IOError as e:
        custom_print(f"Error: Could not save metadata cache to '{cache_file_path}': {e}", level="ERROR", to_console=True)
    finally: # Ensure log files are closed even if errors occur
        if log_file_handle:
            log_file_handle.close()
        if manual_log_file_handle:
            manual_log_file_handle.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Organize audiobook files based on metadata.")
    parser.add_argument('--source-path', type=str, required=True, help="The root directory containing your messy audiobooks.")
    parser.add_argument('--author', type=str, help="Focus organization on a specific author.")
    parser.add_argument('--series', type=str, help="Focus organization on a specific series (requires --author if series name is not unique).")
    parser.add_argument('--force-empty', action='store_true', help="Force emptying of destination directories without prompt.")
    args = parser.parse_args()
    SOURCE_ROOT_DIR = os.path.normpath(args.source_path)
    parent_of_source = os.path.dirname(SOURCE_ROOT_DIR)
    source_folder_name = os.path.basename(SOURCE_ROOT_DIR)
    DEST_BASE_DIR = os.path.join(parent_of_source, f"{source_folder_name}_organized")
    DATA_DIR = os.path.join(parent_of_source, ".audiobook_organizer_data")
    LOG_FILE_PATH = os.path.join(DATA_DIR, f"organize_audiobooks_v{SCRIPT_VERSION}.log")
    MANUAL_LOG_FILE_PATH = os.path.join(DATA_DIR, f"manual_actions_required_v{SCRIPT_VERSION}.log")
    CACHE_FILE_PATH = os.path.join(DATA_DIR, "audiobook_metadata_cache.json")
    try:
        organize_audiobooks_main(SOURCE_ROOT_DIR, DEST_BASE_DIR, LOG_FILE_PATH, MANUAL_LOG_FILE_PATH, CACHE_FILE_PATH,
                                 target_author=args.author, target_series=args.series, force_empty=args.force_empty)
    except Exception as e:
        print(f"\n[CRITICAL ERROR] An unhandled exception occurred during script execution:", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        print(f"\n[CRITICAL ERROR] Please check '{LOG_FILE_PATH}' and '{MANUAL_LOG_FILE_PATH}' for more details.", file=sys.stderr)
        sys.stderr.flush()
