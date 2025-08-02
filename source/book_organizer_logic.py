import os
import re
from collections import defaultdict

# Import functions from other modules directly for multiprocessing workers
from metadata_utils import get_audio_metadata_and_embedded_image_status, parse_opf_metadata, extract_series_info, extract_internal_part_info, strip_series_info_from_title, strip_part_info_from_title, normalize_publisher_name
from file_system_utils import custom_print, sanitize_filename, hard_link_to_leftbehind

# SCRIPT VERSION - Increment this each time the script is modified and sent
SCRIPT_VERSION = "1.0.36" # Incremented version for this change

AUDIO_EXTENSIONS = ('.mp3', '.m4a', '.m4b')
IMAGE_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.gif', '.webp')

def _get_physical_folder_metadata(args):
    """
    Worker function for multiprocessing pool to get metadata for a single physical folder.
    Args:
        args (tuple): A tuple containing (physical_folder_path, audiobook_cache).
    Returns:
        dict: A dictionary containing metadata and processing results for the folder.
    """
    physical_folder_path, audiobook_cache = args # Correct unpacking from single tuple
    
    folder_name = os.path.basename(physical_folder_path)
    custom_print(f"  Info: Pre-scanning folder: '{folder_name}'", to_console=False)

    combined_metadata = None
    book_has_embedded_image = False
    all_audio_files_details_in_folder = []
    worker_cache_updates = {} # Cache updates specific to this worker

    audio_files_in_folder = [
        f for f in os.listdir(physical_folder_path)
        if os.path.isfile(os.path.join(physical_folder_path, f)) and f.lower().endswith(AUDIO_EXTENSIONS)
    ]
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
    opf_file = next((f for f in os.listdir(physical_folder_path) if f.lower().endswith('.opf')), None)
    opf_metadata = {}
    if opf_file:
        opf_path = os.path.join(physical_folder_path, opf_file)
        custom_print(f"  DEBUG: Found OPF file: '{opf_file}' in '{folder_name}'", level="DEBUG", to_console=False)
        opf_metadata = parse_opf_metadata(opf_path, custom_print)
        if opf_metadata:
            custom_print(f"  DEBUG: OPF metadata for '{folder_name}': {opf_metadata}", level="DEBUG", to_console=False)
            
    # Process each audio file for its metadata
    for audio_file_name in audio_files_in_folder:
        audio_file_path = os.path.join(physical_folder_path, audio_file_name)
        
        # Check cache first
        if audio_file_path in audiobook_cache:
            file_metadata = audiobook_cache[audio_file_path]['metadata']
            has_embedded_image = audiobook_cache[audio_file_path]['has_embedded_image']
            custom_print(f"  Info: Using cached metadata for '{audio_file_name}'", to_console=False)
        else:
            file_metadata, has_embedded_image = get_audio_metadata_and_embedded_image_status(audio_file_path, custom_print)
            if file_metadata:
                worker_cache_updates[audio_file_path] = {
                    'metadata': file_metadata,
                    'has_embedded_image': has_embedded_image
                }
            custom_print(f"  DEBUG: Fresh metadata for '{audio_file_name}': {file_metadata}", level="DEBUG", to_console=False)

        if file_metadata:
            all_audio_files_details_in_folder.append({
                'file_path': audio_file_path,
                'metadata': file_metadata,
                'has_embedded_image': has_embedded_image
            })
            if has_embedded_image:
                book_has_embedded_image = True
        else:
            custom_print(f"  Warning: Could not extract metadata for audio file: '{audio_file_name}'. It will be treated as unorganized.", level="WARNING", to_console=True)

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
    series_max_numbers, ambiguous_base_names, parent_book_path = args # Correct unpacking from single tuple

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
    part_number_int = logical_book_info.get('part_number_int') # Integer part number for sorting/padding
    total_parts_int = logical_book_info.get('total_parts_int') # Integer total parts for padding

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
            # If there's a float in the series, we might need padding for integer parts to align
            # e.g., if max is 10.5, then 1.0 would be 01.0. But user wants 2.5, not 02.5.
            # So, only pad if the integer part of max_series_num is >= 10.
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
    base_book_path = ""
    if sanitized_series_name:
        # Series folder
        current_series_folder_name = f"{sanitized_series_name}{book_or_series_distinguisher_to_apply}"
        dest_series_path = os.path.join(dest_author_path, current_series_folder_name)
        os.makedirs(dest_series_path, exist_ok=True)
        custom_print(f"  Info: Created series folder: '{os.path.relpath(dest_series_path, dest_base_dir)}'", to_console=False)
        base_book_path = os.path.join(dest_series_path, f"{final_book_folder_name_prefix}{sanitized_core_book_title}")
    else:
        # No series, book folder directly under author
        base_book_path = os.path.join(dest_author_path, f"{final_book_folder_name_prefix}{sanitized_core_book_title}{book_or_series_distinguisher_to_apply}")

    # Now, determine the final destination path for the current part/book
    dest_book_path = base_book_path
    if logical_book_info.get('is_multi_part') and part_display_name:
        # For multi-part books, create a subfolder for each part
        # Ensure part_display_name is clean for folder creation, e.g., "(1 of 5)"
        clean_part_folder_name = part_display_name.strip('()')
        dest_book_path = os.path.join(base_book_path, clean_part_folder_name)
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
                custom_print(f"  Warning: Could not parse track number '{track_num_raw}' for '{os.path.basename(src_audio_file_path)}'.", level="WARNING", to_console=False) # Changed to to_console=False
        
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
        # Omit track info if it's a single track (num_audio_files_in_part == 1)
        if num_audio_files_in_part > 1: 
            if track_num is not None and track_total is not None:
                track_info_for_filename = f" Track {track_num:0{padding}d} of {track_total:0{padding}d}"
            elif track_num is not None: # Fallback if total tracks is missing, but individual track number exists
                track_info_for_filename = f" Track {track_num:0{padding}d}"
            else: # Fallback if no track number is found, use an index if multiple files in THIS part
                # Sort files by name to ensure consistent indexing if track metadata is missing
                sorted_audio_files_paths = sorted([d['file_path'] for d in all_audio_files_details_for_this_part])
                current_file_index = sorted_audio_files_paths.index(src_audio_file_path) + 1
                track_info_for_filename = f" Track {current_file_index:0{padding}d} of {num_audio_files_in_part:0{padding}d}"
                custom_print(f"  DEBUG: Using generated track index {current_file_index} for '{os.path.basename(src_audio_file_path)}'", level="DEBUG", to_console=False)

        # Determine the file title for the audio file name
        base_file_name_for_audio = sanitized_core_book_title
        if logical_book_info.get('is_multi_part') and part_display_name:
            # For multi-part books, the audio file name should start with the core book title and then the part info
            # Reconstruct the part info without the outer parentheses for the filename
            # e.g., "(Part 01 of 05)" -> "Part 1 of 5"
            part_info_for_audio_filename = part_display_name.strip('()')
            base_file_name_for_audio = f"{sanitized_core_book_title} {part_info_for_audio_filename}"
            custom_print(f"  DEBUG: Generated base_file_name_for_audio for multi-part: '{base_file_name_for_audio}'", level="DEBUG", to_console=False)
        else:
            # For single-part books, use the cleaned_file_title if it's more specific, otherwise core_book_title
            original_file_title = audio_metadata.get('title') or os.path.splitext(os.path.basename(src_audio_file_path))[0]
            temp_file_title_for_audio_name = strip_series_info_from_title(original_file_title, sanitized_series_name, series_book_num_for_folder)
            cleaned_file_title_for_audio_name = strip_part_info_from_title(temp_file_title_for_audio_name)
            
            if not cleaned_file_title_for_audio_name or cleaned_file_title_for_audio_name.lower() in [sanitized_core_book_title.lower(), 'chapter', 'track', 'part']:
                base_file_name_for_audio = sanitized_core_book_title
            else:
                base_file_name_for_audio = cleaned_file_title_for_audio_name
            custom_print(f"  DEBUG: Generated base_file_name_for_audio for single-part: '{base_file_name_for_audio}'", level="DEBUG", to_console=False)


        final_audio_file_name = sanitize_filename(f"{base_file_name_for_audio}{track_info_for_filename}{os.path.splitext(src_audio_file_path)[1]}")
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
                    
                    dest_image_path = os.path.join(dest_book_path, f"{cover_image_base_name} Cover{os.path.splitext(file_name)[1]}")
                    
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
            
            elif file_name.lower().endswith(('.epub', '.pdf', '.txt')) and src_file_path not in successfully_linked_paths: # Handle EPUBs and PDFs explicitly
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

            elif not file_name.lower().endswith(AUDIO_EXTENSIONS + IMAGE_EXTENSIONS + ('.opf', '.epub', '.pdf', '.txt')) and src_file_path not in successfully_linked_paths: # Catch any other unhandled files
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
    
    return linked_count, errors_count, final_book_path_relative_to_dest, audio_manual_logs, non_audio_manual_logs, successfully_linked_paths, associated_physical_folders

def find_longest_common_substring(strings):
    """
    Finds the longest common substring among a list of strings.
    This is used to determine the 'core' title for multi-part books.
    Args:
        strings (list): A list of strings.
    Returns:
        str: The longest common substring.
    """
    if not strings:
        return ""
    
    if len(strings) == 1:
        return strings[0]

    # Convert all strings to lowercase for case-insensitive comparison
    lower_strings = [s.lower() for s in strings]

    s1 = lower_strings[0]
    min_len = len(s1)
    
    longest_common = ""
    
    for i in range(min_len):
        for j in range(i + 1, min_len + 1):
            substring = s1[i:j]
            is_common = True
            for k in range(1, len(lower_strings)):
                if substring not in lower_strings[k]:
                    is_common = False
                    break
            if is_common and len(substring) > len(longest_common):
                longest_common = substring
    
    # Attempt to return the original casing from one of the input strings
    # that contains the longest common substring.
    if longest_common:
        for original_s in strings:
            if longest_common.lower() in original_s.lower():
                # Find the exact substring in the original string
                match = re.search(re.escape(longest_common), original_s, re.IGNORECASE)
                if match:
                    return match.group(0)
    
    return longest_common
