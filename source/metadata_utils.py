import os
import re
import xml.etree.ElementTree as ET
from mutagen.mp3 import MP3, EasyMP3
from mutagen.m4a import M4A # Keep M4A for specific M4A tag handling if needed
from mutagen.mp4 import MP4 # Explicitly import MP4 for broader MP4 container support
from mutagen.id3 import ID3NoHeaderError, ID3, TXXX, TORY
from mutagen.mp4 import MP4Tags, MP4Cover, AtomDataType
from PIL import Image
import io

# Import custom_print and sanitize_filename from file_system_utils
from file_system_utils import custom_print, sanitize_filename

# SCRIPT VERSION - Increment this each time the script is modified and sent
SCRIPT_VERSION = "1.0.37" # Incremented version for this change

AUDIO_EXTENSIONS = ('.mp3', '.m4a', '.m4b')
IMAGE_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.gif', '.webp')

def _get_tag_value(tag_obj):
    """Safely extracts string value from a Mutagen tag object."""
    if tag_obj is None:
        return None
    if isinstance(tag_obj, (list, tuple)):
        return str(tag_obj[0]) if tag_obj else None
    return str(tag_obj)

def get_audio_metadata_and_embedded_image_status(file_path, custom_print_func):
    """
    Extracts metadata from an audio file using Mutagen and checks for embedded cover art.
    Prioritizes 'performer' as author if 'artist' is generic.
    Args:
        file_path (str): The path to the audio file.
        custom_print_func (function): The logging function.
    Returns:
        tuple: (dict of metadata, bool indicating if embedded image exists)
    """
    metadata = {}
    has_embedded_image = False
    try:
        if file_path.lower().endswith('.mp3'):
            audio = MP3(file_path, ID3=EasyMP3)
            # Check for ID3v2 tags first
            if audio.tags:
                for key, value in audio.tags.items():
                    # Common tags: album, artist, title, genre, comment, grouping, description, TIT3, TRACKTOTAL
                    if key.startswith('album'):
                        metadata['album'] = _get_tag_value(value)
                    elif key.startswith('artist'):
                        metadata['artist'] = _get_tag_value(value)
                    elif key.startswith('title'):
                        metadata['title'] = _get_tag_value(value)
                    elif key.startswith('genre'):
                        metadata['genre'] = _get_tag_value(value)
                    elif key.startswith('comment'):
                        metadata['comment'] = _get_tag_value(value)
                    elif key.startswith('grouping'):
                        metadata['grouping'] = _get_tag_value(value)
                    elif key.startswith('desc') or key.startswith('description'):
                        metadata['description'] = _get_tag_value(value)
                    elif key.startswith('TIT3'): # Subtitle
                        metadata['TIT3'] = _get_tag_value(value)
                    elif key.startswith('TRCK') or key.startswith('track'): # Track number
                        metadata['track'] = _get_tag_value(value)
                    elif key.startswith('TPUB') or key.startswith('publisher'): # Publisher
                        metadata['publisher'] = _get_tag_value(value)
                    elif key.startswith('TPE4') or key.startswith('performer'): # Performer/Narrator
                        metadata['performer'] = _get_tag_value(value)
                    elif key.startswith('TORY') or key.startswith('originalyear'): # Original Release Year
                        metadata['original_release_year'] = _get_tag_value(value)
                    elif key.startswith('TDRC') or key.startswith('date'): # Recording Date
                        metadata['date'] = _get_tag_value(value)
                    elif key.startswith('TXXX'): # Custom TXXX frames
                        if hasattr(value, 'desc') and hasattr(value, 'text'):
                            for text_item in value.text:
                                if value.desc.lower() == 'tracktotal':
                                    metadata['TRACKTOTAL'] = text_item
                                elif value.desc.lower() == 'series':
                                    metadata['series'] = text_item
                                elif value.desc.lower() == 'series_book_num':
                                    try:
                                        metadata['series_book_num'] = float(text_item)
                                    except ValueError:
                                        pass # Ignore if not a valid number
                                elif value.desc.lower() == 'publisher': # Fallback for TXXX publisher
                                    if 'publisher' not in metadata:
                                        metadata['publisher'] = text_item
                                elif value.desc.lower() == 'performer': # Fallback for TXXX performer
                                    if 'performer' not in metadata:
                                        metadata['performer'] = text_item
                                elif value.desc.lower() == 'copyright': # Fallback for TXXX copyright
                                    if 'copyright' not in metadata:
                                        metadata['copyright'] = text_item
                                elif value.desc.lower() == 'composer': # Fallback for TXXX composer
                                    if 'composer' not in metadata:
                                        metadata['composer'] = text_item
                                elif value.desc.lower() == 'narratedby': # Fallback for TXXX narratedby
                                    if 'narratedby' not in metadata:
                                        metadata['narratedby'] = text_item
                                elif value.desc.lower() == 'woas': # Work or Series
                                    if 'woas' not in metadata:
                                        metadata['woas'] = text_item

                # Check for embedded images
                if 'APIC:' in audio.tags:
                    has_embedded_image = True

        elif file_path.lower().endswith(('.m4a', '.m4b')):
            # Use MP4 for broader compatibility with M4B files
            audio = MP4(file_path)
            # M4A/MP4 tags are usually more consistent
            if '\xa9alb' in audio: metadata['album'] = _get_tag_value(audio['\xa9alb'])
            if '\xa9ART' in audio: metadata['artist'] = _get_tag_value(audio['\xa9ART'])
            if '\xa9nam' in audio: metadata['title'] = _get_tag_value(audio['\xa9nam'])
            if '\xa9gen' in audio: metadata['genre'] = _get_tag_value(audio['\xa9gen'])
            if '\xa9cmt' in audio: metadata['comment'] = _get_tag_value(audio['\xa9cmt'])
            if '----:com.apple.iTunes:grouping' in audio: metadata['grouping'] = _get_tag_value(audio['----:com.apple.iTunes:grouping'])
            if '\xa9des' in audio: metadata['description'] = _get_tag_value(audio['\xa9des'])
            if 'trkn' in audio: 
                track_info = audio['trkn'][0]
                metadata['track'] = f"{track_info[0]}/{track_info[1]}" if len(track_info) > 1 and track_info[1] else str(track_info[0])
            if 'disk' in audio: 
                disc_info = audio['disk'][0]
                metadata['disc'] = f"{disc_info[0]}/{disc_info[1]}" if len(disc_info) > 1 and disc_info[1] else str(disc_info[0])
            if '\xa9wrt' in audio: metadata['composer'] = _get_tag_value(audio['\xa9wrt']) # Composer often used for publisher/narrator in audiobooks
            if 'cprt' in audio: metadata['copyright'] = _get_tag_value(audio['cprt'])
            if '\xa9pub' in audio: metadata['publisher'] = _get_tag_value(audio['\xa9pub']) # Standard publisher tag
            if '----:com.apple.iTunes:publisher' in audio: metadata['publisher'] = _get_tag_value(audio['----:com.apple.iTunes:publisher']) # iTunes specific publisher
            if '----:com.apple.iTunes:performer' in audio: metadata['performer'] = _get_tag_value(audio['----:com.apple.iTunes:performer']) # iTunes specific performer
            if '\xa9day' in audio: metadata['date'] = _get_tag_value(audio['\xa9day'])

            # Check for embedded images
            if 'covr' in audio and audio['covr']:
                has_embedded_image = True
        
        # Post-processing: Prioritize performer as author if 'artist' is generic
        if metadata.get('artist', '').lower() in ['various artists', 'unknown artist', ''] and metadata.get('performer'):
            custom_print_func(f"  DEBUG: Prioritizing 'performer' ('{metadata['performer']}') over generic 'artist' ('{metadata.get('artist')}') for author.", level="DEBUG", to_console=False)
            metadata['author'] = metadata['performer']
        elif 'artist' in metadata:
            metadata['author'] = metadata['artist'] # Default to artist as author
        
        # Fallback for publisher using copyright if publisher tag is missing
        if not metadata.get('publisher') and metadata.get('copyright'):
            custom_print_func(f"  DEBUG: Using copyright '{metadata['copyright']}' as fallback for publisher.", level="DEBUG", to_console=False)
            metadata['publisher'] = metadata['copyright']

    except ID3NoHeaderError:
        custom_print_func(f"  Warning: No ID3 tag found for MP3 file: '{file_path}'.", level="WARNING", to_console=False) # Keep warnings in log, not console by default
    except Exception as e:
        custom_print_func(f"  Error reading metadata from '{file_path}': {e}", level="ERROR", to_console=True) # Errors should always go to console
    return metadata, has_embedded_image

def parse_opf_metadata(opf_file_path, custom_print_func):
    """
    Parses metadata from an OPF (Open Packaging Format) file.
    Args:
        opf_file_path (str): The path to the OPF file.
        custom_print_func (function): The logging function.
    Returns:
        dict: A dictionary of extracted metadata.
    """
    metadata = {}
    try:
        tree = ET.parse(opf_file_path)
        root = tree.getroot()
        
        # Define namespaces
        namespaces = {
            'opf': 'http://www.idpf.org/2007/opf',
            'dc': 'http://purl.org/dc/elements/1.1/',
            'dcterms': 'http://purl.org/dc/terms/'
        }

        # Extract common metadata
        title_element = root.find('.//dc:title', namespaces)
        if title_element is not None:
            metadata['title'] = title_element.text

        creator_element = root.find('.//dc:creator', namespaces)
        if creator_element is not None:
            metadata['artist'] = creator_element.text
            metadata['author'] = creator_element.text # Also set as author

        publisher_element = root.find('.//dc:publisher', namespaces)
        if publisher_element is not None:
            metadata['publisher'] = publisher_element.text

        date_element = root.find('.//dc:date', namespaces)
        if date_element is not None:
            metadata['date'] = date_element.text

        description_element = root.find('.//dc:description', namespaces)
        if description_element is not None:
            metadata['description'] = description_element.text

        # Specific for series and series_book_num (often in meta tags or custom elements)
        for meta in root.findall('.//opf:meta', namespaces):
            if meta.get('property') == 'belongs-to-series':
                metadata['series'] = meta.text
            if meta.get('property') == 'series-index':
                try:
                    metadata['series_book_num'] = float(meta.text)
                except ValueError:
                    pass # Ignore if not a valid number
        
        custom_print_func(f"  DEBUG: OPF metadata for '{os.path.basename(opf_file_path)}': {metadata}", level="DEBUG", to_console=False)

    except ET.ParseError as e:
        custom_print_func(f"  Warning: Could not parse OPF file '{opf_file_path}': {e}", level="WARNING", to_console=False) # Keep warnings in log, not console by default
    except Exception as e:
        custom_print_func(f"  Error reading OPF metadata from '{opf_file_path}': {e}", level="ERROR", to_console=True) # Errors should always go to console
    return metadata

def extract_series_info(grouping, album, title):
    """
    Extracts series name and book number from various metadata fields.
    Prioritizes 'grouping' then 'album' then 'title'.
    Handles common patterns like "Series Name #X", "Series Name, Book X", "Series X - Book Name".
    Args:
        grouping (str): The grouping metadata field.
        album (str): The album metadata field.
        title (str): The title metadata field.
    Returns:
        tuple: (sanitized_series_name, series_book_num)
    """
    potential_series_strings = [grouping, album, title]
    
    # Regex to capture series name and number
    # Group 1: Series Name (non-greedy)
    # Group 2: Book Number (can be integer or float)
    # This regex is ordered to prioritize common patterns.
    series_patterns = [
        re.compile(r'^(.*?)\s*[#S]\s*(\d+(?:\.\d+)?)\s*[-–—:]?\s*(.*)?$', re.IGNORECASE), # "Series #X", "Series S X"
        re.compile(r'^(.*?),\s*(?:Book|Vol|Volume|Part)\s*(\d+(?:\.\d+)?)\s*[-–—:]?\s*(.*)?$', re.IGNORECASE), # "Series, Book X"
        re.compile(r'^(.*?)\s*[-–—:]\s*(?:Book|Vol|Volume|Part)\s*(\d+(?:\.\d+)?)\s*[-–—:]?\s*(.*)?$', re.IGNORECASE), # "Series - Book X"
        re.compile(r'^(.*?)\s*(\d+(?:\.\d+)?)\s*[-–—:]?\s*(.*)?$', re.IGNORECASE) # "Series X" (most generic, last)
    ]

    series_name = None
    series_book_num = None

    for s in potential_series_strings:
        if not s:
            continue
        
        for pattern in series_patterns:
            match = pattern.match(s.strip())
            if match:
                # Prioritize a non-empty series name from the match
                extracted_name = match.group(1).strip()
                if extracted_name:
                    series_name = extracted_name
                
                # Extract book number
                try:
                    series_book_num = float(match.group(2))
                except (ValueError, IndexError):
                    series_book_num = None # Could not parse as float
                
                # If we found both, we can break
                if series_name and series_book_num is not None:
                    break
        if series_name and series_book_num is not None:
            break

    # Final sanitization of series name
    if series_name:
        # Remove common trailing book/volume indicators if they weren't part of the number extraction
        series_name = re.sub(r'(?:,\s*)?(?:Book|Vol|Volume|Part)\s*(\d+(?:\.\d+)?)', '', series_name, flags=re.IGNORECASE).strip()
        series_name = re.sub(r'(\s*[-–—:]\s*)?(?:Book|Vol|Volume|Part)\s*(\d+(?:\.\d+)?)', '', series_name, flags=re.IGNORECASE).strip()
        # Remove trailing numbers that might be mistaken for series numbers if not already captured
        series_name = re.sub(r'\s*(\d+(?:\.\d+)?)$', '', series_name).strip()
        series_name = sanitize_filename(series_name)

    return series_name, series_book_num

def extract_internal_part_info(folder_name):
    """
    Extracts part information (e.g., "Part 1 of 5", "1 of 5", "XofY") from a folder name.
    Args:
        folder_name (str): The name of the physical folder.
    Returns:
        tuple: (part_designation, part_number, total_parts)
               part_designation (str): "Part", "Disc", etc., or None
               part_number (float): The extracted part number, or None
               total_parts (int): The total number of parts, or None
    """
    # Pattern for " (Part X of Y)", "(X of Y)", "(Disc X of Y)", "(XofY)"
    # Group 1: Optional designation like "Part", "Disc", "Volume"
    # Group 2: Part number (can be integer or float)
    # Group 3: Total parts
    patterns = [
        # (Part X of Y), (Disc X of Y), (Volume X of Y)
        re.compile(r'\((?:(Part|Disc|Volume)\s+)?(\d+(?:\.\d+)?)\s+of\s+(\d+)\)', re.IGNORECASE),
        # (X of Y) or (XofY) - more generic, captures "2of5"
        re.compile(r'\((\d+(?:\.\d+)?)\s*(?:of)?\s*(\d+)\)', re.IGNORECASE), 
        # (X) - for single part indication
        re.compile(r'\((\d+(?:\.\d+)?)\)', re.IGNORECASE) 
    ]

    part_designation = None
    part_number = None
    total_parts = None

    for pattern in patterns:
        match = pattern.search(folder_name)
        if match:
            # Safely get groups, defaulting to None if not present
            # The number of groups can vary based on the matched pattern
            g1 = match.group(1) if len(match.groups()) >= 1 else None
            g2 = match.group(2) if len(match.groups()) >= 2 else None
            g3 = match.group(3) if len(match.groups()) >= 3 else None

            # Determine which groups correspond to part_num_str and total_parts_str
            part_num_str = None
            total_parts_str = None

            if pattern == patterns[0]: # (Designation X of Y)
                part_designation = g1.capitalize() if g1 else "Part"
                part_num_str = g2
                total_parts_str = g3
            elif pattern == patterns[1]: # (X of Y) or (XofY)
                part_designation = "Part" # Default to "Part"
                part_num_str = g1
                total_parts_str = g2
            elif pattern == patterns[2]: # (X)
                part_designation = "Part" # Default to "Part"
                part_num_str = g1
                total_parts_str = None # No total parts specified

            try:
                if part_num_str is not None: # Only attempt conversion if string is not None
                    part_number = float(part_num_str)
            except ValueError:
                part_number = None
            
            if total_parts_str:
                try:
                    total_parts = int(total_parts_str)
                except ValueError:
                    total_parts = None
            
            if part_number is not None: # Found valid part info, break
                break

    return part_designation, part_number, total_parts

def strip_series_info_from_title(title, series_name, series_book_num):
    """
    Strips series name and book number from a title string.
    This helps in getting a cleaner 'core' book title.
    Args:
        title (str): The original title string.
        series_name (str): The extracted series name.
        series_book_num (float): The extracted series book number.
    Returns:
        str: The title with series info removed.
    """
    if not title:
        return ""
    
    cleaned_title = title
    
    # Remove series name if present at the beginning or end
    if series_name:
        # Escape series_name for regex
        escaped_series_name = re.escape(series_name)
        # Remove "Series Name #X" or "Series Name, Book X" patterns
        cleaned_title = re.sub(rf'^{escaped_series_name}\s*[#S]\s*\d+(\.\d+)?\s*[-–—:]?\s*', '', cleaned_title, flags=re.IGNORECASE).strip()
        cleaned_title = re.sub(rf'^{escaped_series_name},\s*(?:Book|Vol|Volume|Part)\s*\d+(\.\d+)?\s*[-–—:]?\s*', '', cleaned_title, flags=re.IGNORECASE).strip()
        cleaned_title = re.sub(rf'^{escaped_series_name}\s*[-–—:]\s*(?:Book|Vol|Volume|Part)\s*\d+(\.\d+)?\s*[-–—:]?\s*(.*)?$', '', cleaned_title, flags=re.IGNORECASE).strip()
        
        # Also remove if series name appears at the end
        cleaned_title = re.sub(rf'\s*[-–—:]?\s*{escaped_series_name}\s*[#S]\s*\d+(\.\d+)?$', '', cleaned_title, flags=re.IGNORECASE).strip()
        cleaned_title = re.sub(rf'\s*,\s*{escaped_series_name},\s*(?:Book|Vol|Volume|Part)\s*\d+(\.\d+)?$', '', cleaned_title, flags=re.IGNORECASE).strip()
        cleaned_title = re.sub(rf'\s*[-–—:]\s*{escaped_series_name}\s*[-–—:]\s*(?:Book|Vol|Volume|Part)\s*\d+(\.\d+)?$', '', cleaned_title, flags=re.IGNORECASE).strip()

        # Remove just the series name if it's a standalone prefix/suffix
        cleaned_title = re.sub(rf'^{escaped_series_name}\s*[-–—:]?\s*', '', cleaned_title, flags=re.IGNORECASE).strip()
        cleaned_title = re.sub(rf'\s*[-–—:]?\s*{escaped_series_name}$', '', cleaned_title, flags=re.IGNORECASE).strip()


    # Remove standalone book number if present
    if series_book_num is not None:
        # Convert to string to handle both int and float
        book_num_str = str(series_book_num)
        # Remove patterns like " - 1", " (1)", "Book 1"
        cleaned_title = re.sub(rf'\s*[-–—:]?\s*{re.escape(book_num_str)}(\s*of\s*\d+)?$', '', cleaned_title, flags=re.IGNORECASE).strip()
        cleaned_title = re.sub(rf'\s*\((?:Book|Vol|Volume|Part)?\s*{re.escape(book_num_str)}(\s*of\s*\d+)?\)', '', cleaned_title, flags=re.IGNORECASE).strip()
        
    return cleaned_title.strip()

def strip_part_info_from_title(title):
    """
    Strips part information (e.g., " (Part 1 of 5)", " (1 of 5)") from a title string.
    Args:
        title (str): The original title string.
    Returns:
        str: The title with part info removed.
    """
    if not title:
        return ""
    
    # Regex to capture and remove part indicators like " (Part X of Y)", "(X of Y)", "(X)"
    # This should be more aggressive to remove common part notations.
    part_patterns = [
        r'\s*\(Part\s+\d+(?:\.\d+)?\s+of\s+\d+\)',
        r'\s*\(\d+(?:\.\d+)?\s*of\s*\d+\)', # Matches (XofY) or (X of Y)
        r'\s*\(Disc\s+\d+\s+of\s+\d+\)',
        r'\s*\(\d+(?:\.\d+)?\)', # Matches standalone numbers in parentheses like "(1)"
        r'\s*\[Part\s+\d+(?:\.\d+)?\s+of\s+\d+\]',
        r'\s*\[\d+(?:\.\d+)?\s+of\s+\d+\]',
        r'\s*\[Disc\s+\d+\s+of\s+\d+\]',
        r'\s*\[\d+(?:\.\d+)?\]', # Matches standalone numbers in brackets like "[1]"
        r'\s*-\s*Part\s+\d+(?:\.\d+)?(?:\s+of\s+\d+)?', # Matches " - Part 1" or " - Part 1 of 5"
        r'\s*-\s*\d+(?:\.\d+)?(?:\s+of\s+\d+)?', # Matches " - 1" or " - 1 of 5"
        r'\s*\(Dramatized Adaptation\)' # Specific for your "Words of Radiance" case
    ]

    cleaned_title = title
    for pattern in part_patterns:
        cleaned_title = re.sub(pattern, '', cleaned_title, flags=re.IGNORECASE).strip()
    
    # Remove any trailing hyphens, spaces, or dots that might be left
    cleaned_title = cleaned_title.strip(' -.')
    
    return cleaned_title.strip()


def normalize_publisher_name(publisher_name):
    """
    Normalizes publisher names to a consistent format.
    Args:
        publisher_name (str): The original publisher name.
    Returns:
        str: The normalized publisher name.
    """
    if not publisher_name:
        return None
    
    normalized = publisher_name.lower().strip()
    
    # Common replacements
    replacements = {
        'graphic audio': 'Graphic Audio',
        'audible studios': 'Audible',
        'audible': 'Audible',
        'hachette audio': 'Hachette Audio',
        'random house audio': 'Random House Audio',
        'macmillan audio': 'Macmillan Audio',
        'harperaudio': 'HarperAudio',
        'simon & schuster audio': 'Simon & Schuster Audio',
        'prh audio': 'PRH Audio',
        'tantor audio': 'Tantor Audio',
        'brilliance audio': 'Brilliance Audio',
        'podium audio': 'Podium Audio',
        'dreamscape media': 'Dreamscape Media',
        'recorded books': 'Recorded Books',
        'blackstone audio': 'Blackstone Audio',
        'scholastic audio': 'Scholastic Audio',
        'michael-scott earle': 'Self-Published', # Example for specific authors
        'actors everywhere': 'Actors Everywhere', # Keep as is if this is a valid publisher/narrator
        'graphic': 'Graphic Audio' # Normalizing 'Graphic' to 'Graphic Audio'
    }

    for old, new in replacements.items():
        if normalized == old:
            return new
            
    # If not in direct replacements, check for substrings
    for old, new in replacements.items():
        if old in normalized:
            return new # Return the standard name if a substring matches

    # If still not normalized, sanitize and return as is
    return sanitize_filename(publisher_name)
