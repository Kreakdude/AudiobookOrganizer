import os
import shutil
import sys
import threading
import re

# Global lock for print statements to avoid interleaving output from multiple processes/threads
print_lock = threading.Lock()

# Global variables to hold log file handles for multiprocessing workers
_global_log_file_handle = None
_global_manual_log_file_handle = None

def set_global_log_handles(log_file_h, manual_log_file_h):
    """
    Sets the global log file handles for use by multiprocessing workers.
    This function is intended to be used as the 'initializer' for multiprocessing.Pool.
    """
    global _global_log_file_handle
    global _global_manual_log_file_handle
    _global_log_file_handle = log_file_h
    _global_manual_log_file_handle = manual_log_file_h

def custom_print(message, level="INFO", to_console=True, log_file_handle=None, end='\n'):
    """
    Custom print function that logs to a file and optionally to the console.
    It prioritizes global log handles (for multiprocessing) over passed arguments.
    Args:
        message (str): The message to print.
        level (str): The log level (e.g., "INFO", "WARNING", "ERROR").
        to_console (bool): Whether to print the message to the console.
        log_file_handle (file object, optional): A file handle to write logs to.
        end (str): String appended after the message. Defaults to '\n'.
    """
    with print_lock:
        log_message = f"[{level}] {message}"
        
        # Determine which log file handle to use
        current_log_file_handle = log_file_handle
        if _global_log_file_handle: # If global handle is set (from multiprocessing)
            current_log_file_handle = _global_log_file_handle

        # Always write full message with a newline to the log file for readability
        if current_log_file_handle:
            current_log_file_handle.write(log_message + "\n")
            current_log_file_handle.flush() # Ensure it's written immediately
        
        # For console, use the 'end' argument
        if to_console:
            sys.stdout.write(log_message + end)
            sys.stdout.flush()

def sanitize_filename(name):
    """Sanitizes a string to be used as a filename or directory name."""
    if not name:
        return "Untitled"
    # Replace invalid characters with underscores
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', name)
    # Replace leading/trailing spaces or dots
    sanitized = sanitized.strip(' .')
    # Replace multiple spaces with a single space
    sanitized = re.sub(r'\s+', ' ', sanitized)
    # Truncate if too long (common filesystem limit is 255, but keep shorter for safety)
    if len(sanitized) > 200:
        sanitized = sanitized[:200]
    return sanitized

def hard_link_to_leftbehind(src_path, source_root_dir, leftbehind_base_dir, reason="", manual_log_list=None, level="INFO"):
    """
    Hard-links a file to the 'leftbehind' directory, maintaining its relative path structure.
    Args:
        src_path (str): The full path to the source file.
        source_root_dir (str): The root directory from which relative paths are calculated.
        leftbehind_base_dir (str): The base directory for leftbehind files.
        reason (str): The reason the file is being hard-linked to leftbehind.
        manual_log_list (list or file object, optional): A list or file object to append manual actions to.
        level (str): The log level for the manual action.
    Returns:
        bool: True if hard-link was successful, False otherwise.
    """
    relative_path = os.path.relpath(src_path, source_root_dir)
    dest_path = os.path.join(leftbehind_base_dir, relative_path)
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    try:
        if not os.path.exists(dest_path): # Avoid trying to link if already exists (e.g., from a previous run)
            os.link(src_path, dest_path)
            log_message = f"  Info: Hard-linked '{relative_path}' to '{os.path.relpath(dest_path, os.path.dirname(leftbehind_base_dir))}' in 'leftbehind'. Reason: {reason}"
            custom_print(log_message, to_console=False)
            
            # Determine which manual log list/handle to use
            current_manual_log_list = manual_log_list
            if _global_manual_log_file_handle:
                current_manual_log_list = _global_manual_log_file_handle

            if current_manual_log_list is not None:
                if isinstance(current_manual_log_list, list):
                    current_manual_log_list.append(f"[{level}] {log_message}")
                else: # Assume it's a file handle
                    current_manual_log_list.write(f"[{level}] {log_message}\n")
                    current_manual_log_list.flush()
            return True
        else:
            log_message = f"  Info: File '{relative_path}' already exists in 'leftbehind'. Skipping hard-link. Reason: {reason}"
            custom_print(log_message, to_console=False)
            return True # Consider it successful as it's already there
    except OSError as e:
        error_message = f"  Error: Could not hard-link '{relative_path}' to '{os.path.relpath(dest_path, os.path.dirname(leftbehind_base_dir))}' in 'leftbehind': {e}. Reason: {reason}"
        custom_print(error_message, level="ERROR", to_console=True)
        
        current_manual_log_list = manual_log_list
        if _global_manual_log_file_handle:
            current_manual_log_list = _global_manual_log_file_handle

        if current_manual_log_list is not None:
            if isinstance(current_manual_log_list, list):
                current_manual_log_list.append(f"[{level}] {error_message}")
            else: # Assume it's a file handle
                current_manual_log_list.write(f"[{level}] {error_message}\n")
                current_manual_log_list.flush()
        return False

def setup_directories(dest_base_dir, leftbehind_base_dir, force_empty, custom_print_func):
    """
    Sets up destination and leftbehind directories, handling existing content.
    Args:
        dest_base_dir (str): Path to the destination directory.
        leftbehind_base_dir (str): Path to the leftbehind directory.
        force_empty (bool): If True, forces emptying of directories without prompt.
        custom_print_func (function): The logging function.
    Returns:
        bool: True if directories are set up successfully, False otherwise.
    """
    # Destination directory handling
    if os.path.exists(dest_base_dir):
        custom_print_func(f"Warning: Destination directory '{dest_base_dir}' already exists.", level="WARNING", to_console=True)
        if not force_empty:
            response = input("Do you want to empty its contents before proceeding? (y/N): ").strip().lower()
        else:
            response = 'y'
            custom_print_func("Proceeding with emptying destination directory due to --force-empty flag.", level="INFO", to_console=True)
        
        if response == 'y':
            try:
                custom_print_func(f"Attempting to remove and recreate '{dest_base_dir}'...", to_console=True)
                if os.path.exists(dest_base_dir):
                    shutil.rmtree(dest_base_dir)
                os.makedirs(dest_base_dir, exist_ok=True)
                custom_print_func(f"Directory '{dest_base_dir}' emptied and recreated.", to_console=True)
            except OSError as e:
                custom_print_func(f"Error removing or recreating destination directory '{dest_base_dir}': {e}", level="ERROR", to_console=True)
                custom_print_func("This might be due to files being in use by another process. Please ensure no other applications are accessing this directory.", level="ERROR", to_console=True)
                return False
        else:
            custom_print_func("Aborting: Destination directory not emptied. Please clear it manually or confirm to proceed.", level="INFO", to_console=True)
            return False
    else:
        try:
            os.makedirs(dest_base_dir, exist_ok=True)
        except OSError as e:
            custom_print_func(f"Error creating destination directory '{dest_base_dir}': {e}", level="ERROR", to_console=True)
            return False
    
    # Leftbehind directory handling
    if os.path.exists(leftbehind_base_dir):
        custom_print_func(f"Warning: Leftbehind directory '{leftbehind_base_dir}' already exists. Its contents will be cleared for this run.", level="WARNING", to_console=True)
        try:
            custom_print_func(f"Attempting to remove and recreate '{leftbehind_base_dir}'...", to_console=True)
            shutil.rmtree(leftbehind_base_dir)
            os.makedirs(leftbehind_base_dir, exist_ok=True)
            custom_print_func(f"Directory '{leftbehind_base_dir}' emptied and recreated.", to_console=True)
        except OSError as e:
            custom_print_func(f"Error removing or recreating leftbehind directory '{leftbehind_base_dir}': {e}", level="ERROR", to_console=True)
            custom_print_func("This might be due to files being in use by another process. Please ensure no other applications are accessing this directory.", level="ERROR", to_console=True)
            return False
    else:
        try:
            os.makedirs(leftbehind_base_dir, exist_ok=True)
        except OSError as e:
            custom_print_func(f"Error creating leftbehind directory '{leftbehind_base_dir}': {e}", level="ERROR", to_console=True)
            return False
            
    return True

def cleanup_empty_directories(path, custom_print_func):
    """
    Recursively removes empty directories within the given path.
    Args:
        path (str): The root path to start cleaning from.
        custom_print_func (function): The logging function.
    """
    custom_print_func(f"  Info: Cleaning up empty directories under '{path}'...", to_console=False)
    for dirpath, dirnames, filenames in os.walk(path, topdown=False):
        if not dirnames and not filenames:
            try:
                os.rmdir(dirpath)
                custom_print_func(f"  Info: Removed empty directory: '{dirpath}'", to_console=False)
            except OSError as e:
                custom_print_func(f"  Warning: Could not remove empty directory '{dirpath}': {e}", level="WARNING", to_console=False)
    custom_print_func(f"  Info: Empty directory cleanup complete for '{path}'.", to_console=False)

