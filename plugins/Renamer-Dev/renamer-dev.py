import requests
import shutil
from pathlib import Path
import stashapi.log as logger
import logging
import json
from pythonjsonlogger import jsonlogger
import re
import sys
import os
import platform

script_dir = os.path.dirname(os.path.abspath(__file__))

settings_template_path = os.path.join(script_dir, "renamer_settings.py.template")
settings_path = os.path.join(script_dir, "renamer_settings.py")
if not os.path.exists(settings_path):
    if os.path.exists(settings_template_path):
        shutil.copy(settings_template_path, settings_path)
        
from renamer_settings import config

try:
    from renamer_settings import debug_hookContext
except ImportError:
    debug_hookContext = None

IS_WINDOWS = platform.system() == 'Windows'

class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def format(self, record):
        log_record = super().format(record)
        log_dict = json.loads(log_record)
        return json.dumps(log_dict)

def setup_external_logger():
    script_directory = os.path.dirname(os.path.abspath(__file__))
    log_path = os.path.join(script_directory, 'renamer.json')
    logger = logging.getLogger('ext_log')
    logger.setLevel(logging.INFO)

    log_handler = logging.FileHandler(log_path)
    formatter = CustomJsonFormatter('%(asctime)s %(levelname)s %(message)s')
    log_handler.setFormatter(formatter)

    logger.addHandler(log_handler)
    return logger

ext_log = setup_external_logger()


def graphql_request(query, variables=None):
    headers = {
        "Accept-Encoding": "gzip, deflate, br",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "ApiKey": config.get("api_key", "") 
    }
    response = requests.post(config['endpoint'], json={'query': query, 'variables': variables}, headers=headers)
    try:
        data = response.json()
        return data.get('data')
    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON from response: {response.text}")
        return None

def fetch_stash_directories():
    configuration_query = """
        query Configuration {
            configuration {
                general {
                    stashes {
                        path
                    }
                }
            }
        }
    """
    result = graphql_request(configuration_query)
    return [makePath(stash['path']) for stash in result['configuration']['general']['stashes']]

def replace_illegal_characters(filename):
    if filename == None:
        return None

    illegal_chars = r'[<>:"/\\|?*]'

    # Step 1: Replace bad characters with ' - '
    replaced = re.sub(illegal_chars, ' - ', filename)
    
    # Step 2: Collapse multiple spaces into one
    normalized = re.sub(r'\s{2,}', ' ', replaced)

    return normalized

def apply_regex_transformations(value, key):
    transformations = config.get('regex_transformations', {})
    for transformation_name, transformation_details in transformations.items():
        if key in transformation_details['fields']:
            pattern = re.compile(transformation_details['pattern'])
            replacement_function = lambda match: transformation_details['replacement'](match)
            value = re.sub(pattern, replacement_function, value)
    return value

def apply_studio_template(studio_name, scene_data):
    templates = config.get("studio_templates", {})
    template = templates.get(studio_name, "")
    
    if not template:
        return None
    
    # Prepare template data with transformations and limits
    template_data = {}
    for key, value in scene_data.items():
        if isinstance(value, dict) and 'name' in value:
            value = value['name']
        if key == 'performers':
            value = sort_performers(value)
            value = config['separator'].join(performer['name'] for performer in value)
        if key == 'tags':
            filtered_tags = [tag['name'] for tag in value if tag['name'] in config['tag_whitelist']]
            value = config['separator'].join(filtered_tags) if filtered_tags else ''
        if key == 'date' and value:
            value = apply_date_format(value)
        value = apply_regex_transformations(value, key) if isinstance(value, str) else value
        value = replace_illegal_characters(value) if isinstance(value, str) else value
        template_data[key] = value

    filename = template
    for key, value in template_data.items():
        wrapper = config['wrapper_styles'].get(key, ('', ''))
        filename = filename.replace(f"${key}", f"{wrapper[0]}{value}{wrapper[1]}")
    
    logger.info(f"Applying studio template for '{studio_name}': {filename}")
    return filename

def sort_performers(performers):
    sorted_performers = sorted(performers, key=lambda x: x['name'])
    if config['performer_limit'] is not None and len(sorted_performers) > config['performer_limit']:
        sorted_performers = sorted_performers[:config['performer_limit']]
    return sorted_performers

def rename_associated_files(directory, filename_base, new_filename_base, dry_run=False, scene_id=None):
    for ext in config['associated_files']:
        check_file  = f"{filename_base}*.{ext}"
        associated_file = directory / check_file
        if os.path.exists(associated_file):
            new_associated_file = directory / f"{new_filename_base}*.{ext}"
            if dry_run:
                logger.info(f"Dry run: Detected and would move/rename '{associated_file}' to '{new_associated_file}'")
            else:
                shutil.move(str(associated_file), str(new_associated_file))
                logger.info(f"Moved and renamed associated file '{associated_file}' to '{new_associated_file}'")
                if scene_id:
                    ext_log.info(f"Moved and renamed associated file", extra={"original_path": str(associated_file), "new_path": str(new_associated_file), "scene_id": scene_id})
        

        if(0):
            associated_files = list(directory.glob(f"{filename_base}*.{ext}"))
            if len(associated_files) == 1 or any(file.stem.startswith(filename_base) for file in associated_files):
                for associated_file in associated_files:
                    if associated_file.stem.startswith(filename_base) or len(associated_files) == 1:
                        new_associated_file = directory / f"{new_filename_base}{associated_file.suffix}"
                        if dry_run:
                            logger.info(f"Dry run: Detected and would move/rename '{associated_file}' to '{new_associated_file}'")
                        else:
                            shutil.move(str(associated_file), str(new_associated_file))
                            logger.info(f"Moved and renamed associated file '{associated_file}' to '{new_associated_file}'")
                            if scene_id:
                                ext_log.info(f"Moved and renamed associated file", extra={"original_path": str(associated_file), "new_path": str(new_associated_file), "scene_id": scene_id})
                    else:
                        logger.info(f"Associated file '{associated_file}' does not match base name '{filename_base}' and will not be renamed.")
            else:
                logger.info(f"No unique or matching associated files found for extension '.{ext}' in directory '{directory}'")

def move_associated_files(directory, new_directory, filename_base, dry_run, scene_id=None):
    for ext in config['associated_files']:
        check_file  = f"{filename_base}.{ext}"
        associated_file = directory / check_file
        if os.path.exists(associated_file):
            new_associated_file = new_directory / f"{new_filename_base}*.{ext}"
            if dry_run:
                logger.info(f"Dry run: Would move '{associated_file}' to '{new_associated_file}'")
            else:
                shutil.move(str(associated_file), str(new_associated_file))
                logger.info(f"Moved associated file '{associated_file}' to '{new_associated_file}'")
                if scene_id:
                    ext_log.info(f"Moved associated file", extra={"original_path": str(associated_file), "new_path": str(new_associated_file), "scene_id": scene_id})

    for file in config['unassociated_files']:                    
        for ext in config['associated_files']:
            check_file  = f"{file}.{ext}"
            associated_file = directory / check_file
            if os.path.exists(associated_file):
                new_associated_file = new_directory / check_file
                if dry_run:
                    logger.info(f"Dry run: Would move '{associated_file}' to '{new_associated_file}'")
                else:
                    shutil.move(str(associated_file), str(new_associated_file))
                    logger.info(f"Moved unassociated file '{associated_file}' to '{new_associated_file}'")
                    if scene_id:
                        ext_log.info(f"Moved associated file", extra={"original_path": str(associated_file), "new_path": str(new_associated_file), "scene_id": scene_id})


    if(0):
        for item in directory.iterdir():
            if item.suffix[1:] in config['associated_files']:
                new_associated_file = new_directory / item.name
                if dry_run:
                    logger.info(f"Dry run: Would move '{item}' to '{new_associated_file}'")
                else:
                    shutil.move(str(item), str(new_associated_file))
                    logger.info(f"Moved associated file '{item}' to '{new_associated_file}'")
                    if scene_id:
                        ext_log.info(f"Moved associated file", extra={"original_path": str(item), "new_path": str(new_associated_file), "scene_id": scene_id})


def move_trickplay_folder(base_name: str, original_dir: str, destination_dir: str):
    # Construct source folder path
    source_folder = os.path.join(original_dir, f"{base_name}.trickplay")

    # Check if it exists and is a directory
    if os.path.isdir(source_folder):
        dest_folder = os.path.join(destination_dir, f"{base_name}.trickplay")
        if dry_run:
            logger.info(f"Dry run: Would move trickplay '{source_folder}' to '{dest_folder}'")
        else:
            shutil.move(source_folder, dest_folder)
            print(f"Moved: {source_folder} → {dest_folder}")
    

def get_unique_path(target_path):
    """Generate a unique path if target already exists by adding a number suffix."""
    if not target_path.exists():
        return target_path
    
    directory = target_path.parent
    name = target_path.stem
    extension = target_path.suffix
    counter = 1
    
    while True:
        new_path = directory / f"{name} ({counter}){extension}"
        if not new_path.exists():
            return new_path
        counter += 1

def safe_file_operation(source_path, target_path, operation='move', dry_run=False):
    """Safely perform file operations with collision handling."""
    if not source_path.exists():
        logger.error(f"Source file not found: {source_path}")
        return None
        
    unique_target = get_unique_path(target_path)
    
    if unique_target != target_path:
        logger.info(f"File already exists at {target_path}, using {unique_target} instead")
    
    if dry_run:
        logger.info(f"Dry run: Would {operation} '{source_path}' to '{unique_target}'")
        return unique_target
        
    try:
        if operation == 'move':
            shutil.move(str(source_path), str(unique_target))
        else:  # rename
            source_path.rename(unique_target)
        logger.info(f"Successfully {operation}d file to '{unique_target}'")
        return unique_target
    except Exception as e:
        logger.error(f"Failed to {operation} file: {str(e)}")
        return None

def process_files(scene, new_filename, move, rename, dry_run):
    original_path = makePath(scene['file_path'])
    new_path = calculate_new_path(original_path, new_filename)
    directory = original_path.parent
    filename_base = original_path.stem
    new_filename_base = new_path.stem
    new_directory = new_path.parent

    # Check if the file is already in the correct location
    if original_path.parent == new_path.parent:
        if dry_run:
            logger.info(f"Dry run: File '{original_path}' is already in the correct directory.")
        else:
            logger.info(f"File '{original_path}' is already in the correct directory.")
        move = False

    if rename:
        rename_associated_files(directory, filename_base, new_filename_base, dry_run)

    if move:
        new_path = safe_file_operation(original_path, new_path, 'move', dry_run)
        if new_path and not dry_run:
            ext_log.info(f"Moved main file", extra={
                "original_path": str(original_path), 
                "new_path": str(new_path)
            })
            move_associated_files(directory, new_directory, filename_base, dry_run)
        if config["move_trickplay"] and new_path:
           move_trickplay_folder(filename_base, directory.resolve(), new_directory.resolve())
    elif rename:
        new_path = safe_file_operation(original_path, new_path, 'rename', dry_run)
        if new_path and not dry_run:
            ext_log.info(f"Renamed main file", extra={
                "original_path": str(original_path), 
                "new_path": str(new_path)
            })

    if not move:
        move_associated_files(directory, new_directory, filename_base, dry_run)

import datetime

def apply_date_format(value):
    try:
        formatted_date = datetime.datetime.strptime(value, "%Y-%m-%d").strftime(config['date_format'])
        return formatted_date
    except ValueError as e:
        ext_log.error(f"Date formatting error: {str(e)}")
        return value

def form_new_filename(scene):
    studio = scene.get('studio', None)
    studio_name = studio.get('name', '') if studio else None
    templated_filename = apply_studio_template(studio_name, scene)
    
    if templated_filename:
        logger.info(f"Studio template detected for '{studio_name}' and applied: {templated_filename}")
        return templated_filename
    
    parts = []
    for key in config['key_order']:
        if key in config['exclude_keys']:
            continue

        value = scene.get(key)
        if key == 'studio' and not studio:
            continue  # Skip studio if it's None
        if isinstance(value, dict) and 'name' in value:
            value = value['name']
        if key == 'tags':
            filtered_tags = [tag['name'] for tag in value if tag['name'] in config['tag_whitelist']]
            value = config['separator'].join(filtered_tags) if filtered_tags else ''
        elif key == 'performers':
            performers = sort_performers(value)
            value = config['separator'].join(performer['name'] for performer in performers)
        elif key in ['stash_id']:
            stash_id_value = next((stash_id.get(key) for stash_id in scene.get('stash_ids', [])), '') 
            if key == 'stash_id' and stash_id_value:
                value = str(stash_id_value)
        # elif isinstance(value, dict) and 'stash_id' in value: 
            # value = value.get('stash_id')
        elif key == 'date' and value:
            value = apply_date_format(value)
        elif key in ['studio', 'title']:
            value = value.get('name', '') if isinstance(value, dict) else value
        elif key in ['height', 'video_codec', 'frame_rate']:
            file_info_value = next((file_info.get(key) for file_info in scene.get('files', [])), '')
            if key == 'height' and file_info_value:
                value = str(file_info_value) + 'p'
            elif key == 'video_codec' and file_info_value:
                value = file_info_value.upper()
            elif key == 'frame_rate' and file_info_value:
                value = str(file_info_value) + ' FPS'

        if value:  # Skip empty values
            value = apply_regex_transformations(value, key) if isinstance(value, str) else value
            value = replace_illegal_characters(value) if isinstance(value, str) else value
            wrapper = config['wrapper_styles'].get(key, ('', ''))
            part = f"{wrapper[0]}{value}{wrapper[1]}"
            parts.append(part)

    filename = config['separator'].join(parts).rstrip(config['separator'])
    logger.info(f"Generated filename: {filename}")
    return filename

    
def form_new_foldername(scene):
    
    parts = []
    for key in config['folder_key_order']:
        if key in config['exclude_keys']:
            continue

        value = scene.get(key)
        if key == 'studio' and not studio:
            continue  # Skip studio if it's None
        if isinstance(value, dict) and 'name' in value:
            value = value['name']
        if key == 'tags':
            filtered_tags = [tag['name'] for tag in value if tag['name'] in config['tag_whitelist']]
            value = config['separator'].join(filtered_tags) if filtered_tags else ''
        elif key == 'performers':
            performers = sort_performers(value)
            value = config['separator'].join(performer['name'] for performer in performers)
        elif key in ['stash_id']:
            stash_id_value = next((stash_id.get(key) for stash_id in scene.get('stash_ids', [])), '') 
            if key == 'stash_id' and stash_id_value:
                value = str(stash_id_value)
        # elif isinstance(value, dict) and 'stash_id' in value: 
            # value = value.get('stash_id')
        elif key == 'date' and value:
            value = apply_date_format(value)
        elif key in ['studio', 'title']:
            value = value.get('name', '') if isinstance(value, dict) else value
        elif key in ['height', 'video_codec', 'frame_rate']:
            file_info_value = next((file_info.get(key) for file_info in scene.get('files', [])), '')
            if key == 'height' and file_info_value:
                value = str(file_info_value) + 'p'
            elif key == 'video_codec' and file_info_value:
                value = file_info_value.upper()
            elif key == 'frame_rate' and file_info_value:
                value = str(file_info_value) + ' FPS'

        if value:  # Skip empty values
            value = apply_regex_transformations(value, key) if isinstance(value, str) else value
            value = replace_illegal_characters(value) if isinstance(value, str) else value
            wrapper = config['wrapper_styles'].get(key, ('', ''))
            part = f"{wrapper[0]}{value}{wrapper[1]}"
            parts.append(part)

    foldername = config['separator'].join(parts).rstrip(config['separator'])
    logger.info(f"Generated foldername: {foldername}")
    return foldername

def linux_to_windows_path(linux_path: str) -> str:
    if IS_WINDOWS:
        for linux_root, windows_root in config["folder-map"].items():
            if linux_path.startswith(linux_root):
                # Replace the root and convert slashes
                relative_path = linux_path[len(linux_root):].lstrip('/')
                windows_path = f"{windows_root}\\{relative_path}".replace('/', '\\')
                return windows_path
        # If no match, just convert slashes
        return linux_path.replace('/', '\\')
    return linux_path

    
def makePath(linux_path: str) -> Path:
    return Path(linux_to_windows_path(linux_path)) if IS_WINDOWS else Path(linux_path)


def move_or_rename_files(scene, new_filename, move, rename, dry_run):
    if not scene:
        logger.error("No scene data provided to process.")
        return []

    scene_id = scene.get('id', 'Unknown')
    results = []
    action = None  # Initialize action variable

    if not scene.get('title'):
        logger.info(f"Skipping scene {scene_id} due to missing title.")
        return results

    studio = scene.get('studio', None)
    studio_name = studio.get('name', 'No Studio') if studio else 'No Studio'
    tags = {tag['name'] for tag in scene.get('tags', [])}
    tag_path = next((makePath(config['tag_specific_paths'][tag]) for tag in tags if tag in config['tag_specific_paths']), None)

    for file_info in scene.get('files', []):
        original_path = makePath(file_info['path'])
        
        # Verify file exists before proceeding
        if not original_path.exists():
            logger.error(f"Source file not found: {original_path}")
            continue

        current_stash = next((stash for stash in fetch_stash_directories() if original_path.is_relative_to(stash)), None)

        if not current_stash:
            if not dry_run:
                ext_log.error("File is not in any known stash path", extra={"file_path": str(original_path), "scene_id": scene_id})
            continue

        if move:
            if tag_path:
                target_directory = replace_illegal_characters(tag_path) / studio_name / form_new_foldername(scene) 
            else:
                target_directory = current_stash / replace_illegal_characters(studio_name) / form_new_foldername(scene)

            new_path = target_directory / (new_filename + original_path.suffix)

            # Create target directory if it doesn't exist
            if not dry_run:
                target_directory.mkdir(parents=True, exist_ok=True)

            # Check if already in correct location
            if original_path.parent == new_path.parent:
                logger.info(f"File '{original_path}' is already in the correct directory.")
                move = False
            
            # Check if filename already correct
            if original_path.name == new_path.name:
                logger.info(f"File '{original_path}' already has the correct filename.")
                rename = False

            if not (move or rename):
                continue

            if dry_run:
                action = "move" if move else "rename"
                logger.info(f"Dry run: Would {action} file: {original_path} -> {new_path}")

                if config["move_trickplay"]:
                    move_trickplay_folder(original_path.stem, original_path.parent, target_directory)
                
                if move:
                    move_associated_files(original_path.parent, target_directory, original_path.stem, dry_run, scene_id)
                continue

            try:
                if move:
                    new_path = safe_file_operation(original_path, new_path, 'move', dry_run)
                    if new_path:
                        action = "Moved"
                        if scene_id != 'Unknown':
                            ext_log.info(f"Moved main file", extra={
                                "original_path": str(original_path), 
                                "new_path": str(new_path), 
                                "scene_id": scene_id
                            })
                        move_associated_files(original_path.parent, target_directory, original_path.stem, dry_run, scene_id)
                elif rename:
                    new_path = safe_file_operation(original_path, new_path, 'rename', dry_run)
                    if new_path:
                        action = "Renamed"
                        if scene_id != 'Unknown':
                            ext_log.info(f"Renamed main file", extra={
                                "original_path": str(original_path), 
                                "new_path": str(new_path), 
                                "scene_id": scene_id
                            })

                if action and new_path:  # Only log if action was successful
                    logger.info(f"{action} file from '{original_path}' to '{new_path}'.")
                    results.append({
                        "action": action,
                        "original_path": str(original_path),
                        "new_path": str(new_path),
                        "scene_id": scene_id
                    })

            except Exception as e:
                logger.error(f"Failed to {move and 'move' or 'rename'} file: {str(e)}")

    return results



def find_scene_by_id(scene_id):
    query_find_scene = """
    query FindScene($scene_id: ID!) {
        findScene(id: $scene_id) {
            id
            title
            date
            files {
                path
                height
                video_codec
                frame_rate
            }
            studio {
                name
            }
            performers {
                name
            }
            tags {
                name
            }
            stash_ids {
                stash_id
            }
        }
    }
    """
    scene_data = graphql_request(query_find_scene, variables={"scene_id": scene_id})
    return scene_data.get('findScene')

def is_debugger_attached():
    return any('pydevd' in mod for mod in sys.modules)

def get_hook_context():
    try:
        if debug_hookContext != None and is_debugger_attached():
            json_input = json.loads(debug_hookContext)
        else:
            json_input = json.loads(sys.stdin.read())        

        hook_context = json_input.get('args', {}).get('hookContext', {})
        return hook_context
    except json.JSONDecodeError:
        logger.error("Failed to decode JSON input.")
        return {}

def main():
    hook_context = get_hook_context()
    if not hook_context:
        logger.error("No hook context provided.")
        return

    scene_id = hook_context.get('id')
    if not scene_id:
        logger.error("No scene ID provided in the hook context.")
        return

    detailed_scene = find_scene_by_id(scene_id)
    if not detailed_scene:
        logger.error(f"Failed to fetch details for scene ID: {scene_id}")
        return

    new_filename = form_new_filename(detailed_scene)
    move_or_rename_files(detailed_scene, new_filename, config['move_files'], config['rename_files'], config['dry_run'])

if __name__ == '__main__':
    main()
