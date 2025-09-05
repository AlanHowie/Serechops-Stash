# This script is a background thread that updates StashDB by running the Library Task

import os
from pathlib import Path
import time
import requests
from renamer_settings import config

print("Renamer Library Service Running.")

lock_file_name = "renamer-lock.dat"
lock_file_location = Path(os.path.join(os.path.dirname(__file__), lock_file_name))

while True:
    if not lock_file_location.exists():
        exit(-1)
        break

    last_modified = lock_file_location.stat().st_mtime
    age = time.time() - last_modified

    if age > 20:        
        break

    time.sleep(5)  # Avoid hammering the disk


def log_paths(paths: list[str]):
    print("🔍 Scanning Paths:")
    for i, path in enumerate(paths, start=1):
        print(f"  {i:02d}. {path}")

def graphql_request(operationName, query, variables=None):
    headers = {
        "Accept-Encoding": "gzip, deflate, br",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "ApiKey": config.get("api_key", "") 
    }
    response = requests.post(config['endpoint'], json={'operationName': operationName,'query': query, 'variables': variables}, headers=headers)
    try:
        data = response.json()
        return data.get('data')
    except:
        return None


def update_library(paths):
    query="""mutation MetadataScan($input: ScanMetadataInput!) {
         metadataScan(input: $input)
    }
    """
    vars = {
        "input": {
            "paths": paths,
            "rescan": False,
            "scanGenerateClipPreviews": False,
            "scanGenerateCovers": False,
            "scanGenerateImagePreviews": False,
            "scanGeneratePhashes": False,
            "scanGeneratePreviews": False,
            "scanGenerateSprites": False,
            "scanGenerateThumbnails": False
        }
    }

    log_paths(paths)
    graphql_request("MetadataScan", query, vars)

def deduplicate_ordered(items: list[str]) -> list[str]:
    seen = set()
    result = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result

import requests

def send_webhook(paths: list[str]):

    if len(paths) == 0:
        return

    webhook = config["webhook"]
    if not webhook or not webhook["url"] or not webhook["url"]:
        return

    headers = {
        "Content-Type": "application/json",        
        "X-API": webhook["api_key"] or ""
    }

    payload = {
        "instanceName": "StashDB",
        "event": 'updated',
        "paths": paths
    }

    try:
        response = requests.post(webhook["url"], json=payload, headers=headers)
        response.raise_for_status()
        print(f"✅ Webhook sent: {response.status_code}")
    except requests.RequestException as e:
        print(f"❌ Webhook failed: {e}")


# Read in paths we have updated so we can scan the library paths
paths = []
try:
    with lock_file_location.open("r", encoding="utf-8") as f:
        paths = [line.strip() for line in f if line.strip()]

    lock_file_location.unlink()
except:
    print(f"Error loading: {lock_file_name}")

paths = deduplicate_ordered(paths)
update_library(paths)
send_webhook(paths)