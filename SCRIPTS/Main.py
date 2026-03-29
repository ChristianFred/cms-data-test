import os
import json
import urllib.request
import urllib.parse
import threading
import csv
import re
from concurrent.futures import ThreadPoolExecutor 


Data_Store = "Data" # This is the directory where we will store the downloaded datasets. Each dataset will be saved as a separate file within this directory.
Metadata_File = os.path.join("Last_Modified", "Last_Run.json") # This is the path to the JSON file where we will store metadata.
os.makedirs(Data_Store, exist_ok=True) 
os.makedirs(os.path.dirname(Metadata_File), exist_ok=True)

# Okay so here we are loading the previous metadata and once it's read it's listed as metadata.
# I have decided to store metadata as JSON because of it's ease of viewing. 
if os.path.exists(Metadata_File):
    with open(Metadata_File, "r") as f:
        metadata = json.load(f)
# However, if we don't have any metadata it starts up from scratch. 
else:
    metadata = {}

# Here is the variable for storing the CMS JSON
CMS_API_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
# Here we are making a request to the CSM data and loading it as a JSON object. 
with urllib.request.urlopen(CMS_API_URL) as response:
    datasets = json.loads(response.read())

required_datasets = [] # This will hold the datasets that are related to Hospitals and have a distribution URL. We will use this list to process the datasets in parallel later on.
for ds in datasets:
    if "Hospitals" in ds.get("theme", []):
        if "distribution" in ds and ds["distribution"]:
            required_datasets.append({
                "title": ds["title"],
                "url": ds["distribution"][0]["downloadURL"],  # We are assuming the first distribution is the one we want. In a business case might want to check all distributions or look for specific formats.
                "modified": ds["modified"]
            })
        
print(f"Found {len(required_datasets)} datasets related to Hospitals:") # This will print out the number of datasets we found that are related to Hospitals and have a distribution URL.

for i, ds in enumerate(required_datasets): # This will print out the title and URL of each dataset we found, indexed starting from 1.
    print(f"{i+1}: {ds['title']} -> {ds['url']}")

def safe_filename(title): # This function takes a title and converts it into filename by replacing any characters that are not alphanumeric, hyphens, underscores, dots, or spaces with underscores. 
    return re.sub(r'[^\w\-_\. ]', '_', title)

def process_dataset(ds, index): # This function takes a dataset and its index, and processes it by downloading the CSV file, normalizing the headers, and saving it to the local data store. It also updates the metadata with the last modified date of the dataset.
    title = ds["title"]
    url = ds["url"]
    modified = ds["modified"]

    # Ensure data dir exists
    os.makedirs(Data_Store, exist_ok=True)

    # Determine file extension from URL, default to .csv if not found
    try:
        url_path = urllib.parse.urlparse(url).path
        ext = os.path.splitext(url_path)[1] or ".csv"
    except Exception:
        ext = ".csv"

    # Local CSV path with filename and modified timestamp
    local_filename = os.path.join(Data_Store, f"{index+1}_{safe_filename(title)}{ext}")

    # Skip if dataset has not been modified AND the local file exists
    if metadata.get(title) == modified and os.path.exists(local_filename) and os.path.getsize(local_filename) > 0:
        print(f"Skipping unchanged dataset: {title}")
        return

    try: # We are using urllib to download the file from the URL and saving it to the local filename. If there is any error during the download, we catch it and print an error message.
        with urllib.request.urlopen(url) as response, open(local_filename, "wb") as out_file:
            while True:
                chunk = response.read(8192)
                if not chunk:
                    break
                out_file.write(chunk)
        print(f"Downloaded: {title} -> {local_filename}")
    except Exception as e:
        print(f"Error downloading {title}: {e}")
        return

    # Read CSV, normalize headers to snake_case
    try:
        with open(local_filename, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        if not rows:
            print(f"No data in CSV for {title}")
            return

        headers = rows[0]
        new_headers = [re.sub(r"[^\w]", "_", h.lower()) for h in headers]
        rows[0] = new_headers

    # Write back processed CSV
        with open(local_filename, "w", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(rows)

        if 'metadata_lock' in globals(): 
            with metadata_lock:
                metadata[title] = modified
        else:
            metadata[title] = modified
    except Exception as e:
        print(f"Error processing CSV for {title}: {e}")
        return
    
metadata_lock = threading.Lock() # We are using a threading lock to ensure that when multiple threads are updating the shared metadata dictionary
with ThreadPoolExecutor(max_workers=5) as executor: # We are using a ThreadPoolExecutor to process the datasets in parallel. This allows us to download and process multiple datasets at the same time.
    futures = [executor.submit(process_dataset, ds, idx) for idx, ds in enumerate(required_datasets)]
    # Ensure all finish
    for f in futures:
        try:
            f.result()
        except Exception as e:
            print(f"Error in worker: {e}")

with open(Metadata_File, "w", encoding="utf-8") as f: # After all datasets have been processed, we write the updated metadata back to the JSON file.
    json.dump(metadata, f, indent=4)

print(f"All {len(required_datasets)} datasets processed. Each has a separate file.") # Finally, we print out a message indicating that all datasets have been processed and saved as separate files in the data store.