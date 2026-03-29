import os
import json
import urllib.request
import csv
from concurrent.futures import ThreadPoolExecutor 

# Along with my code I will be adding line comments to explain my reasoning for certain choices.
# Below are folders where stuff will be saved such as data and json files.
Data_Store = "Data"
Metadata_File = "Last_Modified/Last_Run.json"

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
    data_bytes = response.read()
    datasets = json.loads(data_bytes)

#Filtering Step 
required_datasets = []
# Below is a Loop that goes through the datasets returened by the API and it only includes datasets
# Where Hospitals are in the theme we are then only storing relevant information shown below. 
for ds in datasets:
    if "Hospitals" in ds.get("theme", []):
        required_datasets.append({
            "title": ds["title"],
            "url": ds["distribution"][0]["downloadURL"],
            "modified": ds["modified"]
        })

print(f"Found {len(required_datasets)} datasets related to Hospitals.")
# Self comment - to check individual datasets do: for ds in required_datasets: print("-", ds["title"])

# We are now looping over our collected datasets and identifying the info we want from them. 
for ds in required_datasets:
    title = ds["title"]
    url = ds["url"]
    modified = ds["modified"]
# This section as required only downloads CSVs if they are new or updated. Specifically the .get(title) is locating the last modified timestamp.
    if metadata.get(title) == modified:
        continue # We skip the download if the dataset has not been modified since the last run.



#This is the actual CSV downloading step 
local_filename = f"data/{title.replace(' ', '_')}.csv" #Creating a local filename by replacing spaces with underscores for better file handling.

downloaded_files = os.listdir("Data")
print(f"Files in Data/: {downloaded_files}")

with urllib.request.urlopen(url) as response, open(local_filename, "wb") as out_file: #Opening the URL and the local file in write-binary mode to download the CSV content from the CMS API and save it locally.
    out_file.write(response.read())
with open(local_filename, "r", newline="", encoding="utf-8") as f:
    reader = csv.reader(f)
    rows = list(reader)

    headers = rows[0]
    new_headers = [h.lower().replace(' ','_').replace(',', '').replace("'", "") for h in headers] # Converts headers to snake_case.
    rows[0] = new_headers #Replace the original headers with the new snake_case headers.
    with open(local_filename, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(rows) #Writes the modified rows back to the CSV file, Updating the headers to snake_case.
# Last loop of datasets this time to update the metadata with the latest modified timestamps for each dataset.
for ds in required_datasets:
    title = ds["title"]
    modified = ds["modified"]
    metadata[title] = modified

with open(Metadata_File, "w",encoding="utf-8") as f: #Opening the metadata JSON file in write mode to update it with the latest modified timestamps for each dataset.
    json.dump(metadata, f, indent=4) #Writes the updated metadata back to the JSON file with indentation readability.

#The Section down below is for running on multiple threads to answer that part of the assignment.
# We are using ThreadPoolExecutor to download multiple datasets concurrently, which can speed up the process when dealing with multiple files.
 
def process_dataset(ds):
    title = ds["title"]
    url = ds["url"]
    modified = ds["modified"]
    return title, modified # Returns the title and modified timestamp for each dataset after processing.

with ThreadPoolExecutor(max_workers=5) as executor: # Setting max_workers to 5 to allow up to 5 concurrent downloads. (There wasn't that many entries so this should be sufficient.)
    results = executor.map(process_dataset, required_datasets) # Maps the process_dataset function to each dataset in required_datasets, allowing them to be processed concurrently.

    for title, modified in results:
        metadata[title] = modified # Updates the metadata with the latest modified timestamps for each dataset after processing.
