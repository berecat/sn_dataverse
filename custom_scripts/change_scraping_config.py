import json
import os
import shutil

# Paths
config_path = "scraping/config"  # Change to your actual folder path
input_file_path = "input/default_scraping_config.json"

# Read input JSON content
with open(input_file_path, "r", encoding="utf-8") as f:
    input_content = json.load(f)

# Overwrite files scraping_config_0.json to scraping_config_19.json
for i in range(20):
    file_path = os.path.join(config_path, f"scraping_config_{i}.json")
    
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(input_content, f, indent=4)

    print(f"Overwritten: {file_path}")
