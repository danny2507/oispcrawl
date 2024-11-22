import os
import json
from urllib.parse import urlparse

# Define the function to check for duplicate segments in a URL
def has_duplicate_segments(url):
    """
    Check if the URL contains duplicate path segments.
    """
    parsed_url = urlparse(url)
    path_segments = parsed_url.path.strip("/").split("/")
    return len(path_segments) != len(set(path_segments))

# Directory paths
input_dir = os.getcwd()  # Current directory
output_dir = os.path.join(input_dir, "output")  # Output directory
output_file_path = os.path.join(output_dir, "output.json")  # Output file

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)

# Initialize counters
original_url_count = 0
filtered_url_count = 0

# Initialize the filtered output and a set to track seen URLs
filtered_output = []
seen_urls = set()

# Collect all JSON files in the current directory
json_files = [file for file in os.listdir(input_dir) if file.endswith(".json")]

# Process each JSON file
for json_file in json_files:
    with open(json_file, 'r', encoding='utf-8') as file:
        try:
            data = json.load(file)
            original_url_count += len(data)  # Count all URLs before filtering
            for entry in data:
                if "url" in entry:
                    url = entry["url"]
                    # Check for duplicate segments and duplicate URLs
                    if not has_duplicate_segments(url) and url not in seen_urls:
                        filtered_output.append(entry)
                        seen_urls.add(url)
            filtered_url_count = len(filtered_output)  # Count after filtering
        except json.JSONDecodeError:
            print(f"Error decoding JSON in file: {json_file}")

# Write the filtered output to the output file
with open(output_file_path, 'w', encoding='utf-8') as output_file:
    json.dump(filtered_output, output_file, indent=4, ensure_ascii=False)

# Print the statistics
print(f"Total URLs before filtering: {original_url_count}")
print(f"Total URLs after filtering: {filtered_url_count}")
print(f"Filtered output saved to: {output_file_path}")
