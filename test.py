import json

# Define the input and output file paths
input_file = "crawled_data.json"  # Replace with your input file path
output_file = "extracted_urls.json"  # Replace with your desired output file path

# Read the JSON file
with open(input_file, "r", encoding="utf-8") as f:
    data = json.load(f)

# Extract URLs
urls = [entry["url"] for entry in data if "url" in entry]

# Save the extracted URLs to a new file
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(urls, f, indent=4)

print(f"Extracted URLs saved to {output_file}")
