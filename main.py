import threading
import time
import requests
from bs4 import BeautifulSoup
import json
import os
from urllib.parse import urlparse, urljoin

from utils import clean_text
# Central data structure to hold URL statuses, used sets so that no duplicate, and utilize hashset
url_status = {
    'pending': set(),
    'in_progress': set(),
    'crawled': set(),
'errors': set()
}
lock = threading.Lock()  # Lock for thread-safe access to shared resources: the above url_status


def is_valid_url(url):
    """
    Checks if the URL's domain matches any in TARGET_DOMAINS exactly
    and removes fragments and trailing slashes.
    """
    parsed_url = urlparse(url)
    # Normalize URL by removing fragment and trailing slash
    normalized_url = parsed_url._replace(fragment="").geturl().rstrip("/")

    # Check if domain matches to subdomain level in TARGET_DOMAINS
    for target_domain in TARGET_DOMAINS:
        if parsed_url.hostname == target_domain:
            return normalized_url
    return None
def central_thread(urls):
    """
    Central thread to initialize URL list.
    """
    # initialize saved progress if found
    if os.path.exists("crawl_progress.json"):
        with open("crawl_progress.json", "r", encoding="utf-8") as f:
            try:
                saved_data = json.load(f)
                url_status['pending'].update(saved_data.get('pending', []))
                url_status['in_progress'].update(saved_data.get('in_progress', []))
                url_status['crawled'].update(saved_data.get('crawled', []))
                url_status['errors'].update(saved_data.get('errors', []))
                print("Loaded crawl progress from 'crawl_progress.json'.")
            except json.JSONDecodeError:
                print("Error loading 'crawl_progress.json'. Starting fresh.")
                url_status['pending'].update(urls)
    else:
        url_status['pending'].update(urls)

    # initialize the set with URLs to be crawled
    with lock:
        url_status['pending'].update(urls)

    print("Central thread initialized with URLs.")
    while url_status['pending'] or url_status['in_progress']:
        time.sleep(1)  # To reduce CPU usage
    print("All URLs processed. Shutting down central thread.")

def has_duplicate_segments(url):
    """
    Check if the URL contains duplicate path segments.
    """
    parsed_url = urlparse(url)
    path_segments = parsed_url.path.strip("/").split("/")
    return len(path_segments) != len(set(path_segments))

def worker_thread(name):
    """
    Worker thread to crawl URLs as assigned by the central thread.
    """
    while True:
        time.sleep(0.5)
        url = None
        try:
            with lock:
                if not url_status['pending'] and not url_status['in_progress']:
                    break  # Exit if no more URLs to process

                # Get a URL from the pending set
                if url_status['pending']:
                    url = url_status['pending'].pop()  # Remove URL from pending
                    url_status['in_progress'].add(url)  # Mark as in progress
                    if url in url_status['crawled'] or has_duplicate_segments(url):
                        url_status['in_progress'].remove(url)
                        continue
                else:
                    continue
                print(
                    f"{name}: Pending: {len(url_status['pending'])}, In Progress: {len(url_status['in_progress'])}, Crawled: {len(url_status['crawled'])}, Errors: {len(url_status['errors'])}")

            # Crawl outside the lock
            print(f"{name}: Crawling {url}")
            r = requests.get(url, timeout=10)  # Add timeout to avoid hanging
            r.raise_for_status()  # Raise an HTTPError for bad responses

            # Check if the response is binary content by inspecting Content-Type
            content_type = r.headers.get('Content-Type', '').lower()
            if 'text' not in content_type and 'html' not in content_type:
                print(f"{name}: Skipping binary content at {url} (Content-Type: {content_type})")
                with lock:
                    url_status['in_progress'].remove(url)
                    url_status['errors'].add(url)  # Mark as error since we cannot process it
                continue  # Skip further processing for this URL
            soup = BeautifulSoup(r.text, "html.parser")

            # Extract all tables and convert to Markdown
            tables_as_markdown = []
            for table in soup.find_all("table"):
                markdown_table = convert_table_to_markdown(table)
                tables_as_markdown.append(markdown_table)
                table.decompose()  # Remove the table from the HTML

            # Extract all images and get their srcs
            images = []
            for img in soup.find_all("img"):
                if img is not None:
                    src = img.get("src")  # Safely get the src attribute
                    if src:  # Only process if src exists
                        # Convert relative URLs to absolute URLs
                        absolute_src = urljoin(url, src)
                        images.append(absolute_src)
                    img.decompose()

            # Extract all links and add those ending in ".pdf" to the attachments list
            attachments = []
            for link in soup.find_all("a", href=True):
                href = link["href"]
                # Convert relative URLs to absolute URLs
                absolute_href = urljoin(url, href)
                # Check if the link is a PDF
                if ".pdf" in absolute_href.lower():  # Case-insensitive check for ".pdf"
                    attachments.append(absolute_href)

            # Prepare the data entry
            entry = {
                "_id": {},  # Placeholder, if needed
                "url": url,
                "text": clean_text(soup.get_text()),  # Convert soup to text after removing tables
                "images": images,
                "tables": tables_as_markdown,
                "attachments": attachments,
            }

            # Collect and validate all URLs on the page
            all_urls = set()
            for link in soup.find_all("a", href=True):
                href = link["href"]

                # Parse the base URL
                parsed_url = urlparse(url)
                base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

                # Handle paths starting with "/"
                if href.startswith("/"):
                    absolute_url = urljoin(base_url, href)
                else:
                    # Convert relative URLs to absolute URLs
                    absolute_url = urljoin(url, href)

                # Skip "mailto:" and "tel:" links
                if "mailto:" in href.lower() or "tel:" in href.lower() or has_duplicate_segments(url):
                    continue

                # Validate URL
                valid_url = is_valid_url(absolute_url)
                if valid_url:
                    all_urls.add(valid_url)

            # Write each entry to the JSON file immediately after finishing crawling
            with lock:
                url_status['in_progress'].remove(url)
                url_status['crawled'].add(url)
                print(f"{name}: Finished crawling {url}")

                # Update the JSON file
                if os.path.exists("crawled_data.json"):
                    # Load existing data and append the new entry
                    with open("crawled_data.json", "r+", encoding="utf-8") as f:
                        try:
                            data = json.load(f)
                        except json.JSONDecodeError:
                            data = []
                        data.append(entry)
                        f.seek(0)
                        json.dump(data, f, ensure_ascii=False, indent=4)
                        f.truncate()
                else:
                    # Create a new file with the first entry
                    with open("crawled_data.json", "w", encoding="utf-8") as f:
                        json.dump([entry], f, ensure_ascii=False, indent=4)

                # Add new URLs to the pending list
                for new_url in all_urls:
                    if new_url not in url_status['pending'] and new_url not in url_status[
                        'in_progress'] and new_url not in url_status['crawled'] and new_url not in url_status['errors']:
                        url_status['pending'].add(new_url)

                # Save progress to 'crawl_progress.json'
                with open("crawl_progress.json", "w", encoding="utf-8") as f:
                    json.dump({
                        'pending': list(url_status['pending']),
                        'in_progress': list(url_status['in_progress']),
                        'crawled': list(url_status['crawled']),
                        'errors': list(url_status['errors'])
                    }, f, ensure_ascii=False, indent=4)

        except Exception as e:
            with lock:
                if url:
                    url_status['in_progress'].remove(url)
                    url_status['errors'].add(url)  # Add to errors set
                print(f"{name}: Error processing {url}: {e}")
                # Save progress including errors
                with open("crawl_progress.json", "w", encoding="utf-8") as f:
                    json.dump({
                        'pending': list(url_status['pending']),
                        'in_progress': list(url_status['in_progress']),
                        'crawled': list(url_status['crawled']),
                        'errors': list(url_status['errors'])
                    }, f, ensure_ascii=False, indent=4)

# Helper function to convert table to Markdown
def convert_table_to_markdown(table):
    rows = table.find_all("tr")
    markdown_table = []

    for i, row in enumerate(rows):
        cells = row.find_all(["th", "td"])
        row_data = [cell.get_text(strip=True) for cell in cells]
        markdown_table.append('| ' + ' | '.join(row_data) + ' |')

        # Add separator after header row
        if i == 0:
            markdown_table.append('|' + '---|' * len(cells))

    return '\n'.join(markdown_table)


# URLs to crawl
urls_to_crawl = [
    "https://oisp.hcmut.edu.vn/",
]
TARGET_DOMAINS = ["oisp.hcmut.edu.vn"]


# Start the central thread
central = threading.Thread(target=central_thread, args=(urls_to_crawl,))
central.start()

# Start worker threads
workers = []
for i in range(5):  # Number of worker threads
    worker = threading.Thread(target=worker_thread, args=(f"Worker-{i + 1}",))
    workers.append(worker)
    worker.start()

# Wait for all threads to complete
for worker in workers:
    worker.join()
central.join()

