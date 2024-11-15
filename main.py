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
    'crawled': set()
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


def worker_thread(name):
    """
    Worker thread to crawl URLs as assigned by the central thread.
    """
    while True:
        with lock:
            if not url_status['pending'] and not url_status['in_progress']:
                break  # Exit if no more URLs to process

            # Get a URL from the pending set
            if url_status['pending']:
                url = url_status['pending'].pop()  # Remove URL from pending
                url_status['in_progress'].add(url)  # Mark as in progress
            else:
                continue
        # Crawl outside the lock
        print(f"{name}: Crawling {url}")
        r = requests.get(url)
        soup = BeautifulSoup(r.text, "html.parser")

        # Prepare the data entry
        entry = {
            "_id": {},  # Placeholder, if needed
            "url": url,
            "text": clean_text(soup.get_text()),
            "images": [],  # Empty for now
            "tables": [],  # Empty for now
            "attachments": [],  # Empty for now
            "markdown": ""  # Empty for now
        }
        # Add this inside worker_thread, after soup is created
        all_urls = set()  # Set to collect unique URLs found on the page

        for link in soup.find_all("a", href=True):
            href = link["href"]
            # Convert relative URLs to absolute URLs
            absolute_url = urljoin(url, href)
            # Filter and normalize the URL
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
                # If the file exists, load current data and append the new entry
                with open("crawled_data.json", "r+", encoding="utf-8") as f:
                    try:
                        data = json.load(f)  # Load existing data
                    except json.JSONDecodeError:
                        data = []  # Start with an empty list if file is empty or corrupted
                    data.append(entry)  # Add the new entry
                    f.seek(0)  # Move to the start of the file to overwrite
                    json.dump(data, f, ensure_ascii=False, indent=4)
                    f.truncate()  # Ensure no leftover content from previous writes
            else:
                # If file doesn't exist, create a new one with the first entry
                with open("crawled_data.json", "w", encoding="utf-8") as f:
                    json.dump([entry], f, ensure_ascii=False, indent=4)
            # Add new urls
            for new_url in all_urls:
                # Ensure the URL is not already in pending, in_progress, or crawled
                if new_url not in url_status['pending'] and new_url not in url_status['in_progress'] and new_url not in \
                        url_status['crawled']:
                    url_status['pending'].add(new_url)
            # Save progress to 'crawl_progress.json' after finishing each URL
            with open("crawl_progress.json", "w", encoding="utf-8") as f:
                json.dump({
                    'pending': list(url_status['pending']),
                    'in_progress': list(url_status['in_progress']),
                    'crawled': list(url_status['crawled'])
                }, f, ensure_ascii=False, indent=4)


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
for i in range(3):  # Number of worker threads
    worker = threading.Thread(target=worker_thread, args=(f"Worker-{i + 1}",))
    workers.append(worker)
    worker.start()

# Wait for all threads to complete
for worker in workers:
    worker.join()
central.join()

