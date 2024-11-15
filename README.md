# Crawler for HCMUT OISP Website

This project is a web crawler designed to facilitate crawling data from the HCMUT OISP website. It gathers data from HCMUT's official website for research purposes.

## Features

- Extracts URLs and text content from pages within specified domains.
- Filters and normalizes URLs to avoid duplicates and unnecessary fragments.
- Saves the crawling progress and results to JSON files for easy access and analysis.
- Multithreaded design for efficient data extraction.

## Prerequisites

- Python 3.x
- An internet connection to access the target website.

## Installation

1. Clone this repository:

    ```bash
    git clone https://github.com/yourusername/hcmut-oisp-crawler.git
    cd hcmut-oisp-crawler
    ```

2. Install the required packages:

    ```bash
    pip install -r requirements.txt
    ```

    This will install all necessary dependencies listed in `requirements.txt`.

## Usage

1. To start the crawler, simply run the main Python script:

    ```bash
    python main.py
    ```


2. The script will start crawling from the URLs specified in the initial `urls_to_crawl` list. The progress is saved in `crawl_progress.json`, and the crawled data is saved incrementally in `crawled_data.json`.

## Output

- `crawl_progress.json`: Stores the current status of the crawling process, including pending, in-progress, and crawled URLs, allowing you to resume if interrupted.
- `crawled_data.json`: Contains the extracted data for each URL, including text content, images, tables, and other relevant information.
