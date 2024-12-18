#!/usr/bin/env -S uv run
# /// script
# dependencies = [
#   "pandas",
#   "httpx"
# ]
# ///

import httpx
import pandas as pd
from typing import Iterator, Dict

def extract_date(data: Dict) -> str:
    """Safely extract publication date from Crossref data"""
    # Try published-print first
    date_parts = data.get("published-print", {}).get("date-parts", [[]])
    if date_parts and date_parts[0]:
        return str(date_parts[0][0])

    # Fall back to published-online
    date_parts = data.get("published-online", {}).get("date-parts", [[]])
    if date_parts and date_parts[0]:
        return str(date_parts[0][0])

    # Fall back to created date
    date_parts = data.get("created", {}).get("date-parts", [[]])
    if date_parts and date_parts[0]:
        return str(date_parts[0][0])

    return ""

def crossref_citations(dois: list[str]) -> Iterator[Dict]:
    """Fetch citations from Crossref API"""
    headers = {"User-Agent": "CitationTracker/1.0 (mailto:emiller@cursor.so)"}

    with httpx.Client() as client:
        for doi in dois:
            response = client.get(
                f"https://api.crossref.org/works/{doi}", headers=headers
            )
            response.raise_for_status()
            data = response.json()["message"]

            # Extract and flatten relevant fields
            yield {
                "doi": data.get("DOI"),
                "title": data.get("title", [""])[0]
                if data.get("title")
                else "",  # Handle empty title list
                "published_date": extract_date(data),
                "type": data.get("type"),
                "container_title": data.get("container-title", [""])[0]
                if data.get("container-title")
                else "",
                "author_count": len(data.get("author", [])),
                "citation_count": data.get("is-referenced-by-count"),
                "references_count": data.get("references-count"),
                "publisher": data.get("publisher"),
                "url": data.get("URL"),
            }


def main():
    # List of DOIs to process
    dois = [
        "10.1038/nbt.3820",  # Original Nextflow paper
        "10.1093/bioinformatics/bts480",  # Snakemake paper
        "10.1186/gb-2010-11-8-r86",  # Galaxy paper
    ]

    # Collect all citations
    citations = list(crossref_citations(dois))

    # Convert to DataFrame and save
    df = pd.DataFrame(citations)
    df.to_csv("crossref_citations.csv", index=False)
    print(f"Successfully saved {len(df)} citations to crossref_citations.csv")

if __name__ == "__main__":
    main()
