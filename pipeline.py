#!/usr/bin/env -S uv run
# /// script
# dependencies = [
#   "dlt",
#   "pandas",
#   "requests",
#   "duckdb",
#   "habanero",
#   "backoff"
# ]
# ///

import dlt
from dlt.sources.rest_api import rest_api_source

def crossref_citations(dois: list[str] = None):
    """DLT source for Crossref citations data"""
    if dois is None:
        dois = [
            "10.1038/nbt.3820",  # Original Nextflow paper
            "10.1093/bioinformatics/bts480",  # Snakemake paper
            "10.1186/gb-2010-11-8-r86",  # Galaxy paper
        ]

    # Define the REST API configuration
    config = {
        "client": {
            "base_url": "https://api.crossref.org/works/",
            "headers": {"User-Agent": "CitationTracker/1.0 (mailto:emiller@cursor.so)"},
        },
        "resources": [
            {
                "name": "works",
                "endpoint": {
                    "path": "{doi}",
                    "params": {"doi": {"type": "values", "values": dois}},
                },
            }
        ],
    }

    return rest_api_source(config)


def run_pipeline():
    # Initialize pipeline
    pipeline = dlt.pipeline(
        pipeline_name="crossref_citations",
        destination="duckdb",
        dataset_name="crossref_citations",
    )

    # Run the pipeline
    load_info = pipeline.run(crossref_citations())

    # Show summary if data was loaded
    if load_info and load_info.load_packages:
        with pipeline.sql_client() as client:
            df = client.query("SELECT * FROM crossref_citations.works").df()
            print(f"Pipeline completed successfully. Total records: {len(df)}")
    else:
        print("No data was loaded in the pipeline")

if __name__ == "__main__":
    run_pipeline()
