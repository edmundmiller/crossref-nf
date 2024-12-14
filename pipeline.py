import dlt
from dlt.sources.helpers import requests
import pandas as pd
from typing import Iterator, Dict
from datetime import datetime


@dlt.source
def crossref_source():
    @dlt.resource(write_disposition="replace")
    def citations(doi_list: list[str]) -> Iterator[Dict]:
        for doi in doi_list:
            # Make request to Crossref API
            response = requests.get(
                f"https://api.crossref.org/works/{doi}/citations",
                headers={"User-Agent": "DLT Pipeline (mailto:your-email@example.com)"},
            )
            if response.status_code == 200:
                data = response.json()
                if "message" in data and "items" in data["message"]:
                    for item in data["message"]["items"]:
                        # Extract relevant information
                        yield {
                            "source_doi": doi,
                            "citing_doi": item.get("DOI"),
                            "citing_title": item.get("title", [None])[0],
                            "publication_date": item.get("published-print", {}).get(
                                "date-parts", [[None]]
                            )[0][0],
                            "type": item.get("type"),
                            "container_title": item.get("container-title", [None])[0],
                        }


def run_pipeline():
    # Initialize pipeline
    pipeline = dlt.pipeline(
        pipeline_name="crossref_citations",
        destination="filesystem",
        dataset_name="citations_data",
    )

    # List of DOIs to analyze
    dois = [
        "10.1038/nbt.3820",  # Original Nextflow paper
        "10.1093/bioinformatics/bts480",  # Snakemake paper
        "10.1186/gb-2010-11-8-r86",  # Galaxy paper
    ]

    # Run the pipeline
    info = pipeline.run(
        crossref_source().citations(dois),
        table_name="citations",
        write_disposition="replace",
    )

    # Convert the results to a CSV
    pipeline_path = pipeline.pipeline_path("citations")
    df = pd.read_parquet(pipeline_path)
    df.to_csv("crossref_citations.csv", index=False)


if __name__ == "__main__":
    run_pipeline()
