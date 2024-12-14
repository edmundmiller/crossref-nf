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
import pandas as pd
from typing import Iterator, Dict
from datetime import datetime
import time
from habanero import Crossref, WorksContainer
import backoff
from functools import partial
from tqdm import tqdm
from tqdm.auto import tqdm as tqdm_auto  # Works in both terminal and notebooks


@backoff.on_exception(backoff.expo, Exception, max_tries=5)
def get_work(cr: Crossref, doi: str) -> Dict:
    """Get work details with retries"""
    try:
        result = cr.works(ids=doi)
        if not result or "message" not in result:
            print(f"No data found for DOI: {doi}")
            return None
        return result
    except Exception as e:
        print(f"Error fetching work for DOI {doi}: {str(e)}")
        raise

@backoff.on_exception(backoff.expo, Exception, max_tries=5)
def get_citing_works(cr: Crossref, doi: str, cursor: str = "*") -> Dict:
    """Get works that cite this DOI with retries using cursor pagination"""
    return cr.works(
        query=f"reference.DOI:{doi}",
        cursor=cursor,
        limit=100,
        select=[
            "DOI",
            "title",
            "published-print",
            "published-online",
            "type",
            "container-title",
            "reference",
        ],
    )


def get_citations(doi: str, cr: Crossref) -> Iterator[Dict]:
    """Helper function to get citations for a single DOI with improved pagination"""
    try:
        # First get source work details
        work = get_work(cr, doi)
        if not work:
            return

        work = work["message"]
        source_info = {
            "source_doi": doi,
            "source_title": work.get("title", [None])[0],
            "source_published": work.get("published-print", {}).get(
                "date-parts", [[None]]
            )[0][0],
            "total_citations": work.get("is-referenced-by-count", 0),
        }

        # Show which paper we're processing
        print(f"\nProcessing citations for: {source_info['source_title']}")
        print(f"Total citations to process: {source_info['total_citations']}")

        # Get all citing works using cursor pagination
        if source_info["total_citations"] > 0:
            citations_by_year = {}
            cursor = "*"
            processed = 0

            # Create progress bar for this paper's citations
            pbar = tqdm_auto(
                total=source_info["total_citations"],
                desc="Fetching citations",
                unit="citations",
            )

            while True:
                citing_works = get_citing_works(cr, doi, cursor)
                if not citing_works or "message" not in citing_works:
                    break

                works_container = WorksContainer(citing_works)

                if not works_container.items:
                    break

                # Process each citing work
                for item in works_container.items:
                    published = item.get(
                        "published-print", item.get("published-online", {})
                    )
                    if (
                        published
                        and "date-parts" in published
                        and published["date-parts"][0][0]
                    ):
                        year = published["date-parts"][0][0]
                        citations_by_year[year] = citations_by_year.get(year, 0) + 1
                        processed += 1

                # Update progress bar
                pbar.n = processed
                pbar.refresh()

                # Get next cursor
                cursor = works_container.next_cursor
                if not cursor:
                    break

                time.sleep(1)  # Rate limiting

            pbar.close()

            # Yield citation counts by year
            for year, count in sorted(citations_by_year.items()):
                citation_info = source_info.copy()
                citation_info.update({"year": year, "citation_count": count})
                yield citation_info
        else:
            # If no citations, yield a single record with the current year
            citation_info = source_info.copy()
            citation_info.update({"year": datetime.now().year, "citation_count": 0})
            yield citation_info

    except Exception as e:
        print(f"Error processing citations for DOI {doi}: {str(e)}")


@dlt.source
def crossref_citations(dois: list[str] = None):
    """DLT source for Crossref citations data"""
    if dois is None:
        dois = [
            "10.1038/nbt.3820",  # Original Nextflow paper
            "10.1093/bioinformatics/bts480",  # Snakemake paper
            "10.1186/gb-2010-11-8-r86",  # Galaxy paper
        ]

    # Initialize Crossref client with proper user agent and timeout
    cr = Crossref(
        mailto="crossref@edmundmiller.dev", ua_string="CitationTracker/1.0", timeout=30
    )

    @dlt.resource(primary_key=["source_doi", "year"], write_disposition="replace")
    def citations():
        # Add progress bar for processing DOIs
        for doi in tqdm_auto(dois, desc="Processing papers", unit="paper"):
            yield from get_citations(doi, cr)

    return citations


def run_pipeline():
    # Initialize pipeline with DuckDB
    pipeline = dlt.pipeline(
        pipeline_name="crossref_citations",
        destination="duckdb",
        dataset_name="citations_data",
    )

    # Run the pipeline
    load_info = pipeline.run(crossref_citations())

    # Export to CSV directly from DuckDB
    if load_info and load_info.load_packages:
        import duckdb

        conn = duckdb.connect("crossref_citations.duckdb")
        df = conn.execute("SELECT * FROM citations_data.citations").df()

        # Sort by source_doi and year for better readability
        df = df.sort_values(["source_doi", "year"])

        df.to_csv("crossref_citations.csv", index=False)
        print(f"Pipeline completed successfully. Data saved to crossref_citations.csv")
        print(f"Total records processed: {len(df)}")

        # Print summary
        summary = (
            df.groupby("source_doi")
            .agg(
                {
                    "source_title": "first",
                    "total_citations": "first",
                    "citation_count": "sum",
                }
            )
            .reset_index()
        )
        print("\nCitation Summary:")
        for _, row in summary.iterrows():
            print(f"\n{row['source_title']}")
            print(f"DOI: {row['source_doi']}")
            print(f"Total Citations: {row['total_citations']}")
            yearly_counts = df[df["source_doi"] == row["source_doi"]].sort_values(
                "year"
            )
            print("Citations by year:")
            for _, yr in yearly_counts.iterrows():
                print(f"  {yr['year']}: {yr['citation_count']}")

        conn.close()
    else:
        print("No data was loaded in the pipeline")

if __name__ == "__main__":
    run_pipeline()
