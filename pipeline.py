#!/usr/bin/env -S uv run
# /// script
# dependencies = [
#   "dlt",
#   "duckdb",
#   "crossrefapi",
#   "pandas"
# ]
# ///

import dlt
from crossref.restful import Works, Etiquette
import pandas as pd

def get_citations_by_year(doi):
    # Create Works instance with polite etiquette
    etiquette = Etiquette(
        "CitationTracker",
        "1.0",
        "https://github.com/yourusername/project",
        "your@email.com",
    )
    works = Works(etiquette=etiquette)

    # Get work details for the DOI
    work = works.doi(doi)

    if not work:
        return None

    # Get citation counts by year from the breakdowns
    citations = work.get("breakdowns", {}).get("dois-by-issued-year", [])

    # Convert to dataframe
    df = pd.DataFrame(citations, columns=["year", "citations"])
    df["doi"] = doi

    return df

def main():
    # List of DOIs to analyze
    dois = [
        "10.1038/nbt.3820",  # Original Nextflow paper
        "10.1093/bioinformatics/bts480",  # Snakemake paper
        "10.1186/gb-2010-11-8-r86",  # Galaxy paper
    ]

    # Get citations for each DOI
    all_citations = []
    for doi in dois:
        citations_df = get_citations_by_year(doi)
        if citations_df is not None:
            all_citations.append(citations_df)

    # Combine all results
    if all_citations:
        final_df = pd.concat(all_citations)

        # Save to CSV
        final_df.to_csv("crossref_citations.csv", index=False)
        print("Citations saved to crossref_citations.csv")

if __name__ == "__main__":
    main()
