#! /usr/bin/env python3
"""
A script that uses multiprocessing to extract article data
from the JSON files in Papers Past open data tar files.

The JSON open data is released as one tar file per newspaper title,
containing one gzipped JSON file per issue. The data is saved by
newspaper issue in a pandas dataframe in parquet format with detailed
error and completion logging in separate files.

This is a complementary script to multiprocess_pp_issues_mets_alto_full.py,
which extracts article data from the METS/ALTO XML open data.

Author: Karin Stahel
Claude Opus 5.5 was used to assist with aspects of code development.
"""
# %%
import os
import re
from functools import partial
import tarfile
import gzip
import time
from multiprocessing import Pool, cpu_count
import logging
import json
import argparse
import pandas as pd
from tqdm import tqdm
import importlib.util
import sys

# Check if either pyarrow or fastparquet is installed
def check_parquet_dependencies():
    """
    Check if either pyarrow or fastparquet is installed for parquet file support.
    """
    pyarrow_available = importlib.util.find_spec("pyarrow") is not None
    fastparquet_available = importlib.util.find_spec("fastparquet") is not None

    if not (pyarrow_available or fastparquet_available):
        print("ERROR: Neither 'pyarrow' nor 'fastparquet' is installed.")
        print("Please install one of these packages to support parquet file operations:")
        print("  pip install pyarrow")
        print("  or")
        print("  pip install fastparquet")
        sys.exit(1)

    if pyarrow_available:
        return "pyarrow"
    return "fastparquet"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("json_extraction.log"),
        logging.StreamHandler()
    ]
)

# Check for parquet dependencies before proceeding
parquet_engine = check_parquet_dependencies()
logging.info(f"Using {parquet_engine} for parquet file operations")


# Map JSON list fields of the logical items to output column names
LIST_FIELDS = {
    "TY": "type",
    "txTI": "title",
    "txIN": "text",
    "txAU": "author",
    "CITATION": "citation",
    "REGION": "region",
}

# Map keys of the RIGHTS dict to output column names
RIGHTS_FIELDS = {
    "statement": "rights_statement",
    "text": "rights_text",
    "url": "rights_url",
}

KNOWN_KEYS = {"ID", "LE", "RIGHTS"} | set(LIST_FIELDS)

KNOWN_LEVELS = {"Document", "Page", "Logical"}

# %%
def get_tar_index(tar_path):
    """
    Map each file in a tar to its (offset, size) so issues can be read directly
    without scanning the tar. Scanning a large tar is slow, so the index is saved
    to a tar_index directory alongside the tar files and reused while it is newer
    than the tar.

    Args:
        tar_path: Path to the newspaper tar file

    Returns:
        Dictionary mapping tar member names to [offset, size] lists
    """
    index_dir = os.path.join(os.path.dirname(tar_path), "tar_index")
    newspaper_id = os.path.basename(tar_path)[:-len(".tar")]
    index_file = os.path.join(index_dir, f"{newspaper_id}.json")

    if os.path.exists(index_file) and os.path.getmtime(index_file) >= os.path.getmtime(tar_path):
        with open(index_file, encoding = "utf-8") as f:
            return json.load(f)

    logging.info(f"Building tar index for {tar_path}")
    with tarfile.open(tar_path, "r") as tar:
        tar_index = {m.name: [m.offset_data, m.size] for m in tar.getmembers() if m.isfile()}

    try:
        os.makedirs(index_dir, exist_ok=True)
        with open(index_file, "w", encoding = "utf-8") as f:
            json.dump(tar_index, f)
    except OSError as e:
        logging.warning(f"Could not save tar index {index_file}: {str(e)}")

    return tar_index

# %%
def find_tar(newspaper_id, input_paths):
    """
    Find the tar file for a newspaper title in the input directories.

    Args:
        newspaper_id: Newspaper code (e.g., "CHP")
        input_paths: List of paths to the input directories

    Returns:
        Full path to the tar file, or None if not found
    """
    for input_path in input_paths:
        potential_path = os.path.join(input_path, f"{newspaper_id}.tar")
        if os.path.exists(potential_path):
            return potential_path
    return None

# %%
def list_value(item, key):
    """
    Return the value of a JSON list field, or None if the field is missing or empty.
    Multiple values (e.g. several authors in txAU) are joined with "; ".

    Args:
        item: Dictionary for a single JSON item
        key: Field name

    Returns:
        String value or None
    """
    values = item.get(key)
    if not values:
        return None
    if isinstance(values, str):
        return values
    return "; ".join(str(v) for v in values)

# %%
def extract_articles_from_json(items, issue_code):
    """
    Given the list of items from an issue JSON file, return a dictionary of the
    logical items (articles, advertisements, etc.) with their text and metadata.

    The issue JSON contains one Document item for the issue, one Page item for
    each page and one Logical item for each article, advertisement, etc.

    Args:
        items: List of item dictionaries from the issue JSON file
        issue_code: Issue code identifier

    Returns:
        Tuple of (dictionary mapping item IDs to lists of column values,
        number of pages in the issue, issue type from the Document item)
    """
    issue_pages = sum(1 for item in items if "Page" in (item.get("LE") or []))
    issue_type = next((list_value(item, "TY") for item in items
                       if "Document" in (item.get("LE") or [])), None)

    articles = {}
    unknown_keys = set()
    unknown_levels = set()

    for item in items:
        level = item.get("LE") or []
        if not KNOWN_LEVELS.intersection(level):
            unknown_levels.add(", ".join(level) or "missing")
        if "Logical" not in level:
            continue

        item_id = item.get("ID")
        if not item_id:
            continue

        unknown_keys.update(set(item) - KNOWN_KEYS)

        rights = item.get("RIGHTS") or {}
        articles[item_id] = (
            [", ".join(level)]
            + [list_value(item, key) for key in LIST_FIELDS]
            + [rights.get(key) for key in RIGHTS_FIELDS]
            + [issue_pages]
        )

    if unknown_keys:
        logging.warning(f"Issue {issue_code}: Unknown JSON fields not extracted: {', '.join(sorted(unknown_keys))}")
    if unknown_levels:
        logging.warning(f"Issue {issue_code}: Items with unknown LE levels not extracted: {'; '.join(sorted(unknown_levels))}")

    return articles, issue_pages, issue_type

# %%
def process_issue(args, output_path, rev_date):
    """
    Process by single issue - extracting article data from the issue JSON file.

    Args:
        args: Tuple of (issue_code, (tar_path, offset, size))
        output_path: Path to the output directory
        rev_date: Revision date for output files

    Returns:
        Tuple of (issue_code, success_flag, number_of_articles, number_of_pages, issue_type)
    """
    issue_code, (tar_path, offset, size) = args
    start_time = time.time()

    try:
        with open(tar_path, "rb") as tar_file:
            tar_file.seek(offset)
            # strict=False allows the control characters found in the text of some issues
            items = json.loads(gzip.decompress(tar_file.read(size)), strict = False)

        articles, issue_pages, issue_type = extract_articles_from_json(items, issue_code)

        # Issues with an IRREGULARITY type have no pages or articles
        if len(articles) == 0:
            logging.info(f"No articles found in JSON file for {issue_code} (issue type {issue_type})")
            return issue_code, True, 0, issue_pages, issue_type

        df = pd.DataFrame.from_dict(
            articles,
            orient = "index",
            columns = [
                "level",                    # LE: item level, e.g. "Logical, Primary"
                "type",                     # TY: e.g. ARTICLE, ADVERTISEMENT, SECTION, TITLE_SECTION
                "title",                    # txTI: article title
                "text",                     # txIN: full article text (including the heading)
                "author",                   # txAU: author(s), where given
                "citation",                 # CITATION: newspaper, volume, issue, date and page
                "region",                   # REGION: region of the newspaper
                "rights_statement",         # RIGHTS statement
                "rights_text",              # RIGHTS text
                "rights_url",               # RIGHTS url
                "issue_pages",              # Number of pages in the issue
            ]
        )
        df.index.name = "article_id"

        # Set an explicit dtype so columns that are empty in an issue (e.g. author)
        # have the same parquet schema across all issues
        string_cols = [col for col in df.columns if col != "issue_pages"]
        df[string_cols] = df[string_cols].astype("string")

        output_file_path = os.path.join(
            output_path,
            "pp_issue_json_dfs",
            f"PP_JSON_{issue_code}_{rev_date}.parquet"
        )

        df.to_parquet(output_file_path, engine = parquet_engine)

        elapsed = time.time() - start_time
        logging.info(f"Processed {issue_code} with {len(df)} articles in {elapsed:.2f} seconds")

        return issue_code, True, len(df), issue_pages, issue_type

    except Exception as e:
        logging.error(f"Error processing {issue_code}: {str(e)}")

        return issue_code, False, 0, 0, None

# %%
def batch_process_issues(issues, max_workers, output_path, rev_date):
    """
    Process multiple issues in parallel.

    Args:
        issues: Dictionary mapping issue codes to (tar_path, offset, size) tuples
        max_workers: Maximum number of parallel workers
        output_path: Path to the output directory
        rev_date: Revision date for output files

    Returns:
        Tuple of (successful_issues, failed_issues, empty_issues, statistics)
    """
    if not issues:
        print("No issues to process")
        return [], [], {}, {}

    # Determine optimal worker count if not specified
    if max_workers is None:
        max_workers = max(1, min(cpu_count() - 1, len(issues))) # Leave one core free for system processes

    print(f"Starting parallel processing with {max_workers} workers for {len(issues)} issues")

    issue_items = list(issues.items())

    process_issue_partial = partial(process_issue,
                                    output_path = output_path,
                                    rev_date = rev_date)

    if max_workers == 1 or len(issue_items) == 1:
        # Single-process mode for small jobs or debugging
        results = []
        for item in tqdm(issue_items, desc="Processing issues"):
            results.append(process_issue_partial(item))
    else:
        with Pool(processes=max_workers) as pool:
            results = list(tqdm(
                pool.imap(process_issue_partial, issue_items, chunksize = 16),
                total = len(issue_items),
                desc = "Processing issues"
            ))

    successful = [r[0] for r in results if r[1] and r[2] > 0]
    failed = [r[0] for r in results if not r[1]]
    empty = {r[0]: r[4] for r in results if r[1] and r[2] == 0}

    stats = {r[0]: {"articles": r[2], "pages": r[3]} for r in results if r[1] and r[2] > 0}

    print(f"Completed processing {len(successful)} issues successfully, {len(failed)} failed, "
          f"{len(empty)} without articles")

    if failed:
        print(f"Failed issues: {', '.join(failed)}")

    return successful, failed, empty, stats

# %%
def discover_issues(input_paths, newspaper_ids = None, newspaper_year_codes = None):
    """
    Discover issues in the newspaper tar files in the input directories.

    Args:
        input_paths: List of paths to the directories containing tar files
        newspaper_ids: List of newspaper codes to process, e.g. CHP (optional)
        newspaper_year_codes: List of newspaper_year codes to process, e.g. CHP_1903 (optional)

    Returns:
        Dictionary mapping issue codes to (tar_path, offset, size) tuples
    """
    if newspaper_year_codes:
        newspaper_ids = sorted({code.split("_")[0] for code in newspaper_year_codes})

    issues = {}
    tar_count = 0
    for input_path in input_paths:
        tar_files = [f for f in os.listdir(input_path) if f.endswith(".tar")]

        for tar_file in tar_files:
            newspaper_match = re.fullmatch(r"([A-Z]+)\.tar", tar_file)

            if not newspaper_match:
                logging.warning(f"Skipping file with unexpected format: {tar_file}")
                continue

            newspaper = newspaper_match.group(1)

            if newspaper_ids and newspaper not in newspaper_ids:
                continue

            tar_path = os.path.join(input_path, tar_file)
            try:
                tar_index = get_tar_index(tar_path)
            except Exception as e:
                logging.error(f"Problem with tar file {tar_file}: {str(e)}")
                continue
            tar_count += 1

            # Path format: newspaper/year/date.json.gz
            for name, (offset, size) in tar_index.items():
                issue_match = re.fullmatch(fr"{newspaper}/(\d{{4}})/(\d{{8}})\.json\.gz", name)
                if not issue_match:
                    continue
                year, date_str = issue_match.groups()
                if newspaper_year_codes and f"{newspaper}_{year}" not in newspaper_year_codes:
                    continue
                issue_code = f"{newspaper}_{date_str}"
                if issue_code not in issues:
                    issues[issue_code] = (tar_path, offset, size)

    logging.info(f"Discovered {len(issues)} issues across {tar_count} tar files")

    return issues

# %%
def locate_issues(issue_codes, input_paths):
    """
    Locate the JSON files for a list of issue codes in the newspaper tar files.

    Args:
        issue_codes: List of issue codes (e.g., ["CHP_19031228"])
        input_paths: List of paths to the directories containing tar files

    Returns:
        Dictionary mapping issue codes to (tar_path, offset, size) tuples
    """
    issues = {}
    tar_indexes = {}

    for issue_code in issue_codes:
        parts = issue_code.split("_")
        if len(parts) != 2 or not re.fullmatch(r"\d{8}", parts[1]):
            logging.error(f"Skipping issue {issue_code}: Invalid issue code format. Expected format: TITLE_YYYYMMDD")
            continue

        newspaper_id, date_str = parts
        if newspaper_id not in tar_indexes:
            tar_path = find_tar(newspaper_id, input_paths)
            tar_indexes[newspaper_id] = (tar_path, get_tar_index(tar_path) if tar_path else None)
        tar_path, tar_index = tar_indexes[newspaper_id]

        if tar_path is None:
            logging.error(f"Skipping issue {issue_code}: Tar file {newspaper_id}.tar not found in any input paths")
            continue

        json_file = f"{newspaper_id}/{date_str[:4]}/{date_str}.json.gz"
        if json_file not in tar_index:
            logging.error(f"Skipping issue {issue_code}: {json_file} not found in {tar_path}")
            continue

        offset, size = tar_index[json_file]
        issues[issue_code] = (tar_path, offset, size)

    return issues

# %%
def parse_arguments():
    """Parse the command line arguments"""
    parser = argparse.ArgumentParser(description = "Papers Past Open Data JSON Extraction Script")

    parser.add_argument("--input", dest = "input_paths", nargs = "+", required = True,
                        help = "Input directories containing tar files")

    parser.add_argument("--output", dest = "output_path", required = True,
                        help = "Output directory for processed files")

    parser.add_argument("--date", dest = "rev_date", required = False,
                        help = "Revision date for output files (e.g., '20250305'). If not provided, current date will be used.")

    parser.add_argument("--workers", dest = "max_workers", type = int, default = None,
                        help = "Maximum number of parallel workers (default: auto)")

    parser.add_argument("--issue-file", dest = "issue_file", default=None,
                        help = "File containing list of issue codes to process (optional)")

    parser.add_argument("--issues", dest = "issues", nargs = "+", default=[],
                        help = "Space-separated list of issue codes to process (optional)")

    parser.add_argument("--newspaper-year-file", dest = "newspaper_year_file", default=None,
                        help= "File containing list of newspaper_year codes to process (optional)")

    parser.add_argument("--newspaper-year-codes", dest="newspaper_codes", nargs="+", default=[],
                        help="Space-separated list of newspaper_year codes to process, e.g. CHP_1903 (optional)")

    parser.add_argument("--newspapers", dest="newspapers", nargs="+", default=[],
                        help="Space-separated list of newspaper codes to process all years of, e.g. CHP (optional)")

    args = parser.parse_args()

    # If no date provided, use current date
    if not args.rev_date:
        args.rev_date = time.strftime("%Y%m%d")
        print(f"No date specified, using current date: {args.rev_date}")

    return args

# %%
def main():
    """Main function to run the Papers Past Open Data JSON extraction process."""

    args = parse_arguments()

    input_paths = args.input_paths
    output_path = args.output_path
    rev_date = args.rev_date

    os.makedirs(os.path.join(output_path, "pp_issue_json_dfs"), exist_ok=True)
    os.makedirs(os.path.join(output_path, "pp_issue_processing_summaries"), exist_ok=True)

    # Option 1: Process specific issues from command line
    if args.issues:
        issues = locate_issues(args.issues, input_paths)

    # Option 2: Process issues from a file
    elif args.issue_file:
        with open(args.issue_file, "r", encoding = "utf-8") as f:
            issue_codes = [line.strip() for line in f if line.strip()]
        issues = locate_issues(issue_codes, input_paths)

    # Option 3: Process specified newspaper_year codes in input paths
    elif args.newspaper_year_file:
        with open(args.newspaper_year_file, "r", encoding = "utf-8") as f:
            newspaper_year_codes = [line.strip() for line in f if line.strip()]
        issues = discover_issues(input_paths, newspaper_year_codes = newspaper_year_codes)

    # Option 4: Process specified newspaper_year codes from command line
    elif args.newspaper_codes:
        issues = discover_issues(input_paths, newspaper_year_codes = args.newspaper_codes)

    # Option 5: Process all years of specified newspapers from command line
    elif args.newspapers:
        issues = discover_issues(input_paths, newspaper_ids = args.newspapers)

    # Option 6: Process all issues in input paths
    else:
        issues = discover_issues(input_paths)

    print(f"Loaded {len(issues)} issues to process")

    # Process the issues
    start_time = time.time()
    successful, failed, empty, stats = batch_process_issues(issues,
                                                     args.max_workers,
                                                     output_path,
                                                     rev_date)
    elapsed = time.time() - start_time

    summary = {
        "total_issues": len(issues),
        "successful": len(successful),
        "failed": len(failed),
        "empty": len(empty),
        "elapsed_seconds": elapsed,
        "failed_issues": failed,
        "successful_issues": successful,
        "empty_issues": empty,
        "stats": stats,
    }

    summary_path = os.path.join(
        output_path,
        "pp_issue_processing_summaries",
        f"summary_json_{time.strftime('%Y%m%d_%H%M%S')}.json"
    )

    with open(summary_path, "w", encoding = "utf-8") as f:
        json.dump(summary, f, indent=2)

    # Statistics and problems
    total_articles = sum(stat["articles"] for stat in stats.values())
    total_pages = sum(stat["pages"] for stat in stats.values())

    print(f"Processed {len(successful)} issues successfully in {elapsed:.2f} seconds")
    print(f"Total articles extracted: {total_articles}")
    print(f"Total pages processed: {total_pages}")
    print(f"Issues without articles (e.g. IRREGULARITY issues): {len(empty)}")
    if successful:
        print(f"Average articles per issue: {total_articles / len(successful):.2f}")
        print(f"Average processing time per issue: {elapsed / len(issues):.2f} seconds")
    else:
        print("No issues were successfully processed")

    print(f"Summary saved to {summary_path}")
    print(f"Finished processing!")

if __name__ == "__main__":
    main()
