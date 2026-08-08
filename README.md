# Papers Past open data METS/ALTO extraction script

This Python script uses multiprocessing to efficiently extract article data from METS/ALTO XML (in tar.gz files) in the [National Library of New Zealand's Papers Past open data](https://natlib.govt.nz/about-us/open-data/papers-past-metadata/papers-past-newspaper-open-data-pilot/overview-papers-past-newspaper-open-data-pilot). The script processes the archive files and saves extracted article data by newspaper issue as dataframes in parquet format with detailed error and completion logging. Each row in the dataframe is an article in that newspaper issue.

## Features

- Extracts newspaper article content and layout related information from METS/ALTO XML files
- Processes multiple issues in parallel using multiprocessing
- Provides detailed logging and statistics about the extraction process
- Supports various input options (specific issues, newspaper codes, etc.)
- Outputs data in parquet format

## Installation

### Setting up a Python virtual environment

It's recommended to run this script in a virtual environment to manage dependencies cleanly. Open a terminal or command prompt and run the following commands:

#### Windows
```bash
# Create a new virtual environment
python -m venv pp_env

# Activate the environment
pp_env\Scripts\activate

# Install required dependencies using requirements.txt (recommended)
pip install -r requirements.txt
```

#### macOS/Linux
```bash
# Create a new virtual environment
python3 -m venv pp_env

# Activate the environment
source pp_env/bin/activate

# Install required dependencies using requirements.txt (recommended)
pip install -r requirements.txt
```

The `requirements.txt` file includes all necessary dependencies with appropriate version constraints, including pyarrow for parquet file operations. This is the recommended installation method to ensure compatibility.

If you need to install dependencies individually instead:
```bash
pip install pandas>=1.5.3 lxml>=4.9.2 tqdm>=4.65.0 pyarrow>=8.0.0
```

For more information on virtual environments, see the [Python documentation](https://docs.python.org/3/library/venv.html).

## Usage

Run this script from the command line using Python. As shown below, there are multiple ways to specify which newspaper issues to process, including individual issue codes, lists in text files, or newspaper-year combinations.

### Basic usage

```bash
python multiprocess_pp_issues_mets_alto_full.py --input /path/to/data --output /path/to/output
```

### Examples

#### Process all issues in input directories

```bash
python multiprocess_pp_issues_mets_alto_full.py --input /data/papers_past --output /results
```

#### Process specific issues by code

```bash
python multiprocess_pp_issues_mets_alto_full.py --input /data/papers_past --output /results --issues DSC_18471002 TC_18580910
```

#### Process issues listed in a file

```bash
python multiprocess_pp_issues_mets_alto_full.py --input /data/papers_past --output /results --issue-file issues.txt
```

Where `issues.txt` contains one issue code per line:
```
DSC_18471002
TC_18580910
NENZC_18571024
```

#### Process specific newspaper-year combinations

```bash
python multiprocess_pp_issues_mets_alto_full.py --input /data/papers_past --output /results --newspaper-year-codes DSC_1847 NENZC_1857
```

#### Process newspaper-year combinations listed in a file

```bash
python multiprocess_pp_issues_mets_alto_full.py --input /data/papers_past --output /results --newspaper-year-file newspaper_years.txt
```

Where `newspaper_years.txt` contains one newspaper-year code per line:
```
DSC_1847
TC_1858
NENZC_1857
```

#### Specify number of worker processes

```bash
python multiprocess_pp_issues_mets_alto_full.py --input /data/papers_past --output /results --workers 8
```

### Command line arguments

| Argument | Description |
|----------|-------------|
| `--input` | One or more input directories containing tar.gz files (required) |
| `--output` | Output directory for processed files (required) |
| `--date` | Revision date for output files (e.g., '20250329') (optional, defaults to current date) |
| `--workers` | Maximum number of parallel workers (default: automatic) |
| `--issue-file` | File containing list of issue codes to process |
| `--issues` | Space-separated list of issue codes to process |
| `--newspaper-year-file` | File containing list of newspaper_year codes to process |
| `--newspaper-year-codes` | Space-separated list of newspaper_year codes to process |

## Output structure

The script generates output in the following structure:

```
output_directory/
├── pp_issue_mets_alto_dfs/
│   ├── PP_NEWSPAPER_DATE_REVDATE.parquet
│   └── ...
└── pp_issue_processing_summaries/
    └── summary_YYYYMMDD_HHMMSS.json
```

Each parquet file contains extracted article data for a single newspaper issue, and the summary JSON file contains statistics and issues for the processing run.

### Note on dictionary columns

The parquet files contain four dictionary columns (`block_line_counts`, `block_style_refs`, `title_block_line_counts`, `title_block_style_refs`). Each maps the block IDs belonging to an article to a value: `block_line_counts` and `title_block_line_counts` map each block ID to its number of text lines, while `block_style_refs` and `title_block_style_refs` map each block ID to its `STYLEREFS` value.

To keep memory usage low, these columns are stored as **JSON strings** rather than as native dictionaries. Writing them as native dictionaries causes parquet to store the column as a struct, expanding it to include the keys from every row in the file and padding each article with `None` for the keys that don't belong to it. That padding is very expensive in memory once the file is read back into pandas. The JSON-string format avoids this with each row holding only its own block IDs and values.

To use the columns as dictionaries after reading a file, parse them back with `json.loads`. You can use the following helper function:

```python
import json


def parse_parquet_dicts(df, columns=None):
    """
    Parse the JSON-string dictionary columns of a Papers Past METS/ALTO
    dataframe (read from parquet into pandas) back into Python dictionaries.

    The four dictionary columns are stored as JSON strings on disk to keep
    memory usage low. This function parses them back into dictionaries, where
    each row's dict contains only its own block IDs and values.

    Args:
        df:         pandas df read from a Papers Past parquet file created with
                    the script in this repo.
        columns:    List of column names to parse. If None, defaults to
                    the four dict columns: block_line_counts, block_style_refs,
                    title_block_line_counts, title_block_style_refs

    Returns:
        Dataframe with the specified columns parsed into dictionaries
    """
    if columns is None:
        columns = [
            "block_line_counts",
            "block_style_refs",
            "title_block_line_counts",
            "title_block_style_refs",
        ]

    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda s: json.loads(s) if isinstance(s, str) else s
            )
    return df
```

Usage:

```python
df = pd.read_parquet("PP_CHP_19031228_20250305.parquet")
df = parse_parquet_dicts(df)
```

#### Recovering block IDs

Earlier versions of these files included separate `block_ids` and `title_block_ids` columns. These have been removed because the same information is held in the dictionary keys. Because the IDs are stored as the dictionary keys and JSON preserves their order, you can recover the block ID lists after parsing if required:

```python
# Content block IDs for the first article, in reading order:
content_block_ids = list(df.loc[0, "block_line_counts"].keys())

# Title block IDs for the first article:
title_block_ids = list(df.loc[0, "title_block_line_counts"].keys())
```

## Acknowledgements

The Papers Past article extraction code is adapted from the work of [Joshua Wilson Black](https://github.com/JoshuaWilsonBlack/newspaper-philosophy-methods)

Wilson Black, J. (2023). Creating specialized corpora from digitized historical newspaper archives: An iterative bootstrapping approach. *Digital Scholarship in the Humanities, 38*(2), 779–797. [https://doi.org/10.1093/llc/fqac079](https://doi.org/10.1093/llc/fqac079)
