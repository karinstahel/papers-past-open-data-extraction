# Papers Past Open Data METS/ALTO extraction script

This Python script uses multiprocessing to effciently extract article data from METS/ALTO XML (in tar.gz files) in the [National Library of New Zealand's Papers Past open data](https://natlib.govt.nz/about-us/open-data/papers-past-metadata/papers-past-newspaper-open-data-pilot/overview-papers-past-newspaper-open-data-pilot). The script processes the archive files and saves extracted article data by newspaper issue in parquet format with detailed error and completion logging.

## Features

- Extracts newspaper article content and layout related information from METS/ALTO XML files
- Processes multiple issues in parallel using multiprocessing
- Provides detailed logging and statistics about the extraction process
- Supports various input options (specific issues, newspaper codes, etc.)
- Outputs data in parquet format

## Usage

Run this script from the command line using Python. The tool offers multiple ways to specify which newspaper issues to process, including individual issue codes, lists in text files, or newspaper-year combinations.

### Basic Usage

```bash
python multiprocess_pp_issues_mets_alto_full.py --input /path/to/data --output /path/to/output --date 20250329
```

### Usage Examples

#### Process all issues in input directories

```bash
python multiprocess_pp_issues_mets_alto_full.py --input /data/papers_past --output /results --date 20250329
```

#### Process specific issues by code

```bash
python multiprocess_pp_issues_mets_alto_full.py --input /data/papers_past --output /results --date 20250329 --issues DSC_18471002 TC_18580910
```

#### Process issues listed in a file

```bash
python multiprocess_pp_issues_mets_alto_full.py --input /data/papers_past --output /results --date 20250329 --issue-file issues.txt
```

Where `issues.txt` contains one issue code per line:
```
DSC_18471002
TC_18580910
NENZC_18571024
```

#### Process specific newspaper-year combinations

```bash
python multiprocess_pp_issues_mets_alto_full.py --input /data/papers_past --output /results --date 20250329 --newspaper-codes DSC_1847 NENZC_1857
```

#### Process newspaper-year combinations listed in a file

```bash
python multiprocess_pp_issues_mets_alto_full.py --input /data/papers_past --output /results --date 20250329 --newspaper-year-file newspaper_years.txt
```

Where `newspaper_years.txt` contains one newspaper-year code per line:
```
DSC_1847
TC_1858
NENZC_1857
```

#### Specify number of worker processes

```bash
python multiprocess_pp_issues_mets_alto_full.py --input /data/papers_past --output /results --date 20250329 --workers 8
```

### Command Line Arguments

| Argument | Description |
|----------|-------------|
| `--input` | One or more input directories containing tar.gz files (required) |
| `--output` | Output directory for processed files (required) |
| `--date` | Revision date for output files (e.g., '20250329') (required) |
| `--workers` | Maximum number of parallel workers (default: automatic) |
| `--issue-file` | File containing list of issue codes to process |
| `--issues` | Space-separated list of issue codes to process |
| `--newspaper-year-file` | File containing list of newspaper_year codes to process |
| `--newspaper-codes` | Space-separated list of newspaper codes to process |

## Output Structure

The script generates output in the following structure:

```
output_directory/
├── pp_issue_mets_alto_dfs/
│   ├── PP_NEWSPAPER_DATE_REVDATE.parquet
│   └── ...
└── pp_issue_processing_summaries/
    └── summary_YYYYMMDD_HHMMSS.json
```

Each parquet file contains extracted article data for a single newspaper issue, and the summary JSON file contains statistics and details about the processing run.

## Acknowledgements

This code is adapted from code originally developed by [Joshua Wilson Black](https://github.com/JoshuaWilsonBlack/newspaper-philosophy-methods)

Wilson Black, J. (2023). Creating specialized corpora from digitized historical newspaper archives: An iterative bootstrapping approach. *Digital Scholarship in the Humanities, 38*(2), 779–797. [https://doi.org/10.1093/llc/fqac079](https://doi.org/10.1093/llc/fqac079)
