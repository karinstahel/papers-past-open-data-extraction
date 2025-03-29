# Papers Past Open Data METS/ALTO extraction script

This Python script extracts article data from METS/ALTO files in Papers Past open data tar.gz files using multiprocessing. The tool processes newspaper archives and saves extracted article data by newspaper issue in parquet format with detailed error and completion logging.

## Features

- Extracts newspaper article content and metadata from METS/ALTO XML files
- Preserves article structure and layout information
- Handles complex text normalization including hyphenation and special characters
- Processes multiple issues in parallel using multiprocessing
- Provides detailed logging and statistics about the extraction process
- Supports various input options (specific issues, newspaper codes, etc.)
- Outputs data in parquet format for efficient storage and analysis

## Dependencies

```
lxml
pandas
tqdm
```

## Installation

1. Clone this repository
2. Install the required dependencies:
   ```
   pip install lxml pandas tqdm
   ```

## Usage

The script offers multiple ways to specify which newspaper issues to process.

### Basic Usage

```bash
python mets_alto_extraction.py --input /path/to/data --output /path/to/output --date 20250329
```

### Usage Examples

#### Process all issues in input directories

```bash
python mets_alto_extraction.py --input /data/papers_past --output /results --date 20250329
```

#### Process specific issues by code

```bash
python mets_alto_extraction.py --input /data/papers_past --output /results --date 20250329 --issues CHP_19031228 NZFL_19040101
```

#### Process issues listed in a file

```bash
python mets_alto_extraction.py --input /data/papers_past --output /results --date 20250329 --issue-file issues.txt
```

Where `issues.txt` contains one issue code per line:
```
CHP_19031228
NZFL_19040101
AGS_19000203
```

#### Process specific newspaper-year combinations

```bash
python mets_alto_extraction.py --input /data/papers_past --output /results --date 20250329 --newspaper-codes CHP_1903 NZFL_1904
```

#### Process newspaper-year combinations listed in a file

```bash
python mets_alto_extraction.py --input /data/papers_past --output /results --date 20250329 --newspaper-year-file newspaper_years.txt
```

Where `newspaper_years.txt` contains one newspaper-year code per line:
```
CHP_1903
NZFL_1904
AGS_1900
```

#### Specify number of worker processes

```bash
python mets_alto_extraction.py --input /data/papers_past --output /results --date 20250329 --workers 8
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

This code is adapted from work by [Joshua Wilson Black](https://github.com/JoshuaWilsonBlack/newspaper-philosophy-methods)

Wilson Black, J. (2023). Creating specialized corpora from digitized historical newspaper archives: An iterative bootstrapping approach. *Digital Scholarship in the Humanities, 38*(2), 779–797. [https://doi.org/10.1093/llc/fqac079](https://doi.org/10.1093/llc/fqac079)



## Author

Karin Stahel, with assistance from Claude 3.7 Sonnet for troubleshooting and code refinement.
