#! /usr/bin/env python3
"""
A script that uses multiprocessing to extract article data
from METS/ALTO files in Papers Past open data tar.gz files.

The data is saved by newspaper issue in a pandas dataframe in
parquet format with detailed error and completion logging in separate files.

Author: Karin Stahel
Claude 3.7 Sonnet was used to assist with aspects of
troubleshooting and code refinement.

Adapted from code created by Joshua Wilson Black (2023).
See https://doi.org/10.1093/llc/fqac079
"""
# %%
import os
import re
from functools import partial
import tarfile
import time
from multiprocessing import Pool, cpu_count
import logging
import json
import argparse
from lxml import etree as ET
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

# Check for parquet dependencies before proceeding
parquet_engine = check_parquet_dependencies()
logging.info(f"Using {parquet_engine} for parquet file operations")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("mets_alto_extraction.log"),
        logging.StreamHandler()
    ]
)

# Load METS namespace
# http://www.loc.gov/standards/mets/namespace.html
NS = {
    "mets": "http://www.loc.gov/METS/",
    # Code is ALTO namespace agnostic and will handle various implementations using local.name()
}

# %%
def clean_text(text):
    """
    Normalises and cleans text by handling special characters and standardising spacing.
    - Converts non-string inputs to strings
    - Replaces escape sequences with appropriate characters
    - Preserves consecutive hyphens and number-hyphen patterns
    - Removing redundant spaces
    - Standardises spacing around hyphens

    Args:
        text: The input text to clean. Can be a string or any other type that will be
        converted to a string (None becomes an empty string).

    Returns:
        str: The normalised text with standardised spacing and preserved
        hyphenation patterns.
    """
    if not isinstance(text, str):
        return "" if text is None else str(text)

    # Replace escape sequences using regular expression
    text = re.sub(r'\\([\'"\n\r\t])',
                 lambda m: ' ' if m.group(1) in 'nrt' else m.group(1),
                 text)

    # Protect consecutive hyphens by marking them
    text = re.sub(r'(\-{2,})', lambda m: f'__HYPHEN_{len(m.group(1))}__', text)
    # Protect number-hyphen sequences
    text = re.sub(r'(\d+\'?)(\s+)(\-+)', r'\1__NUMHYPH__\3', text)
    text = " ".join(text.split())
    # Restore consecutive hyphens
    text = re.sub(r'__HYPHEN_(\d+)__', lambda m: '-' * int(m.group(1)), text)
    # Restore number-hyphen connections
    text = re.sub(r'__NUMHYPH__', '-', text)
    # Fix spacing around multiple consecutive hyphens
    text = re.sub(r'(\d+\'?)-(\-+)', r'\1-\2', text)

    return text

# %%
def mets2codes_inner(text, issue_code):
    """
    Given METS file as text string, return a dictionary of articles with
    separate title and content blocks. Also captures presence of non-text elements
    such as TABLE, IMAGE, etc. but excludes content from ILLUSTRATION, IMAGE, CAPTION.

    Args:
        text: XML content as bytes
        issue_code: Issue code identifier

    Returns:
        Dictionary mapping article IDs to (title, title_block_ids, text_block_ids,
        non_text_elements, order_map) tuples where order_map provides the
        ordering information for text blocks
    """
    # Load the mets xml file
    parser = ET.XMLParser(remove_blank_text=True, recover=True)

    try:
        mets_root = ET.fromstring(text, parser)
    except ET.XMLSyntaxError as e:
        logging.error(f"XML parsing error for {issue_code}: {str(e)}")
        return {}

    xpath_articles = ".//mets:div[@TYPE='ARTICLE']"
    articles_div = mets_root.xpath(xpath_articles, namespaces=NS)

    art_dict = {}

    # Define types to track their presence but exclude content
    exclude_content_types = {"ILLUSTRATION", "IMAGE", "CAPTION"}

    for article in articles_div:
        attributes = article.attrib
        article_id = attributes.get("DMDID", "")
        if not article_id:
            continue  # Skip articles without an ID

        article_title = attributes.get("LABEL", "UNTITLED")
        issue_article_id = f"{issue_code}_{article_id[7:]}" if len(article_id) > 7 else f"{issue_code}_{article_id}"

        # Extract heading blocks (as before)
        title_block_ids = []
        title_divs = article.xpath(".//mets:div[@TYPE='HEADING']", namespaces=NS)
        for title_div in title_divs:
            for area in title_div.xpath(".//mets:area", namespaces=NS):
                block_id = area.get("BEGIN")
                if block_id and block_id not in title_block_ids:
                    title_block_ids.append(block_id)

        # Extract text blocks - both regular TEXT blocks and others like AUTHOR
        all_content_divs = []  # Contains (div, block_id, order_value)

        # Process all divs that might contain content (everything except HEADING)
        for div in article.xpath(".//mets:div[not(@TYPE='HEADING')]", namespaces=NS):
            div_type = div.get("TYPE", "")
            if not div_type:
                continue

            # Skip content extraction for excluded types
            if div_type in exclude_content_types:
                continue

            # Get order value for sequencing
            order_str = div.get("ORDER", "999")  # Default high value if not specified
            try:
                order_val = int(order_str)
            except ValueError:
                order_val = 999

            # Process direct area elements in this div
            for area in div.xpath("./mets:area", namespaces = NS):
                block_id = area.get("BEGIN")
                if block_id:
                    all_content_divs.append((div, block_id, order_val))

        # Sort by ORDER value
        all_content_divs.sort(key=lambda x: x[2])

        # Create a list of text block IDs in the correct order
        text_block_ids = []
        # Create a dictionary mapping block_id to its position in the sequence
        order_map = {}

        for idx, (div, block_id, _) in enumerate(all_content_divs):
            if block_id not in text_block_ids:
                text_block_ids.append(block_id)
                order_map[block_id] = idx  # Store position for later sequencing

        # Extract non-text elements
        non_text_elements = []
        for child_div in article.xpath("./mets:div", namespaces = NS):
            div_type = child_div.get("TYPE", "")
            if div_type and div_type not in ["HEADING"]:
                if div_type == "BODY":
                    # For BODY, check if it has TEXT children or non-text children
                    body_text_divs = child_div.xpath(".//mets:div[@TYPE='TEXT']", namespaces = NS)
                    if not body_text_divs:
                        non_text_elements.append(div_type)
                else:
                    non_text_elements.append(div_type)

                # Also capture any sub-types within this div
                for sub_div in child_div.xpath(".//mets:div", namespaces = NS):
                    sub_type = sub_div.get("TYPE", "")
                    if sub_type and sub_type not in ["TEXT", "HEADING", "BODY_CONTENT"]:
                        non_text_elements.append(sub_type)

                        # If this is a TABLE, etc., ensure its block IDs are in text_block_ids
                        # But EXCLUDE content from specified types
                        if sub_type not in exclude_content_types:
                            for area in sub_div.xpath(".//mets:area", namespaces=NS):
                                block_id = area.get("BEGIN")
                                if block_id and block_id not in text_block_ids:
                                    text_block_ids.append(block_id)
                                    order_map[block_id] = len(order_map)  # Add at the end

        # Store with the additional order_map
        art_dict[issue_article_id] = (article_title, title_block_ids, text_block_ids, non_text_elements, order_map)

    # Free memory
    mets_root.clear()

    return art_dict

# %%
def parse_pages(pages_tarinfo, tar):
    """
    Given list of pages as tarinfo objects, return dictionary with
    page IDs as keys and tuples of (XML root, namespace dict) as values.

    Args:
        pages_tarinfo: List of TarInfo objects for page files
        tar: Open tarfile object

    Returns:
        Dictionary mapping page IDs to tuples of (parsed XML root, namespace dict)
    """
    page_info = {}

    for i, page in enumerate(pages_tarinfo):
        with tar.extractfile(page) as f:
            text = f.read()
        parser = ET.XMLParser(remove_blank_text=True, recover=True)
        try:
            root = ET.fromstring(text, parser)

            # Get the original namespace map from the root
            original_nsmap = root.nsmap.copy()

            # Create a clean namespace map for XPath (with no empty prefixes)
            xpath_nsmap = {}

            # Handle default namespace (xmlns without prefix)
            if None in original_nsmap:
                alto_ns = original_nsmap[None]
                # Add with 'alto' prefix for XPath queries
                xpath_nsmap["alto"] = alto_ns
                # Only log this at debug level
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    logging.debug(f"Found default ALTO namespace in {page.name}: {alto_ns}")
            else:
                # Look for any namespace with 'alto' in the URL
                alto_found = False
                for prefix, uri in original_nsmap.items():
                    if "alto" in uri.lower():
                        xpath_nsmap["alto"] = uri
                        alto_found = True
                        # Only log this at debug level
                        if logging.getLogger().isEnabledFor(logging.DEBUG):
                            logging.debug(f"Found ALTO namespace with prefix {prefix} in {page.name}: {uri}")
                        break

                    # Copy other namespaces as-is
                    xpath_nsmap[prefix] = uri

                # If no ALTO namespace found, create a dummy one to avoid XPath errors
                if not alto_found:
                    # Use debug level instead of warning as this is common and expected
                    logging.debug(f"No ALTO namespace found in {page.name}, using fallback")
                    xpath_nsmap["alto"] = "http://www.loc.gov/standards/alto/ns-generic"

            # For any other namespaces with real prefixes, copy them
            for prefix, uri in original_nsmap.items():
                if prefix is not None:
                    xpath_nsmap[prefix] = uri

            page_info[f"P{i+1}"] = (root, xpath_nsmap)
        except ET.XMLSyntaxError as e:
            logging.error(f"XML parsing error for page {i+1}: {str(e)}")
            continue

    return page_info

# %%
def process_text_block(block_strings):
    """
    Given XML string elements from text block, return whole block as single string
    and a list of word confidence values.
    Handles:
    1. Legitimate hyphenated words (using SUBS_CONTENT)
    2. Multiple consecutive hyphens (preserved)
    3. Numbers followed by hyphens in tabular data

    Args:
        block_strings: List of XML string elements

    Returns:
        Tuple of (concatenated text, list of word confidence values)
    """
    # Handle empty blocks
    if not block_strings:
        return "", []
    words = []
    word_confidences = []
    skip_next = False
    i = 0
    while i < len(block_strings):
        if skip_next:
            skip_next = False
            i += 1
            continue
        s = block_strings[i]
        content = s.get("CONTENT", "")
        subs_type = s.get("SUBS_TYPE", "")
        subs_content = s.get("SUBS_CONTENT", "")
        wc = s.get("WC", "0") # word confidence
        try:
            word_confidence = float(wc)
        except (ValueError, TypeError):
            word_confidence = 0.0

        # Case 1: Check if this is a string with multiple consecutive hyphens that should be preserved
        if content and re.match(r'^-{2,}$', content):
            # This is for consecutive hyphens - preserve it exactly
            words.append(content)
            word_confidences.append(word_confidence)
            i += 1
            continue
        # Case 2: Handle numeric data followed by hyphens in tabular contexts
        if re.match(r'^\d+\'?$', content) and subs_type == "HypPart1" and i + 1 < len(block_strings):
            next_s = block_strings[i + 1]
            next_content = next_s.get("CONTENT", "")
            next_subs_type = next_s.get("SUBS_TYPE", "")
            # If the next content is just hyphens or starts with hyphens, this is likely tabular data
            if (next_content and (re.match(r'^-+', next_content) or next_subs_type == "HypPart2"
                    and re.match(r'^-+', next_content))):
                # Preserve the number and add a hyphen
                words.append(content + "-")
                word_confidences.append(word_confidence)
                i += 1
                continue
        # Case 3: Handle genuine hyphenated words (like "com-puter" split across lines)
        if subs_type == "HypPart1" and i + 1 < len(block_strings):
            next_s = block_strings[i + 1]
            next_subs_type = next_s.get("SUBS_TYPE", "")
            next_content = next_s.get("CONTENT", "")
            if next_subs_type == "HypPart2":
                # Genuine hyphenated words typically don't have multiple hyphens in SUBS_CONTENT
                if (subs_content and
                    not re.search(r'-{2,}', subs_content) and
                    not (next_content and re.match(r'^-{2,}$', next_content))):
                    # Use SUBS_CONTENT
                    words.append(subs_content)
                    word_confidences.append(word_confidence)
                    skip_next = True
                    i += 1
                    continue
                else:
                    # Just add the current content and let the next loop handle the next part
                    words.append(content)
                    word_confidences.append(word_confidence)
                    i += 1
                    continue
        # Case 4: Normal content - use as-is
        words.append(content)
        word_confidences.append(word_confidence)
        i += 1
    # Join with spaces
    total_string = " ".join(words)
    return total_string, word_confidences

# %%
def process_block(block_id, page_info, block_type = "content"):
    """
    Process a single block regardless of namespace.

    Args:
        block_id: Block ID
        page_info: Dictionary of parsed ALTO XML roots and their namespaces
        block_type: Type of block ("title" or "content")

    Returns:
        Dictionary of extracted block data
    """
    match = re.match(r"P(\d+)_", block_id)
    if not match:
        return None

    page_no = f"P{match.group(1)}"
    if page_no not in page_info:
        return None

    page_root = page_info[page_no][0]

    alto_block_type = "TextBlock" if "TB" in block_id else "ComposedBlock" if "CB" in block_id else None
    if not alto_block_type:
        return None

    # Using local-name approach
    xml_blocks = page_root.xpath(f".//*[local-name()='{alto_block_type}' and @ID='{block_id}']")
    if not xml_blocks:
        return None
    xml_block = xml_blocks[0]

    block_strings = xml_block.xpath(".//*[local-name()='String']")
    block_lines = xml_block.xpath(".//*[local-name()='TextLine']")

    if block_type == "title":
        text_key = "title_block"
        confidence_key = "title_confidences"
        prefix = "title_"
        block_id_key = "processed_title_block_ids"
    else:
        text_key = "text_blocks"
        confidence_key = "word_confidences"
        prefix = ""
        block_id_key = "processed_text_block_ids"

    result = {
        text_key: [],
        confidence_key: [],
        f"{prefix}line_widths": [],
        f"{prefix}line_heights": [],
        f"{prefix}line_hpos": [],
        f"{prefix}line_vpos": [],
        f"{prefix}block_hpos": [],
        f"{prefix}block_vpos": [],
        f"{prefix}block_widths": [],
        f"{prefix}block_heights": [],
        block_id_key: [block_id],
    }

    if block_strings:
        text, confidences = process_text_block(block_strings)
        result[text_key].append(text)
        result[confidence_key].extend(confidences)

    # Process layout information and handle potential float values in attributes
    try:
        block_hpos = int(float(xml_block.get("HPOS", "0")))
        block_vpos = int(float(xml_block.get("VPOS", "0")))
        block_width = int(float(xml_block.get("WIDTH", "0")))
        block_height = int(float(xml_block.get("HEIGHT", "0")))
    except ValueError:
        # Fallback in case of invalid values
        logging.warning(f"Invalid dimension value for block {block_id}")
        block_hpos = block_vpos = block_width = block_height = 0

    result[f"{prefix}block_hpos"].append(block_hpos)
    result[f"{prefix}block_vpos"].append(block_vpos)
    result[f"{prefix}block_widths"].append(block_width)
    result[f"{prefix}block_heights"].append(block_height)

    for line in block_lines:
        try:
            line_hpos = int(float(line.get("HPOS", "0")))
            line_vpos = int(float(line.get("VPOS", "0")))
            line_width = int(float(line.get("WIDTH", "0")))
            line_height = int(float(line.get("HEIGHT", "0")))
        except ValueError:
            # Fallback in case of invalid values
            logging.warning(f"Invalid dimension value for line in block {block_id}")
            line_hpos = line_vpos = line_width = line_height = 0

        result[f"{prefix}line_hpos"].append(line_hpos)
        result[f"{prefix}line_vpos"].append(line_vpos)
        result[f"{prefix}line_widths"].append(line_width)
        result[f"{prefix}line_heights"].append(line_height)

    return result

# %%
def extract_text_from_alto(article_codes, page_info, issue_code):
    """
    Extract text and layout information for each article with accurate title identification.
    Maintains proper order of text blocks.

    Args:
        article_codes: Dictionary of articles from METS file with structure:
                      {article_id: (mets_title,
                                    title_block_ids,
                                    text_block_ids,
                                    non_text_elements,
                                    order_map)}
        page_info: Dictionary of parsed ALTO XML roots and their namespaces
        issue_code: Issue code identifier

    Returns:
        Dictionary of articles with extracted text and layout info
    """
    texts_dict = {}
    skipped_articles = 0

    for article_id, data in article_codes.items():
        # Unpack data - including order_map
        if len(data) == 5:
            mets_title, title_block_ids, text_block_ids, non_text_elements, order_map = data
        else:
            mets_title, title_block_ids, text_block_ids, non_text_elements = data
            order_map = {}  # Empty order map

        # Log articles that don't have any blocks
        if len(title_block_ids) == 0 and len(text_block_ids) == 0:
            logging.warning(f"Skipping article '{article_id}' with title '{mets_title}' - No text or title blocks found")
            skipped_articles += 1
            continue

        title_data = process_title_blocks(title_block_ids, page_info, issue_code)
        content_data = process_content_blocks(text_block_ids, page_info, issue_code, order_map)
        texts_dict[article_id] = combine_article_data(mets_title,
                                                      title_data,
                                                      content_data,
                                                      non_text_elements)

    if skipped_articles > 0:
        logging.info(f"Issue {issue_code}: Extracted {len(texts_dict)} articles, skipped {skipped_articles} articles")

    return texts_dict

# %%
def process_title_blocks(title_block_ids, page_info, issue_code):
    """
    Process title blocks and extract relevant data.

    Args:
        title_block_ids: List of title block IDs
        page_info: Dictionary of parsed ALTO XML roots and their namespaces
        issue_code: Issue code identifier

    Returns:
        Dictionary containing processed title data
    """
    title_data = {
        "title_block": [],
        "title_confidences": [],
        "title_line_widths": [],
        "title_line_heights": [],
        "title_line_hpos": [],
        "title_line_vpos": [],
        "title_block_hpos": [],
        "title_block_vpos": [],
        "title_block_widths": [],
        "title_block_heights": [],
        "processed_title_block_ids": [],
    }

    for block_id in title_block_ids:
        block_data = process_block(block_id, page_info, block_type = "title")
        if block_data:
            for key, value in block_data.items():
                if isinstance(value, list):
                    title_data[key].extend(value)
                else:
                    title_data[key] = value

    return title_data

# %%

def process_content_blocks(text_block_ids, page_info, issue_code, order_map = None):
    """
    Process content blocks and extract relevant data.
    Preserves the correct order of blocks.

    Args:
        text_block_ids: List of content block IDs
        page_info: Dictionary of parsed ALTO XML roots and their namespaces
        issue_code: Issue code identifier
        order_map: Dictionary mapping block_ids to their sequence position

    Returns:
        Dictionary containing processed content data
    """
    # Initialise arrays for content data
    content_data = {
        "text_blocks": [],
        "word_confidences": [],
        "line_widths": [],
        "line_heights": [],
        "line_hpos": [],
        "line_vpos": [],
        "block_hpos": [],
        "block_vpos": [],
        "block_widths": [],
        "block_heights": [],
        "processed_text_block_ids": [],
        "block_order_positions": [],  # Store position for each block
    }

    blocks_data = []  # Will store (block_id, block_data, order_position)

    for block_id in text_block_ids:
        block_data = process_block(block_id, page_info, block_type="content")
        if block_data:
            # Get position from order_map or assign a high value
            position = order_map.get(block_id, 999) if order_map else 999
            blocks_data.append((block_id, block_data, position))

    # Sort blocks by position
    blocks_data.sort(key=lambda x: x[2])

    # Process blocks in correct order
    for block_id, block_data, position in blocks_data:
        content_data["text_blocks"].extend(block_data.get("text_blocks", []))
        content_data["word_confidences"].extend(block_data.get("word_confidences", []))
        content_data["processed_text_block_ids"].extend(block_data.get("processed_text_block_ids", []))
        content_data["block_order_positions"].extend([position] * len(block_data.get("text_blocks", [])))
        content_data["line_widths"].extend(block_data.get("line_widths", []))
        content_data["line_heights"].extend(block_data.get("line_heights", []))
        content_data["line_hpos"].extend(block_data.get("line_hpos", []))
        content_data["line_vpos"].extend(block_data.get("line_vpos", []))
        content_data["block_hpos"].extend(block_data.get("block_hpos", []))
        content_data["block_vpos"].extend(block_data.get("block_vpos", []))
        content_data["block_widths"].extend(block_data.get("block_widths", []))
        content_data["block_heights"].extend(block_data.get("block_heights", []))

    return content_data

# %%
def combine_article_data(mets_title, title_data, content_data, non_text_elements):
    """
    Combine title and content data into a single article dictionary.

    Args:
        mets_title: Original METS title
        title_data: Processed title data
        content_data: Processed content data
        non_text_elements: List of non-text element types (TABLE, IMAGE, etc.)

    Returns:
        Combined article data
    """
    title_text = " ".join(title_data["title_block"]) if title_data["title_block"] else mets_title
    full_text = " ".join(content_data["text_blocks"])
    title_text = clean_text(title_text)
    full_text = clean_text(full_text)

    return (
        mets_title, # Original METS title
        title_text, # Title text from ALTO (just in case it's different)
        full_text,
        title_data["title_line_widths"],
        title_data["title_line_heights"],
        title_data["title_line_hpos"],
        title_data["title_line_vpos"],
        title_data["title_block_hpos"],
        title_data["title_block_vpos"],
        title_data["title_block_widths"],
        title_data["title_block_heights"],
        title_data["title_confidences"],
        title_data["processed_title_block_ids"],
        content_data["line_widths"],
        content_data["line_heights"],
        content_data["line_hpos"],
        content_data["line_vpos"],
        content_data["block_hpos"],
        content_data["block_vpos"],
        content_data["block_widths"],
        content_data["block_heights"],
        content_data["word_confidences"],
        content_data["processed_text_block_ids"],
        non_text_elements,
    )

# %%
def get_mets_and_alto_files(tar, dir_prefix):
    """
    Get METS and ALTO files from a tar archive.

    Args:
        tar: Open tarfile object
        dir_prefix: Directory prefix within the tar file

    Returns:
        Tuple of (mets_file, page_files)
    """
    all_members = tar.getmembers()
    mets_file = None
    page_files = []

    for member in all_members:
        if member.name.startswith(dir_prefix) and member.name.endswith(".xml"):
            if "mets.xml" in member.name:
                mets_file = member
            elif re.search(r"/\d+\.xml$", member.name):
                page_files.append(member)

    # Sort page files by number
    page_files.sort(key = lambda x: int(re.search(r"/(\d+)\.xml$", x.name).group(1)))

    return mets_file, page_files

# %%
def process_issue(args, input_paths, output_path, rev_date):
    """
    Process by single issue - extracting article info from METS and text from ALTO files.

    Args:
        args: Tuple of (issue_code, mets_path)
        input_paths: List of paths to the input directories
        output_path: Path to the output directory
        rev_date: Revision date for output files

    Returns:
        Tuple of (issue_code, success_flag, number_of_articles, number_of_pages, number_of_skipped_articles)
    """
    issue_code, tar_path_internal = args
    start_time = time.time()

    try:
        parts = tar_path_internal.split(".tar.gz")
        tar_path = parts[0] + ".tar.gz"
        dir_prefix = parts[1][1:].rsplit("/", 1)[0]

        tar_full_path = None
        for input_path in input_paths:
            potential_path = os.path.join(input_path, tar_path)
            if os.path.exists(potential_path):
                tar_full_path = potential_path
                break

        if tar_full_path is None:
            logging.error(f"Tar file {tar_path} not found in any input paths")
            return issue_code, False, 0, 0, 0

        with tarfile.open(tar_full_path) as tar:
            try:
                mets_file, page_files = get_mets_and_alto_files(tar, dir_prefix)
                if mets_file is None:
                    logging.error(f"METS file not found for {issue_code}")
                    return issue_code, False, 0, 0, 0
                if len(page_files) == 0:
                    logging.error(f"ALTO files not found for {issue_code}")
                    return issue_code, False, 0, 0, 0

                mets_text = tar.extractfile(mets_file).read()
                article_codes = mets2codes_inner(mets_text, issue_code)

                if len(article_codes) == 0:
                    logging.warning(f"No articles found in METS file for {issue_code}")
                    return issue_code, False, 0, 0, 0

                total_articles_in_mets = len(article_codes)
                logging.info(f"Found {total_articles_in_mets} articles in METS file for {issue_code}")

                for article_id, article_data in article_codes.items():
                    mets_title = article_data[0]  # First element is the title
                    logging.debug(f"Article in METS: {article_id}, Title: '{mets_title}'")

                page_info = parse_pages(page_files, tar)

                if len(page_info) == 0:
                    logging.error(f"Failed to parse ALTO files for {issue_code}")
                    return issue_code, False, 0, 0, 0

                articles_with_text = extract_text_from_alto(article_codes, page_info, issue_code)
                skipped_articles = total_articles_in_mets - len(articles_with_text)

                if skipped_articles > 0:
                    logging.warning(f"Issue {issue_code}: {skipped_articles} out of {total_articles_in_mets} articles were skipped")

                df = pd.DataFrame.from_dict(
                    articles_with_text,
                    orient = "index",
                    columns = [
                        "mets_title",           # Original METS title
                        "title_text",           # Title text from ALTO
                        "text",                 # Content text from ALTO
                        "title_line_widths",    # Width of each line in title
                        "title_line_heights",   # Height of each line in title
                        "title_line_hpos",      # HPOS of each line in title
                        "title_line_vpos",      # VPOS of each line in title
                        "title_block_hpos",     # HPOS of each block in title
                        "title_block_vpos",     # VPOS of each block in title
                        "title_block_widths",   # Width of each block in title
                        "title_block_heights",  # Height of each block in title
                        "title_confidences",    # Word confidences in title
                        "title_block_ids",      # Block IDs for title
                        "line_widths",          # Width of each line in content
                        "line_heights",         # Height of each line in content
                        "line_hpos",            # HPOS of each line in content
                        "line_vpos",            # VPOS of each line in content
                        "block_hpos",           # HPOS of each block in content
                        "block_vpos",           # VPOS of each block in content
                        "block_widths",         # Width of each block in content
                        "block_heights",        # Height of each block in content
                        "word_confidences",     # Word confidences in content
                        "block_ids",            # Block IDs for content
                        "non_text_elements",    # List of each non-text element found (including duplicates)
                    ]
                )

                output_file_path = os.path.join(
                    output_path,
                    "pp_issue_mets_alto_dfs",
                    f"PP_{issue_code}_{rev_date}.parquet"
                )

                df.to_parquet(output_file_path, engine = parquet_engine)

                # Clear memory
                for root, _ in page_info.values():
                    root.clear()

                elapsed = time.time() - start_time
                logging.info(f"Processed {issue_code} with {len(df)} articles in {elapsed:.2f} seconds")

                return issue_code, True, len(df), len(page_files), skipped_articles

            except Exception as e:
                logging.error(f"Error processing tar content for {issue_code}: {str(e)}")
                return issue_code, False, 0, 0, 0

    except Exception as e:
        logging.error(f"Error processing {issue_code}: {str(e)}")

        return issue_code, False, 0, 0, 0

# %%
def batch_process_issues(issues, max_workers, input_paths, output_path, rev_date):
    """
    Process multiple issues in parallel.

    Args:
        issues: Dictionary mapping issue codes to METS file paths
        max_workers: Maximum number of parallel workers
        input_paths: List of one or more paths to the input directories
        output_path: Path to the output directory
        rev_date: Revision date for output files

    Returns:
        Tuple of (successful_issues, failed_issues, statistics)
    """
    # Determine optimal worker count if not specified
    if max_workers is None:
        max_workers = min(cpu_count() - 1, len(issues))  # Leave one core free for system processes

    print(f"Starting parallel processing with {max_workers} workers for {len(issues)} issues")

    issue_items = list(issues.items())

    process_issue_partial = partial(process_issue,
                                    input_paths = input_paths,
                                    output_path = output_path,
                                    rev_date = rev_date)
    results = []

    with Pool(processes=max_workers) as pool:
        results = list(tqdm(
            pool.imap(process_issue_partial, issue_items),
            total = len(issue_items),
            desc = "Processing issues"
        ))

    successful = [r[0] for r in results if r[1]]
    failed = [r[0] for r in results if not r[1]]

    stats = {r[0]: {"articles": r[2],
                    "pages": r[3] if len(r) > 3 else 0,
                    "skipped_articles": r[4] if len(r) > 4 else 0} for r in results if r[1]}

    print(f"Completed processing {len(successful)} issues successfully, {len(failed)} failed")

    if failed:
        print(f"Failed issues: {', '.join(failed)}")

    return successful, failed, stats

# %%
def discover_and_process_issues(input_paths, newspaper_year_codes = None):
    """
    Discover specified tar.gz files in the input directories and extract issue codes
    from their structure for processing.

    Args:
        input_paths: List of paths to the directories containing tar.gz files
        newspaper_year_codes: List of newspaper_year codes to process (optional)

    Returns:
        Dictionary mapping issue codes to their paths
    """
    issues = {}
    for input_path in input_paths:
        tar_files = [f for f in os.listdir(input_path) if f.endswith(".tar.gz")]

        for tar_file in tar_files:
            tar_path = os.path.join(input_path, tar_file)
            newspaper_year_match = re.match(r"([A-Z]+)_(\d{4})\.tar\.gz", tar_file)

            if not newspaper_year_match:
                logging.warning(f"Skipping file with unexpected format: {tar_file}")
                continue

            newspaper = newspaper_year_match.group(1)
            year = newspaper_year_match.group(2)
            newspaper_year_code = f"{newspaper}_{year}"

            if newspaper_year_codes and newspaper_year_code not in newspaper_year_codes:
                continue

            try:
                with tarfile.open(tar_path) as tar:
                    mets_files = [m for m in tar.getmembers() if m.name.endswith("mets.xml")]

                    for mets_file in mets_files:
                        # Extract issue code from path
                        # Path format: newspaper/year/newspaper_date/MM_01/mets.xml
                        path_parts = mets_file.name.split("/")
                        if len(path_parts) >= 4:
                            issue_dir = path_parts[-3]  # Get the newspaper_date directory
                            issue_match = re.match(fr"{newspaper}_(\d+)", issue_dir)

                            if issue_match:
                                date_str = issue_match.group(1)
                                issue_code = f"{newspaper}_{date_str}"
                                internal_path = f"{newspaper}/{year}/{issue_dir}/MM_01/mets.xml"
                                issues[issue_code] = f"{tar_file}/{internal_path}"
            except Exception as e:
                logging.error(f"Problem with tar file {tar_file}: {str(e)}")

    logging.info(f"Discovered {len(issues)} issues across {len(tar_files)} tar files")

    return issues

# %%
def construct_issue_path(issue_code):
    """
    Construct the full path to a METS file given just the issue code.

    Args:
        issue_code: Issue code (e.g., "CHP_19031228")

    Returns:
        Full path to the METS file
    """
    parts = issue_code.split("_")

    if len(parts) != 2:
        raise ValueError(f"Invalid issue code format: {issue_code}. Expected format: TITLE_YYYYMMDD")

    newspaper_title = parts[0]
    date_str = parts[1]

    # Extract year
    if len(date_str) < 4:
        raise ValueError(f"Invalid date in issue code: {date_str}")
    year = date_str[:4]

    tar_path = f"{newspaper_title}_{year}.tar.gz"
    internal_path = f"{newspaper_title}/{year}/{issue_code}/MM_01/mets.xml"

    return f"{tar_path}/{internal_path}"

# %%
def parse_arguments():
    """Parse the command line arguments"""
    parser = argparse.ArgumentParser(description = "Papers Past Open Data METS-ALTO Extraction Script")

    parser.add_argument("--input", dest = "input_paths", nargs = "+", required = True,
                        help = "Input directories containing tar.gz files")

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

    parser.add_argument("--newspaper-codes", dest="newspaper_codes", nargs="+", default=[],
                        help="Space-separated list of newspaper codes to process (optional)")

    args = parser.parse_args()

    # If no date provided, use current date
    if not args.rev_date:
        args.rev_date = time.strftime("%Y%m%d")
        print(f"No date specified, using current date: {args.rev_date}")

    return args

# %%
def main():
    """Main function to run the Papers Past Open Data METS-ALTO extraction process."""

    args = parse_arguments()

    input_paths = args.input_paths
    output_path = args.output_path
    rev_date = args.rev_date

    os.makedirs(os.path.join(output_path, "pp_issue_mets_alto_dfs"), exist_ok=True)
    os.makedirs(os.path.join(output_path, "pp_issue_processing_summaries"), exist_ok=True)

    issues = {}

    # Option 1: Process specific issues from command line
    if args.issues:
        for issue_code in args.issues:
            try:
                issues[issue_code] = construct_issue_path(issue_code)
            except ValueError as e:
                logging.error(f"Skipping issue {issue_code}: {str(e)}")

    # Option 2: Process issues from a file
    elif args.issue_file:
        with open(args.issue_file, 'r') as f:
            issue_codes = [line.strip() for line in f if line.strip()]
            for issue_code in issue_codes:
                try:
                    issues[issue_code] = construct_issue_path(issue_code)
                except ValueError as e:
                    logging.error(f"Skipping issue {issue_code}: {str(e)}")

    # Option 3: Process specified newspaper_year codes in input paths
    elif args.newspaper_year_file:
        with open(args.newspaper_year_file, 'r') as f:
            newspaper_year_codes = [line.strip() for line in f if line.strip()]
        issues = discover_and_process_issues(input_paths, newspaper_year_codes)

    # Option 4: Process specified newspaper codes from command line
    elif args.newspaper_codes:
        newspaper_year_codes = args.newspaper_codes
        issues = discover_and_process_issues(input_paths, newspaper_year_codes)

    # Option 5: Process all issues in input paths
    else:
        issues = discover_and_process_issues(input_paths)

    print(f"Loaded {len(issues)} issues to process")

    # Process the issues
    start_time = time.time()
    successful, failed, stats = batch_process_issues(issues,
                                                     args.max_workers,
                                                     input_paths,
                                                     output_path,
                                                     rev_date)
    elapsed = time.time() - start_time

    issues_with_skipped_articles = [
        {
            "issue_code": issue_code,
            "extracted_articles": stats[issue_code]["articles"],
            "skipped_articles": stats[issue_code]["skipped_articles"]
        }
        for issue_code in stats
        if stats[issue_code]["skipped_articles"] > 0
    ]

    summary = {
        "total_issues": len(issues),
        "successful": len(successful),
        "failed": len(failed),
        "elapsed_seconds": elapsed,
        "failed_issues": failed,
        "successful_issues": successful,
        "stats": stats,
        "issues_with_skipped_articles": issues_with_skipped_articles
    }

    summary_path = os.path.join(
        output_path,
        "pp_issue_processing_summaries",
        f"summary_{time.strftime('%Y%m%d_%H%M%S')}.json"
    )

    with open(summary_path, "w", encoding = "utf-8") as f:
        json.dump(summary, f, indent=2)

    # Statistics and problems
    total_articles = sum(stat["articles"] for stat in stats.values())
    total_pages = sum(stat["pages"] for stat in stats.values())
    total_skipped_articles = sum(stat["skipped_articles"] for stat in stats.values())

    print(f"Processed {len(successful)} issues successfully in {elapsed:.2f} seconds")
    print(f"Total articles extracted: {total_articles}")
    print(f"Total articles skipped: {total_skipped_articles}")
    print(f"Total pages processed: {total_pages}")
    print(f"Average articles per issue: {total_articles / len(successful) if successful else 0:.2f}")
    print(f"Average processing time per issue: {elapsed / len(issues):.2f} seconds")

    if total_skipped_articles > 0:
        print(f"\nWARNING: {total_skipped_articles} articles were skipped during extraction")
        print(f"See log file and summary JSON for details")

    print(f"Summary saved to {summary_path}")
    print(f"Finished processing! 😊")

if __name__ == "__main__":
    main()
