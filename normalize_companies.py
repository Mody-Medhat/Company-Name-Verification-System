# -*- coding: utf-8 -*-
"""Normalize company names and create representative batches."""

import re
import unicodedata
import math
import os
from multiprocessing import Pool, cpu_count
from collections import defaultdict, Counter
import difflib
import pandas as pd

stopwords = set(["ltd", "limited", "inc", "llc", "corp", "corporation"])
abbreviations = {
    "co.": "company",
    "intl": "international",
    "int'l": "international",
    "ind.": "industry",
    "tech.": "technology",
    "elec.": "electronic",
}
remove_prefixes = ["guangzhou"]
fuzzy_threshold = 0.9

input_URL = "https://drive.google.com/file/d/1g5-2W6SgD3n9S03qEucTU3gf_Mvv5A84/view?usp=sharing"
input_path = "https://drive.google.com/uc?export=download&id=" + input_URL.split("/")[-2]
output_directory = "./enrichment_artifacts"
batch_directory = os.path.join(output_directory, "batches")
target_batch_size = 2000
is_dry_run = False
row_limit = None

try:
    from unidecode import unidecode as transliterate
except ImportError:
    def transliterate(text):
        return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")

def clean_unicode(text: str) -> str:
    """Clean and normalize unicode text."""
    if not isinstance(text, str):
        return ""
    text = text.strip()
    text = unicodedata.normalize("NFKC", text)
    try:
        text = transliterate(text)
    except Exception:
        pass
    return text

def perform_basic_cleaning(text: str) -> str:
    """Remove punctuation, keep words intact."""
    text = re.sub(r"[&@/\\]+", " and ", text)
    text = re.sub(r"[^A-Za-z0-9\s\-]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def apply_light_normalization(raw_name: str) -> str:
    """Light cleanup for downstream matching."""
    raw_name = "" if raw_name is None else str(raw_name)
    cleaned = clean_unicode(raw_name).lower()

    # Expand abbreviations
    for abbr_key, abbr_value in abbreviations.items():
        abbr_key_lower = abbr_key.lower()
        abbr_value_lower = abbr_value.lower()
        cleaned = re.sub(r"\b" + re.escape(abbr_key_lower) + r"\b", abbr_value_lower, cleaned)

    # Remove prefixes if configured
    for prefix in remove_prefixes:
        prefix_lower = prefix.lower()
        if cleaned.startswith(prefix_lower + " "):
            cleaned = cleaned[len(prefix_lower) + 1:].strip()
            break

    cleaned = perform_basic_cleaning(cleaned)
    return cleaned

def apply_heavy_normalization(raw_name: str) -> str:
    """Aggressive cleanup for clustering."""
    light_normalized = apply_light_normalization(raw_name)
    tokens = [
        token
        for token in re.split(r"\s+", light_normalized)
        if token and token not in stopwords
    ]
    tokens = list(set(tokens))  # Remove duplicates
    return " ".join(tokens)

def process_data_chunk(chunk: pd.DataFrame, column_name: str):
    """Process a chunk of data for normalization."""
    records = []
    for _, row in chunk.iterrows():
        raw_name = row[column_name]
        light_normalized = apply_light_normalization(raw_name)
        heavy_normalized = apply_heavy_normalization(raw_name)
        records.append({
            "raw_name": raw_name,
            "normalized_light": light_normalized,
            "normalized_heavy": heavy_normalized,
        })
    return records

def main():
    """Run normalization pipeline."""
    print("Starting company name normalization...")
    
    # Read input data
    try:
        input_dataframe = pd.read_csv(
            input_path, dtype=str, keep_default_na=False, encoding="utf-8"
        )
    except Exception:
        try:
            input_dataframe = pd.read_csv(
                input_path, dtype=str, keep_default_na=False, encoding="latin1"
            )
        except Exception as e:
            print(f"Error reading input file: {e}")
            return

    name_column = input_dataframe.columns[0]
    if name_column not in input_dataframe.columns:
        print(f"Error: Column '{name_column}' not found in CSV.")
        return

    if row_limit:
        input_dataframe = input_dataframe.head(row_limit)

    original_shape = input_dataframe.shape
    print(f"Input shape: {original_shape} (rows, cols) | Using column '{name_column}'")

    # Step 1: Normalize in parallel
    print("Normalizing company names...")
    number_of_cores = cpu_count()
    chunk_size = math.ceil(len(input_dataframe) / number_of_cores)
    chunks = [
        input_dataframe.iloc[
            i * chunk_size : min((i + 1) * chunk_size, len(input_dataframe))
        ]
        for i in range(number_of_cores)
    ]
    
    with Pool(number_of_cores) as pool:
        all_records = pool.starmap(
            process_data_chunk, [(chunk, name_column) for chunk in chunks if not chunk.empty]
        )
    
    records = [record for chunk_records in all_records for record in chunk_records]
    normalized_dataframe = pd.DataFrame(records)

    # Step 2: Clean and deduplicate
    print("Cleaning and deduplicating...")
    rows_before_cleaning = normalized_dataframe.shape[0]
    normalized_dataframe = normalized_dataframe[
        normalized_dataframe["normalized_light"].str.strip() != ""
    ].copy()
    normalized_dataframe = normalized_dataframe.drop_duplicates(
        subset=["normalized_light"]
    ).copy()
    rows_after_cleaning = normalized_dataframe.shape[0]
    deleted_rows = rows_before_cleaning - rows_after_cleaning

    print(f"After cleaning & deduplication: {normalized_dataframe.shape} (rows, cols)")
    print(f"Deleted rows: {deleted_rows}")

    # Step 3: Fuzzy clustering
    print("Performing fuzzy clustering...")
    unique_heavy_normalized = sorted(normalized_dataframe["normalized_heavy"].unique())
    fuzzy_groups = []
    
    if unique_heavy_normalized:
        current_group = [unique_heavy_normalized[0]]
        for heavy in unique_heavy_normalized[1:]:
            last_heavy = current_group[-1]
            similarity = difflib.SequenceMatcher(None, last_heavy, heavy).quick_ratio()
            if similarity >= fuzzy_threshold:
                current_group.append(heavy)
            else:
                fuzzy_groups.append(current_group)
                current_group = [heavy]
        fuzzy_groups.append(current_group)

    # Create fuzzy mapping
    heavy_to_fuzzy_map = {}
    for group in fuzzy_groups:
        key = group[0]
        for heavy in group:
            heavy_to_fuzzy_map[heavy] = key

    normalized_dataframe["fuzzy_heavy"] = normalized_dataframe["normalized_heavy"].map(
        heavy_to_fuzzy_map
    )

    # Step 4: Create clusters and representatives
    print("Creating clusters and representatives...")
    clusters = defaultdict(list)
    for record in normalized_dataframe.to_dict(orient="records"):
        clusters[record["fuzzy_heavy"]].append(record)

    representatives = {}
    for fingerprint, members in clusters.items():
        most_common_normalized = Counter(
            [member["normalized_light"] for member in members]
        ).most_common(1)[0][0]
        representatives[fingerprint] = most_common_normalized

    # Step 5: Add metadata columns
    normalized_dataframe["representative_name"] = normalized_dataframe["fuzzy_heavy"].map(
        representatives
    )
    normalized_dataframe["potential_industry_keywords"] = normalized_dataframe["normalized_heavy"]
    normalized_dataframe["search_query_website"] = (
        normalized_dataframe["representative_name"] + " official website"
    )
    normalized_dataframe["search_query_industry"] = (
        normalized_dataframe["representative_name"] + " industry"
    )

    # Step 6: Save normalized file
    print("Saving normalized data...")
    output_file = os.path.join(output_directory, "minimal_normalized.csv")
    if not is_dry_run:
        try:
            os.makedirs(output_directory, exist_ok=True)
            normalized_dataframe.to_csv(output_file, index=False)
            print(f"Saved -> {output_file} ({len(normalized_dataframe)} rows)")
        except Exception as error:
            print(f"Error saving {output_file}: {error}")
    else:
        print(f"Would save -> {output_file} ({len(normalized_dataframe)} rows)")

    # Step 7: Create batch files
    print("Creating batch files...")
    representatives_dataframe = normalized_dataframe.drop_duplicates(
        subset=["fuzzy_heavy"]
    )[
        [
            "fuzzy_heavy",
            "representative_name",
            "potential_industry_keywords",
            "search_query_website",
            "search_query_industry",
        ]
    ]

    total_representatives = len(representatives_dataframe)
    batch_size = (
        target_batch_size if total_representatives > target_batch_size else total_representatives
    )
    number_of_batches = max(1, (total_representatives + batch_size - 1) // batch_size)

    if not is_dry_run:
        try:
            os.makedirs(batch_directory, exist_ok=True)
        except Exception as error:
            print(f"Error creating {batch_directory}: {error}")
            return

    for batch_index in range(number_of_batches):
        start_index = batch_index * batch_size
        end_index = start_index + batch_size
        batch_data = representatives_dataframe.iloc[start_index:end_index].copy()
        batch_file = os.path.join(batch_directory, f"batch_{batch_index + 1:03d}.csv")
        
        if not is_dry_run:
            try:
                batch_data.to_csv(batch_file, index=False)
                print(f"Saved batch {batch_index + 1}/{number_of_batches} -> {batch_file} ({len(batch_data)} reps)")
            except Exception as error:
                print(f"Error saving {batch_file}: {error}")
        else:
            print(f"Would save batch {batch_index + 1}/{number_of_batches} -> {batch_file} ({len(batch_data)} reps)")

    print("Normalization completed successfully!")

if __name__ == "__main__":
    main()