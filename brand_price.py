# File: brand_price_analysis.py

import os
import json
import jsonlines
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
import numpy as np # For handling potential NaN in plotting

# --- Configuration ---
JSONL_FOLDER = "jsonl_out"  # !!! ADAPT THIS to your actual output folder name from config.ini
WHITE_LABEL_FILE = "white_label_brands.json"
PRICE_COLUMN = 'unitPrice' # Use 'unitPrice' for fair comparison, or 'priceWithTax' as fallback
TOP_N_SUBCATEGORIES = 10 # Number of top first-level sub-categories within Alimentación General to plot
TARGET_MAIN_CATEGORY_PREFIX = "Alimentación general" # The prefix to filter categories (use exact case from JSON)
CATEGORY_SEPARATOR = "/" # The character separating category levels
# --- End Configuration ---

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_product_data(folder_path: str) -> pd.DataFrame:
    """Loads all product data from .jsonl files in a folder."""
    all_products = []
    if not os.path.isdir(folder_path):
        logging.error(f"Error: Folder not found: {folder_path}")
        return pd.DataFrame()

    logging.info(f"Loading product data from: {folder_path}")
    for filename in os.listdir(folder_path):
        if filename.endswith(".jsonl"):
            file_path = os.path.join(folder_path, filename)
            try:
                with jsonlines.open(file_path, mode='r') as reader:
                    for product in reader:
                        all_products.append(product)
            except Exception as e:
                logging.error(f"Error reading {filename}: {e}")

    if not all_products:
        logging.error(f"No product data loaded from {folder_path}. Check the folder and file contents.")
        return pd.DataFrame()

    df = pd.DataFrame(all_products)
    logging.info(f"Loaded {len(df)} products.")
    return df

def load_white_label_brands(file_path: str) -> set:
    """Loads white label brands from a JSON file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            brands = set(brand.strip().upper() for brand in data.get("general", []))
            logging.info(f"Loaded {len(brands)} white-label brand names.")
            return brands
    except FileNotFoundError:
        logging.error(f"Error: White label file not found: {file_path}")
        return set()
    except json.JSONDecodeError:
        logging.error(f"Error: Could not decode JSON from {file_path}")
        return set()
    except Exception as e:
        logging.error(f"Error loading white label brands: {e}")
        return set()

def extract_first_subcategory(category_string: str, main_category_prefix: str, separator: str) -> str:
    """
    Extracts the first level subcategory after the main category prefix.
    Example: 'Alimentación general/Quesos/Quesos frescos' -> 'Quesos'
    """
    if not isinstance(category_string, str) or not category_string.startswith(main_category_prefix):
        return None # Or return 'Unknown' or ''

    parts = category_string.split(separator)
    # parts[0] is main_category_prefix
    if len(parts) > 1 and parts[1]: # Check if there is a part after the first separator and it's not empty
        return parts[1].strip()
    else:
        # Handle cases like "Alimentación general" or "Alimentación general/"
        return None # Or a placeholder like 'Base Level'

def analyze_brand_prices_by_subcategory(df: pd.DataFrame, white_brands: set, price_col: str, main_category_prefix: str, separator: str):
    """
    Analyzes prices and counts grouping by the first-level subcategory
    within the pre-filtered DataFrame.
    """
    if df.empty or white_brands is None:
        logging.warning("Input DataFrame for subcategory analysis is empty or white brands set is missing.")
        return None

    # --- Data Cleaning & Preparation ---
    required_cols = ['brand', 'categoryInSupermarket', price_col]
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        logging.error(f"Missing required columns in filtered DataFrame: {missing}")
        return None

    df_analysis = df.copy()

    # 1. Extract First-Level Subcategory
    df_analysis['first_subcategory'] = df_analysis['categoryInSupermarket'].apply(
        lambda x: extract_first_subcategory(x, main_category_prefix, separator)
    )
    # Remove rows where subcategory couldn't be extracted or is empty/None
    df_analysis = df_analysis.dropna(subset=['first_subcategory'])
    df_analysis = df_analysis[df_analysis['first_subcategory'] != '']

    if df_analysis.empty:
        logging.warning(f"No products found with a valid first-level subcategory after '{main_category_prefix}'.")
        return None

    # 2. Clean brand names
    df_analysis['brand'] = df_analysis['brand'].fillna('UNKNOWN').astype(str).str.strip().str.upper()

    # 3. Classify brands
    df_analysis['is_white_label'] = df_analysis['brand'].apply(lambda x: x in white_brands)
    df_analysis['brand_type'] = df_analysis['is_white_label'].apply(lambda x: 'White-Label' if x else 'Non-White-Label')

    # 4. Clean price column
    df_analysis[price_col] = pd.to_numeric(df_analysis[price_col], errors='coerce')
    df_cleaned = df_analysis.dropna(subset=[price_col])
    df_cleaned = df_cleaned[df_cleaned[price_col] > 0]

    if df_cleaned.empty:
        logging.warning(f"No products with valid '{price_col}' found after cleaning within the filtered subcategories.")
        return None

    logging.info(f"Analyzing {len(df_cleaned)} products grouped by first-level subcategory.")

    # --- Aggregation (by extracted first_subcategory) ---
    analysis = df_cleaned.groupby(['first_subcategory', 'brand_type']).agg(
        mean_price=(price_col, 'mean'),
        product_count=(price_col, 'size')
    ).reset_index()

    # --- Pivot ---
    price_pivot = analysis.pivot(index='first_subcategory', columns='brand_type', values='mean_price')
    count_pivot = analysis.pivot(index='first_subcategory', columns='brand_type', values='product_count')

    # --- Combine and Calculate Total Count ---
    all_first_subcategories = df_cleaned['first_subcategory'].unique()
    combined_analysis = pd.DataFrame(index=all_first_subcategories)
    combined_analysis = combined_analysis.join(price_pivot).join(count_pivot, lsuffix='_price', rsuffix='_count')

    combined_analysis.columns = ['White-Label_price', 'Non-White-Label_price', 'White-Label_count', 'Non-White-Label_count']
    combined_analysis[['White-Label_count', 'Non-White-Label_count']] = combined_analysis[['White-Label_count', 'Non-White-Label_count']].fillna(0).astype(int)
    combined_analysis['total_count'] = combined_analysis['White-Label_count'] + combined_analysis['Non-White-Label_count']

    logging.info("Analysis by first-level subcategory complete.")
    return combined_analysis

def plot_comparison(analysis_df: pd.DataFrame, value_col_prefix: str, title: str, ylabel: str, xlabel:str):
    """Generates a grouped bar chart for the provided DataFrame (assumed to be top N)."""
    if analysis_df is None or analysis_df.empty:
        logging.warning(f"No data to plot for '{title}'.")
        return

    # Sort by total count for better visualization of top categories
    plot_data = analysis_df.sort_values('total_count', ascending=False).copy()

    logging.info(f"Plotting '{title}' for {len(plot_data)} categories.")

    cols_to_plot = [col for col in plot_data.columns if col.endswith(value_col_prefix)]
    plot_subset = plot_data[cols_to_plot]
    plot_subset.columns = [col.replace(value_col_prefix, '').replace('_', '-') for col in cols_to_plot]

    ax = plot_subset.plot(kind='bar', figsize=(15, 8), width=0.8)

    plt.title(title, fontsize=16)
    plt.xlabel(xlabel, fontsize=12) # Use the provided xlabel
    plt.ylabel(ylabel, fontsize=12)
    plt.xticks(rotation=45, ha='right', fontsize=10)
    plt.yticks(fontsize=10)
    plt.legend(title='Brand Type')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()


# --- Main Execution ---
if __name__ == "__main__":
    product_df_full = load_product_data(JSONL_FOLDER)
    white_brands_set = load_white_label_brands(WHITE_LABEL_FILE)

    if not product_df_full.empty and white_brands_set:

        # --- Filter for TARGET_MAIN_CATEGORY_PREFIX ---
        if 'categoryInSupermarket' not in product_df_full.columns:
             logging.error("Column 'categoryInSupermarket' not found. Cannot filter.")
             product_df_target_main = pd.DataFrame()
        else:
            # Ensure case-sensitive match if needed, otherwise use .str.lower() on both sides
            product_df_full['categoryInSupermarket_clean'] = product_df_full['categoryInSupermarket'].fillna('').astype(str).str.strip()
            # Using TARGET_MAIN_CATEGORY_PREFIX directly for case-sensitivity as in example
            target_filter = product_df_full['categoryInSupermarket_clean'].str.startswith(TARGET_MAIN_CATEGORY_PREFIX + CATEGORY_SEPARATOR, na=False) \
                            | (product_df_full['categoryInSupermarket_clean'] == TARGET_MAIN_CATEGORY_PREFIX) # Include if it's exactly the main category
            product_df_target_main = product_df_full[target_filter].copy()
            logging.info(f"Filtered down to {len(product_df_target_main)} products potentially in categories starting with '{TARGET_MAIN_CATEGORY_PREFIX}'.")

        # --- Proceed only if we have products in the target main category ---
        if not product_df_target_main.empty:
            # Analyze these products, grouping by the *first subcategory*
            analysis_results_subcat = analyze_brand_prices_by_subcategory(
                product_df_target_main,
                white_brands_set,
                PRICE_COLUMN,
                TARGET_MAIN_CATEGORY_PREFIX,
                CATEGORY_SEPARATOR
            )

            if analysis_results_subcat is not None and not analysis_results_subcat.empty:

                # --- Select Top N First-Level Sub-Categories ---
                top_n_subcategories_df = analysis_results_subcat.nlargest(TOP_N_SUBCATEGORIES, 'total_count')

                if top_n_subcategories_df.empty:
                    logging.warning(f"No valid first-level sub-categories found within '{TARGET_MAIN_CATEGORY_PREFIX}' after analysis, cannot select top {TOP_N_SUBCATEGORIES}.")
                else:
                    num_selected = len(top_n_subcategories_df)
                    logging.info(f"Selected top {num_selected} most populated first-level sub-categories within '{TARGET_MAIN_CATEGORY_PREFIX}' for plotting.")
                    plot_xlabel = f"First-Level Subcategory within {TARGET_MAIN_CATEGORY_PREFIX}"


                    # --- Plotting using the filtered top N subcategory data ---
                    plot_comparison(
                        top_n_subcategories_df,
                        value_col_prefix='_price',
                        title=f'Mean {PRICE_COLUMN} (Top {num_selected} Sub-Cats in {TARGET_MAIN_CATEGORY_PREFIX}): White-Label vs. Non-White-Label',
                        ylabel=f'Mean {PRICE_COLUMN} (€)',
                        xlabel=plot_xlabel
                    )

                    plot_comparison(
                        top_n_subcategories_df,
                        value_col_prefix='_count',
                        title=f'Product Count (Top {num_selected} Sub-Cats in {TARGET_MAIN_CATEGORY_PREFIX}): White-Label vs. Non-White-Label',
                        ylabel='Number of Products',
                        xlabel=plot_xlabel
                    )
            else:
                logging.warning(f"Analysis by first-level subcategory within '{TARGET_MAIN_CATEGORY_PREFIX}' did not produce results suitable for plotting.")
        else:
            logging.warning(f"No products found belonging to categories starting with '{TARGET_MAIN_CATEGORY_PREFIX}'. Cannot perform analysis.")
    else:
        print("Could not load product data or white label brands. Exiting analysis.")