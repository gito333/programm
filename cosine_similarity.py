# File: similarity_searcher.py

import os
import jsonlines
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler
import logging
import re

# --- Configuration ---
JSONL_FOLDER = "jsonl_out"  # !!! ADAPT THIS to your actual output folder name from config.ini
NUTRITION_FEATURES = [
    'calories', 'protein', 'fat', 'carbohydrates',
    'sugars', 'salt', 'saturatedFattyAcids', 'fiber'
]
TOP_N_SIMILAR = 5 # Number of similar products to show
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
                        if 'productIdInSupermarket' in product and product['productIdInSupermarket']:
                            all_products.append(product)
                        else:
                            logging.warning(f"Skipping record in {filename} due to missing 'productIdInSupermarket'")
            except Exception as e:
                logging.error(f"Error reading {filename}: {e}")

    if not all_products:
        logging.error(f"No product data loaded from {folder_path}. Check the folder and file contents.")
        return pd.DataFrame()

    df = pd.DataFrame(all_products)
    logging.info(f"Loaded {len(df)} products.")
    df['productIdInSupermarket'] = df['productIdInSupermarket'].astype(str)
    df = df.set_index('productIdInSupermarket', drop=False) 
    return df

def extract_nutrition_vector(nutrition_info: dict, features: list) -> list:
    """Extracts a numerical vector for defined nutritional features."""
    vector = []
    if not isinstance(nutrition_info, dict):
        return [0.0] * len(features) 

    for feature in features:
        value = 0.0
        feature_data = nutrition_info.get(feature)
        if isinstance(feature_data, dict):
            raw_value = feature_data.get('value')
            try:
                match = re.search(r'(\d+(\.\d+)?)', str(raw_value))
                if match:
                    value = float(match.group(1))
                elif isinstance(raw_value, str) and '<' in raw_value:
                     match_less = re.search(r'<\s*(\d+(\.\d+)?)', raw_value)
                     if match_less:
                         value = float(match_less.group(1)) / 2 
                     else:
                         value = 0.0 
                elif raw_value is None or str(raw_value).strip().lower() in ['trazas', 'traces', '-']:
                     value = 0.0
                else: 
                    value = float(raw_value)

            except (ValueError, TypeError):
                value = 0.0 
        vector.append(value)
    return vector

def find_similar_products(product_id: str, df: pd.DataFrame, features: list, top_n: int = 5):
    """Finds products similar to the given product_id based on nutrition."""
    if product_id not in df.index:
        logging.error(f"Product ID '{product_id}' not found in the dataset.")
        return None

    logging.info(f"Extracting nutritional vectors for {len(df)} products...")
    nutrition_vectors = df['nutritionInformation'].apply(lambda x: extract_nutrition_vector(x, features))
    nutrition_matrix = np.array(nutrition_vectors.tolist())
    nutrition_matrix_scaled = nutrition_matrix 

    # Handle potential NaN/Inf values after extraction/scaling 

    nutrition_matrix_scaled = np.nan_to_num(nutrition_matrix_scaled)


    logging.info("Calculating cosine similarity matrix...")
    try:
        cosine_sim = cosine_similarity(nutrition_matrix_scaled)
        id_to_index = {id_: i for i, id_ in enumerate(df.index)}
        index_to_id = {i: id_ for id_, i in id_to_index.items()}

        target_index = id_to_index[product_id]
        similarity_scores = list(enumerate(cosine_sim[target_index]))

        similarity_scores = sorted(similarity_scores, key=lambda x: x[1], reverse=True)
        similar_products_indices = [i for i, score in similarity_scores[1:top_n + 1]] # Skip the first one 

        logging.info(f"\nTop {top_n} products similar to '{df.loc[product_id, 'denomination']}' (ID: {product_id}):")

        results = []
        for i in similar_products_indices:
            similar_product_id = index_to_id[i]
            score = cosine_sim[target_index, i]
            name = df.loc[similar_product_id, 'denomination']
            print(f"- ID: {similar_product_id}, Name: {name}, Similarity: {score:.4f}")
            results.append({
                "id": similar_product_id,
                "name": name,
                "score": score
            })
        return results

    except Exception as e:
        logging.error(f"Error calculating similarities: {e}")
        logging.error(f"Matrix shape: {nutrition_matrix_scaled.shape}")
        return None


# --- Main Execution ---
if __name__ == "__main__":
    product_df = load_product_data(JSONL_FOLDER)

    if not product_df.empty:
        # Keep only products with some nutrition info for similarity calculation
        # This avoids issues with all-zero vectors in cosine similarity
        product_df_nutri = product_df.dropna(subset=['nutritionInformation'])
        product_df_nutri = product_df_nutri[product_df_nutri['nutritionInformation'] != {}]
        
        valid_nutri_indices = []
        for idx, row in product_df_nutri.iterrows():
             vec = extract_nutrition_vector(row['nutritionInformation'], NUTRITION_FEATURES)
             if any(v > 0 for v in vec): 
                 valid_nutri_indices.append(idx)
        product_df_nutri_filtered = product_df_nutri.loc[valid_nutri_indices]
        if product_df_nutri_filtered.empty:
             logging.warning("No products with valid numeric nutritional data found after filtering.")
        else:
            logging.info(f"Proceeding with similarity calculation for {len(product_df_nutri_filtered)} products with nutrition data.")

            while True:
                target_product_id = input(f"Enter the 'productIdInSupermarket' to find similar products (or 'quit'): ").strip()
                if target_product_id.lower() == 'quit':
                    break
                if not target_product_id:
                    continue
                find_similar_products(target_product_id, product_df_nutri_filtered, NUTRITION_FEATURES, TOP_N_SIMILAR)
                print("-" * 20)
    else:
        print("Could not load product data. Exiting.")