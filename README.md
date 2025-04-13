
## Setup

1.  **Prerequisites:**
    *   Python 3.7+

2.  **Clone Repository (Optional):**
    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

3.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configuration:**
    *   **`config.ini`:** Review and update this file *before running the scraper*. Pay attention to:
        *   `[FOLDERS]`: Ensure `JSONL_OUTPUT` matches the folder name used in `main.py` (`jsonl_output`).
        *   `[CATEGORIES]`: Specify the Makro categories you want to scrape.
        *   Other API/Supermarket details as needed.
    *   **`white_label_brands.json`:** Ensure this file contains the correct list of white-label brands for the price analysis.

## Execution Workflow (Data Collection & Processing)

The `main.py` script automates the core data collection and processing steps.

1.  **Run the Main Script:**
    ```bash
    python main.py
    ```

2.  **What it Does:**
    *   Creates the `jsonl_output/` and `results/` folders if they don't exist.
    *   Executes `scraper_makro.py` to scrape data based on `config.ini`. Individual product data is saved as `.jsonl` files in `jsonl_output/`.
    *   Executes `merge_jsonl.py` to combine all files from `jsonl_output/` into a single `results/merged_products.jsonl` file.
    *   Executes `json_to_csv.py` to convert `results/merged_products.jsonl` into `results/merged_products.csv`.
    *   Logs the entire process to console and a timestamped `main_run_*.log` file.

3.  **Outputs:**
    *   `jsonl_output/`: Contains one `.jsonl` file per scraped product.
    *   `results/merged_products.jsonl`: A single file containing all scraped products in JSON Lines format.
    *   `results/merged_products.csv`: A CSV representation of all scraped products. **Note:** Nested data (like nutrition information) will appear as string representations of dictionaries in the CSV. Further processing might be needed depending on your analysis tools.

## Running Analysis Scripts

**After** successfully running `python main.py`, you can run the individual analysis scripts:

1.  **Cosine Similarity Search:**
    *   **Purpose:** Finds products that are nutritionally similar to a given product ID.
    *   **Requires:** The individual `.jsonl` files in `jsonl_output/` (as currently written). Ensure the `JSONL_FOLDER` variable inside `similarity_searcher.py` matches `jsonl_output`.
    *   **Command:**
        ```bash
        python similarity_searcher.py
        ```
    *   **Usage:** The script will prompt you to enter a `productIdInSupermarket`. It will then print the top N most similar products based on nutrition. Type `quit` to exit.

2.  **Brand Price Analysis:**
    *   **Purpose:** Compares the average prices and counts of white-label vs. non-white-label products across the most populated sub-categories within "Alimentación general".
    *   **Requires:** The individual `.jsonl` files in `jsonl_output/` (as currently written). Ensure the `JSONL_FOLDER` variable inside `brand_price_analysis.py` matches `jsonl_output`. Also requires `white_label_brands.json`.
    *   **Command:**
        ```bash
        python brand_price_analysis.py
        ```
    *   **Usage:** The script will process the data and display two bar charts comparing prices and product counts. You can configure the number of top categories (`TOP_N_SUBCATEGORIES`) and the target main category (`TARGET_MAIN_CATEGORY_PREFIX`) inside the script itself.

## Troubleshooting

*   **Script Not Found Errors:** Ensure all `.py` files are in the same directory where you are running the commands.
*   **Scraper Errors:** Check the `main_run_*.log` file and any output from `scraper_makro.py`. Errors might be due to network issues, changes in the Makro website/API, or incorrect configuration in `config.ini`.
*   **Permission Errors:** Ensure you have write permissions in the directory where the script is creating folders (`jsonl_output`, `results`).
*   **Encoding Issues (CSV):** If characters appear garbled in Excel, ensure `json_to_csv.py` uses `encoding='utf-8-sig'` when writing the CSV.