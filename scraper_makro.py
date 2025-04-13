#!/usr/bin/env python
# coding: utf8

import sys
import os
import re
import json
import jsonlines
import logging
import datetime
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin

import configparser
from process_request import ProcessRequest
CALCULATED = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


class Scraper:
    """
    A scraper class for extracting product information from an online store.
    """

    def __init__(self) -> None:
        # Load configuration
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')

        # Folders
        self.FOLDER = self.create_folder(self.config['FOLDERS']['JSONL_OUTPUT'])
        self.IMAGE_DIRECTORY = self.create_folder(self.config['FOLDERS']['IMAGE_DIRECTORY'])

        # Networking
        self.prequest = ProcessRequest()

        # Config variables
        self.URL_BASE = self.config['SCRAPER']['URL_BASE']
        self.PRODUCT_KEYS = self.config['PRODUCT_FIELDS']['KEYS'].split(',')
        self.MAX_ITEMS_PER_PAGE = int(self.config['SCRAPER']['MAX_ITEMS_PER_PAGE'])

    def run(self) -> None:
        """
        Entry point to start scraping. Reads categories from config and launches the process.
        """
        categories = self.config['CATEGORIES']['CATEGORIES'].split(',')
        self.scrape_categories(categories)

    def scrape_categories(self, categories: List[str]) -> None:
        """
        Main scraping logic: iterate through categories and pages, parse and store product info.
        """
        for category in categories:
            logger.info(f"Scraping category: {category}")
            page = 1
            number_pages = None

            while True:
                logger.info(f"Fetching page {page} for category {category}")
                query_string = (
                    f"searchdiscover/articlesearch/search?"
                    f"storeId={self.config['API']['STORE_ID']}"
                    f"&language={self.config['API']['LANGUAGE']}"
                    f"&country={self.config['API']['COUNTRY']}"
                    f"&query=*"
                    f"&rows={self.MAX_ITEMS_PER_PAGE}"
                    f"&page={page}"
                    f"&filter=category:{category}"
                    f"&facets=true"
                    f"&categories=true"
                    f"&__t={self.get_timestamp()}"
                )
                url = urljoin(self.URL_BASE, query_string)

                response = self.prequest.set_request(
                    url=url,
                    headers=self.get_headers(1)
                )
                if not response:
                    logger.error("No response or error while requesting page data.")
                    break

                try:
                    parsed = response.json()
                except Exception as e:
                    logger.error(f"Could not parse listing JSON: {e}")
                    break

                amount = parsed.get("amount")
                if not amount:
                    logger.info("No products found or 'amount' missing. Stopping.")
                    break

                # Determine the total number of pages if not known
                if number_pages is None:
                    # E.g. if amount=125, MAX_ITEMS_PER_PAGE=50 => number_pages=3
                    number_pages = (amount // self.MAX_ITEMS_PER_PAGE) + (1 if amount % self.MAX_ITEMS_PER_PAGE else 0)

                # Parse products
                for item_dict in self.parser_products(parsed):
                    # Add additional attributes from config
                    item_dict["supermarket"] = self.config['SUPERMARKET']['NAME']
                    item_dict["supermarketPostalCode"] = self.config['SUPERMARKET']['POSTAL_CODE']
                    item_dict["currency"] = self.config['SUPERMARKET']['CURRENCY']
                    item_dict["country"] = self.config['SUPERMARKET']['COUNTRY']

                    pid = item_dict.get("productIdInSupermarket")
                    if pid:
                        self.dict_to_jsonl(item_dict, pid)

                if page < number_pages:
                    page += 1
                else:
                    break

    def parser_products(self, parsed: Dict[str, Any]) -> List[dict]:
        """
        Given the parsed JSON from the product listing, fetch details for each product ID.
        """
        output_items = []
        results = parsed.get("results", {})
        for key_id in results:
            product_details = self.parser_product_details(key_id)
            if product_details:
                output_items.append(product_details)
        return output_items

    def parser_product_details(self, product_id: str) -> dict:
        """
        Given a product_id, fetch its details (name, brand, ingredients, price, etc.).
        Returns a dictionary of product details.
        """


        # Remove trailing "0032" if present
        product_id = re.sub(r'0032$', "", product_id.strip())

        # Build product detail URL
        query_string = (
            "evaluate.article.v1/betty-articles?"
            f"ids={product_id}"
            "&country=ES"
            "&locale=es-ES"
            "&storeIds=00057"
            "&details=true"
            f"&__t={self.get_timestamp()}"
        )
        url_product = urljoin(self.URL_BASE, query_string)

        # Fetch data from the API
        response = self.prequest.set_request(url_product, headers=self.get_headers(2))
        if not response:
            logger.error(f"No response for product detail URL: {url_product}")
            return {}

        # Parse JSON response
        try:
            parsed_json = response.json()
        except Exception as e:
            logger.error(f"Could not parse JSON for product {product_id}. Error: {e}")
            return {}

        # Extract the result for this product
        result = parsed_json.get("result", {}).get(product_id)
        if not result:
            return {}

        # Prepare a dictionary for storing product details
        items = {}

        # The fields and their JSON "path chains"
        fields_map = {
            "productIdInSupermarket": ["variants", "0032", "bundles", "0021", "customerDisplayId"],
            "denomination":          ["variants", "0032", "description"],
            "categoryInSupermarket": ["variants", "0032", "categories"],
            "brand":                 ["brandName"],
            "manufacturer":          ["variants", "0032", "bundles", "0021", "stores", "00057", "supplier", "supplierName"],
            "description":           ["variants", "0032", "bundles", "0021", "details", "longDescription"],
            "measuringUnit":         ["variants", "0032", "bundles", "0021", "contentData", "weightPerPiece"],
            "units":                 ["variants", "0032", "bundles", "0021", "selector", "contentSize"],
            "priceWithTax":          ["variants", "0032", "bundles", "0021", "stores", "00057", "sellingPriceInfo", "finalPrice"],
            "price":                 ["variants", "0032", "bundles", "0021", "stores", "00057", "sellingPriceInfo", "shelfPrice"],
            "offerPrice":            ["variants", "0032", "bundles", "0021", "stores", "00057", "sellingPriceInfo", "basePrice"],
            "kgGross":               ["variants", "0032", "bundles", "0021", "stores", "00057", "sellingPriceInfo", "kgGross"],
            "isWeightArticle":       ["variants", "0032", "bundles", "0021", "isWeightArticle"],
            "rawIngredients":        ["variants", "0032", "bundles", "0021", "details", "features"],
            "nutritionInformation":  ["variants", "0032", "bundles", "0021", "details", "nutritionalTable"],
            "characteristics":       ["variants", "0032", "bundles", "0021", "details", "characteristicsTable"],
            "promotion":             ["variants", "0032", "bundles", "0021", "stores", "00057", "sellingPriceInfo", "summaryDnrInfo", "name"],
            # "imageLinks":            ["variants", "0032", "imageUrlL"]
        }

        # A dictionary mapping field names to "transform" handler methods
        field_handlers = {
            "categoryInSupermarket": self._handle_category_in_supermarket,
            "brand":                 self._handle_brand,
            "manufacturer":          self._handle_manufacturer,
            "measuringUnit":         self._handle_measuring_unit,
            "isWeightArticle":       self._handle_is_weight_article,
            "rawIngredients":        self._handle_ingredients,
            "nutritionInformation":  self._handle_nutrition,
            "characteristics":       self._handle_characteristics,
            "offerPrice":            self._handle_offer_price,
            "kgGross":               self._handle_kg_gross,
            # "imageLinks":            self._handle_images,
        }

        # 1) Extract all raw values by following the paths in fields_map.
        # 2) Apply transformations if a handler exists.
        for key, path_chain in fields_map.items():
            raw_value = self.get_value(result, path_chain.copy())
            if key in field_handlers:
                items[key] = field_handlers[key](raw_value, items, result)
            else:
                items[key] = raw_value

        name_for_url = items.get("denomination", "").replace(" ", "-").replace("/", "-")
        path_url = f"shop/pv/{product_id}/0032/0021/{name_for_url}"
        items["link"] = urljoin(self.URL_BASE, path_url)
        global CALCULATED
        if CALCULATED:
            with open('debug.txt', 'a') as f:
                f.write(f"link:{items.get('link')}, price = {items.get('unitPrice')}, denomination = {items.get('denomination')}")
            logging.warning(f"link:{items.get('link')}, price = {items.get('unitPrice')}, denomination = {items.get('denomination')}")
            CALCULATED = False


        return items

    def create_folder(self, directory_name: str) -> str:
        """
        Create a folder if it does not exist, and return its absolute path.
        """
        print(directory_name)
        if not os.path.isdir(directory_name):
            try:
                os.mkdir(directory_name)
                logger.info(f"Successfully created directory: {directory_name}")
            except OSError as e:
                logger.error(f"Creation of the directory {directory_name} failed: {e}")
        return os.path.realpath(directory_name)

    def get_headers(self, index: int, url_refer: Optional[str] = None) -> dict:
        """
        Returns the request headers based on the index provided.

        :param index: Index to choose a particular header set.
        :param url_refer: Referer URL if needed.
        :return: Dictionary of headers.
        """
        user_agent = self.config['USER_AGENT']['USER_AGENT']
        headers = [
            {
                "User-Agent": user_agent,
            },
            {
                "User-Agent": user_agent,
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "Referer": "https://tienda.makro.es/shop/category/alimentaci%C3%B3n-general",
                "Connection": "keep-alive",
            },
            {
                "User-Agent": user_agent,
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "CallTreeId": "B0412CEB-4877-425D-870B-1209664F19CD||BTEX-DE4211E8-1DF4-4C5A-A70E-EAE148AE316A",
                "X-Requested-With": "XMLHttpRequest",
                "Connection": "keep-alive",
                "Referer": "https://tienda.makro.es/shop/category/alimentaci%C3%B3n-general"
            },
            {
                "User-Agent": user_agent,
                "Accept": "image/avif,image/webp,*/*",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "Referer": url_refer or "",
                "Connection": "keep-alive"
            }
        ]
        return headers[index]

    def download_file(self, response, file_name: str) -> Union[str, bool]:
        """
        Download a file (e.g., image) from a response object and save it to disk.

        :param response: The requests response object (stream=True).
        :param file_name: The name of the file to save locally.
        :return: The path of the saved file or False if failed with status 404.
        """
        if response.status_code == 404:
            return False

        chunk_size = 2000
        file_path = os.path.join(self.IMAGE_DIRECTORY, file_name)
        with open(file_path, 'wb') as fd:
            for chunk in response.iter_content(chunk_size):
                fd.write(chunk)
        return file_path

    def jsonl_out(self, items: dict, product_id: str) -> None:
        file_name = f"makro_{product_id}.jsonl"
        output_path = os.path.join(self.FOLDER, file_name)
        with jsonlines.open(output_path, mode='w',
                            dumps=lambda x: json.dumps(x, ensure_ascii=False)) as writer:
            writer.write(items)

    def parser_measuring(self, value_dict: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Union[str, float]]:
        """
        Parse measuring (format, unit, value) from content data.
        """
        items = {}

        format_value = self.get_value(
            result, ["variants", "0032", "bundleSelector", "0021"]
        )
        if format_value:
            format_value = re.sub(r'\d+', "", format_value).strip()
        items["format"] = format_value

        other_value_dict = self.get_value(
            result, ["variants", "0032", "bundles", "0021", "contentData", "netContentVolume"]
        )
        if other_value_dict:
            value_dict = other_value_dict

        value = value_dict.get('value')
        unit = value_dict.get('uom')

        if unit == "GRAM":
            items["unit"] = "KG"
            items["value"] = (value / 1000) if value else 0
        elif unit == "ML":
            items["unit"] = "L"
            items["value"] = (value / 1000) if value else 0
        elif unit in ("KG", "L"):
            items["unit"] = unit
            items["value"] = value or 0
        else:
            items["unit"] = unit or ""
            items["value"] = value or 0

        return {
            "format": items.get("format", ""),
            "value": items.get("value", 0),
            "unit": items.get("unit", "")
        }

    def dict_to_jsonl(self, items: dict, product_id: str) -> None:
        """
        Filter the product dictionary to only the desired keys and then write to JSONL.
        """
        filtered_items = {key: items.get(key) for key in self.PRODUCT_KEYS}
        with open('debug.txt', 'a') as f:
            f.write(f"link:{filtered_items.get('link')}, unitprice:{filtered_items.get('unitPrice')}\n")
        logger.info(f"Writing item to JSONL: {filtered_items['denomination']}")
        self.jsonl_out(filtered_items, product_id)

    def get_value(self, obj: Any, chain: List[str]) -> Any:
        """
        Safely retrieve a nested value by following a list of keys.
        """
        if not obj or not chain:
            return ""
        key = chain.pop(0)
        if key in obj:
            return self.get_value(obj[key], chain) if chain else obj[key]
        return ""

    def _handle_category_in_supermarket(self, val, items, result):
        """
        Handles converting categoryInSupermarket from a list with nested dicts
        into a single string (e.g. "Fruits").
        """
        if val and isinstance(val, list) and len(val) > 0:
            return val[0].get('name', "").replace(" / ", "/")
        return None

    def _handle_brand(self, val, items, result):
        """
        Fallback to 'MAKRO' brand if nothing is found.
        """
        return val if val else "MAKRO"

    def _handle_manufacturer(self, val, items, result):
        """
        Return a dictionary with manufacturer name and an empty address if present.
        """
        return {"name": val.strip(), "address": None} if val else None

    def _handle_measuring_unit(self, val, items, result):
        """
        Parse measuring unit from contentData or fallback to netPieceWeight if needed.
        """
        if val:
            return self.parser_measuring(val, result)

        # Fallback check for netPieceWeight
        fallback_path = ["variants", "0032", "bundles", "0021", "contentData", "netPieceWeight"]
        fallback_val = self.get_value(result, fallback_path.copy())
        if fallback_val:
            return self.parser_measuring(fallback_val, result)

        return None

    def _handle_is_weight_article(self, val, items, result):
        """
        Convert 'weight' to WEIGHT or return None otherwise.
        """
        return "WEIGHT" if str(val).lower().strip() == 'weight' else None

    def _handle_ingredients(self, val, items, result):
        """
        Extract ingredients from the data dictionary if present.
        """
        ingredients = []
        for data_dict in val:
            label = data_dict.get("label")
            if label == "Listado de ingredientes":
                leafs = data_dict.get("leafs", [])
                for leaf in leafs:
                    value = leaf.get("label")
                    if value:
                        ingredients.append(value)
        if ingredients:
            return " ".join(ingredients)
        return None

    def _handle_nutrition(self, val, items, result):
        """
        Extract nutritional info from the dictionary structure.
        """
        if not isinstance(val,dict):
            return None
        items = {}
        fields = {
            "Valor energético kcal": "calories",
            "Proteínas": "protein",
            "Grasas": "fat",
            "de los cuales azúcares": "sugars",
            "Hidratos de carbono": "carbohydrates",
            "Fibra alimentaria": "fiber",
            "de las cuales saturadas": "saturatedFattyAcids",
            "Sal": "salt",
        }

        rows = val.get('rows', [])
        for row in rows:
            label = row.get('rowLabel')
            key = fields.get(label)
            if key:
                aux = row.get('cells', [])
                if aux and isinstance(aux[0], dict):
                    data_dict = aux[0]
                    items[key] = {
                        "value": data_dict.get("value"),
                        "unit": data_dict.get("unitOfMeasure")
                    }
        return items

    def _handle_characteristics(self, val, items, result):
        """
        Parse product characteristics from nested rows & cells.
        """
        characteristics = []
        if not isinstance(val, dict):
            return None
        rows = val.get('rows')
        if rows:
            for row in rows:
                label = row.get('rowLabel', "").strip()
                cells = row.get('cells', [])
                if not label:
                    continue
                for cell in cells:
                    # Combine non-empty values in a cell.
                    value = " ".join(cell[key].strip() for key in cell if cell.get(key))
                    value = value.strip()
                    if value:
                        characteristics.append(f"{label}: {value}")

        if characteristics:
            characteristic = ". ".join(characteristics)
            return " ".join(characteristic.split())
        return None

    def _handle_offer_price(self, val, items, result):
        """
        Special logic for offer price (promotions, volume discounts, etc.).
        Uses and potentially modifies 'items' in the process.
        """
        items["unitPriceWithOffer"] = None
        items["percentPromotion"] = None

        # Basic raw matching with shelf price
        if val == items.get("price"):
            # Check for promotion labels
            promo_labels = self.get_value(result, [
                "variants", "0032", "bundles", "0021", "stores",
                "00057", "sellingPriceInfo", "promotionLabels"
            ])
            # Check for levels info
            levels = self.get_value(result, [
                "variants", "0032", "bundles", "0021", "stores",
                "00057", "sellingPriceInfo", "summaryDnrInfo", "levels"
            ])

            # If no promotion labels or levels, it's not a real promotion
            if not promo_labels and not levels:
                return None

            # If there's a bigger logic (e.g. multi-level discounts),
            # apply it here as needed ...
            # (trimmed for brevity)
            return val
        else:
            # If we do have a real offer price different from shelf price
            try:
                shelf_price = items.get("price", 1)
                items["percentPromotion"] = round(1.0 - val / shelf_price, 2)
            except (ZeroDivisionError, TypeError):
                pass
            # Attempt to set a unit price with the offer
            try:
                # measuringUnit.value * units
                measuringUnit = items.get("measuringUnit", {})
                if not measuringUnit:
                    unit_count = 1
                else:
                    unit_count = measuringUnit.get("value", 1) * float(items.get("units", 1))
                items["unitPriceWithOffer"] = round(val / unit_count, 2)
            except (ZeroDivisionError, TypeError):
                pass

        return val

    def _handle_kg_gross(self, val, items, result):
        """
        If kgGross is None, try fallback or compute. Otherwise use val directly as unitPrice.
        """
        def _extract_grams(denomination):
            """
            Extract grams from the item denomination 
            """
            pattern = re.search(r'(\d+(?:\.\d+)?)x(\d+(?:\.\d+)?)([kK]?[gG])|(\d+(?:\.\d+)?)([kK]?[gG])', denomination)
            result = 1000
            if pattern:
                # Case: "85x20g" or "85x20kg"
                if pattern.group(1) and pattern.group(2) and pattern.group(3):
                    quantity = float(pattern.group(1))
                    weight = float(pattern.group(2))
                    unit = pattern.group(3)
                    multiplier = 1000 if unit.lower() == 'kg' else 1
                    result = quantity * weight * multiplier

                # Case: "800g"/"800G" or "2kg"/"2Kg"/"2KG"
                if pattern.group(4) and pattern.group(5):
                    weight = float(pattern.group(4))
                    unit = pattern.group(5)
                    multiplier = 1000 if unit.lower() == 'kg' else 1
                    result = weight * multiplier
            return result

        if val is None:
            alt_unit_price = self.get_value(
                result,
                [
                    "variants", "0032", "bundles", "0021", "stores", "00057",
                    "sellingPriceInfo", "basePriceData", "pricePerUnit", "netPrice"
                ]
            )
            if isinstance(alt_unit_price, (int, float)):
                items["unitPrice"] = alt_unit_price
            else:
                try:
                    if items.get('measuringUnit'):
                        measure_val = items.get("measuringUnit", {}).get("value", 1)
                    else:
                        measure_val = 1
                    unit_count  = measure_val * float(items.get("units", 1))
                    price_w_tax = items.get("priceWithTax", 0)
                    grams = _extract_grams(items.get('denomination'))
                    items["unitPrice"] = round(price_w_tax * 1000 / (grams * unit_count), 2)
                    global CALCULATED
                    CALCULATED = True
                except (ZeroDivisionError, TypeError):
                    items["unitPrice"] = None
        else:
            # If kgGross is not None, it is the unit price
            items["unitPrice"] = val
        return val

    def _handle_images(self, val, items, result):
        """
        Download images using parser_images and return the original URL (or empty list).
        """
        product_id = items.get("productIdInSupermarket", "unknown")
        urls = []
        if val:
            response = self.prequest.set_request(
                val,
                headers=self.get_headers(index=3, url_refer=val),
                stream=True
            )
            if response:
                file_name = f"{product_id}.jpg"
                self.download_file(response, file_name)
                urls.append(val)
        return urls


    @staticmethod
    def get_timestamp() -> int:
        """
        Return the current Unix timestamp (ms).
        """
        now = datetime.datetime.now()
        return int(datetime.datetime.timestamp(now) * 1000)


if __name__ == "__main__":
    print("Current Working Directory:", os.getcwd())
    scraper = Scraper()
    scraper.run()

