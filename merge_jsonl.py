import os
import json
import jsonlines

def merge_jsonl_to_json(input_dir, output_file):
    """
    Merges all .jsonl files in the specified directory into a single JSON file.

    Args:
        input_dir (str): Path to the directory containing .jsonl files.
        output_file (str): Path to the output JSON file.
    """
    all_data = []

    # Iterate over all files in the directory
    for file_name in os.listdir(input_dir):
        if file_name.endswith(".jsonl"):
            file_path = os.path.join(input_dir, file_name)

            # Read each .jsonl file and append its contents to the list
            with jsonlines.open(file_path) as reader:
                for obj in reader:
                    all_data.append(obj)

    # Write all collected data into a single JSON file
    with open(output_file, "w", encoding="utf-8") as json_file:
        json.dump(all_data, json_file, ensure_ascii=False, indent=4)

    print(f"Merged {len(all_data)} records from .jsonl files into {output_file}")

# Example usage
if __name__ == "__main__":
    input_directory = "jsonl_out"  # Replace with your input directory path
    output_file = "results/results.json"       # Output file name

    merge_jsonl_to_json(input_directory, output_file)
