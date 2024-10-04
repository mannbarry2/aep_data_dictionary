import os
import json
import logging
from collections import defaultdict, Counter
import csv
from datetime import datetime
import time

# Setup detailed logging to the console and file
log_file = r'Z:\Luma Test\json_input\process_json.log'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[
    logging.FileHandler(log_file),
    logging.StreamHandler()
])

def read_json_file(file_path, max_retries=3):
    """Read and process the specific JSON file, with retry handling for permission errors."""
    logging.info(f'Processing JSON file: {file_path}')

    all_data = []
    retries = 0

    while retries < max_retries:
        try:
            logging.info(f"Reading file: {file_path}")
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_number, line in enumerate(f, start=1):
                    line = line.strip()
                    if line:  # Skip empty lines
                        try:
                            data = json.loads(line)
                            if isinstance(data, list):
                                all_data.extend(data)
                            else:
                                all_data.append(data)
                        except json.JSONDecodeError as e:
                            logging.error(f"Error decoding JSON in {file_path} at line {line_number}: {e}")
            break  # Exit retry loop if successful
        except PermissionError as e:
            retries += 1
            logging.error(f"Permission denied when accessing {file_path}: {e}")
            if retries < max_retries:
                logging.info(f"Retrying... ({retries}/{max_retries})")
                time.sleep(2)  # Wait before retrying
            else:
                logging.error(f"Max retries reached for {file_path}. Exiting.")
                return []
        except Exception as e:
            logging.error(f"Error reading {file_path}: {e}")
            return []

    logging.info(f"Total records read: {len(all_data)}")
    return all_data

def flatten_data(data, prefix, frequency_counts):
    """Flatten the JSON data and build the frequency count."""
    if isinstance(data, dict):
        for k, v in data.items():
            flatten_data(v, f'{prefix}{k}.', frequency_counts)
    elif isinstance(data, list):
        for item in data:
            flatten_data(item, prefix, frequency_counts)
    else:
        if data is not None:
            frequency_counts[prefix[:-1]][data] += 1

def process_data(data):
    """Process the data and build the frequency dictionary."""
    logging.info("Processing data to build the frequency dictionary.")
    frequency_counts = defaultdict(Counter)
    
    for entry in data:
        flatten_data(entry, '', frequency_counts)

    for key, value in frequency_counts.items():
        logging.info(f"{key}: {len(value)} values")

    logging.info("Finished processing data.")
    return frequency_counts

def write_to_csv(frequency_counts, output_file):
    """Write the frequency counts to a CSV file, sorted by instances in descending order."""
    logging.info(f"Writing frequency counts to CSV file: {output_file}")

    if not frequency_counts:
        logging.error("No data to write to the CSV.")
        return

    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)

        headers = []
        for field in frequency_counts.keys():
            headers.append(f'{field} [Value]')
            headers.append(f'{field} [Instances]')
        
        if headers:
            writer.writerow(headers)
            logging.info("Headers written to CSV.")
        else:
            logging.error("No headers found to write to the CSV file.")

        data_rows = defaultdict(list)
        for field in frequency_counts.keys():
            sorted_items = sorted(frequency_counts[field].items(), key=lambda x: x[1], reverse=True)
            for value, instances in sorted_items:
                data_rows[field].append((value, instances))

        max_length = max((len(data) for data in data_rows.values()), default=0)
        if max_length == 0:
            logging.error("No data found to write to the CSV.")
        else:
            for i in range(max_length):
                row = []
                for field in frequency_counts.keys():
                    try:
                        value, instances = data_rows[field][i]
                    except IndexError:
                        value, instances = '', ''
                    row.append(value)
                    row.append(instances)
                writer.writerow(row)

            logging.info(f"Data successfully written to CSV file: {output_file}")

    logging.info(f"CSV file {output_file} closed.")

def main():
    input_file = r'Z:\Luma Test\json_input\1.json'
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = r'Z:\Luma Test\out'
    output_filename = f'luma-dictionary-{current_time}.csv'
    output_file = os.path.join(output_dir, output_filename)

    data = read_json_file(input_file)
    frequency_counts = process_data(data)
    write_to_csv(frequency_counts, output_file)

if __name__ == "__main__":
    main()
