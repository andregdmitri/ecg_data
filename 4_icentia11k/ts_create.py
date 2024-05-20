import os
import random
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# Define paths
ts_train_path = '.\\ts_files\\train_m.ts'
ts_test_path = '.\\ts_files\\test_m.ts'

# Create directories if they don't exist
os.makedirs(os.path.dirname(ts_train_path), exist_ok=True)
os.makedirs(os.path.dirname(ts_test_path), exist_ok=True)

# Function to read content from CSV file
def read_csv_content(file_path):
    try:
        data = np.genfromtxt(file_path, delimiter=",")
        if data.size == 0:  # Check if the data is empty
            return None
        return data
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

# Function to process a single file
def process_file(file_info):
    file_path, beat, label = file_info
    segment_content = read_csv_content(file_path)
    if segment_content is not None and label:
        if (label == 'V' and beat == 'V') or (label == 'N' and beat == 'N') or (label == 'S' and beat == 'S'):
            return (segment_content, label)
    return None

# Function to create .TS file
def create_ts_file(ts_file_path, df, max_labels):
    segments_N, segments_V, segments_S = [], [], []

    file_info_list = []

    for index, row in df.iterrows():
        patient = row['Patient']
        beat = row['Class']
        path = row['path']

        # Iterate over all records
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith(".csv"):
                    file_path = os.path.join(root, file)
                    # Extract label from the filename
                    label = file.split('_')[-1][0]
                    file_info_list.append((file_path, beat, label))

    # Use ThreadPoolExecutor to parallelize file processing
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        future_to_file_info = {executor.submit(process_file, file_info): file_info for file_info in file_info_list}
        for future in as_completed(future_to_file_info):
            result = future.result()
            if result is not None:
                segment_content, label = result
                if label == 'S':
                    segments_S.append((segment_content, label))
                elif label == 'V':
                    segments_V.append((segment_content, label))
                elif label == 'N':
                    segments_N.append((segment_content, label))

    # Shuffle and balance segments
    random.shuffle(segments_S)
    random.shuffle(segments_V)
    random.shuffle(segments_N)

    min_count = min(len(segments_S), len(segments_V), len(segments_N))
    segments_S = segments_S[:min(max_labels, min_count)]
    segments_V = segments_V[:min(max_labels, min_count)]
    segments_N = segments_N[:min(max_labels, min_count)]

    with open(ts_file_path, 'w') as ts_file:
        for segment_content, label in segments_S + segments_V + segments_N:
            ts_file.write(','.join(map(str, segment_content)) + f":{label}\n")

    print("TS file created successfully.")
    print(f"Number of 'N' labels: {len(segments_N)}")
    print(f"Number of 'V' labels: {len(segments_V)}")
    print(f"Number of 'S' labels: {len(segments_S)}\n")


# Create .TS files
X_test = pd.read_csv('X_test.csv', index_col=0)
X_train = pd.read_csv('X_train.csv', index_col=0)
create_ts_file(ts_test_path, X_test, 500)
create_ts_file(ts_train_path, X_train, 2000)
