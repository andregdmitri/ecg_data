import os
import numpy as np
import wfdb
from multiprocessing import Pool, cpu_count
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from threading import Lock

# Global lock to prevent race conditions when printing
print_lock = Lock()

def download_file(file_url, output_file):
    with open(output_file, 'wb') as f:
        f.write(requests.get(file_url).content)
    with print_lock:
        print(f"Downloaded {output_file}")

def download_files(p_folder, p_subfolder, s_segment, directory_url, output_folder):
    # Loop through the directories and files based on the structure described
    p_folder_str = f"p{p_folder}"
    p_subfolder_str = f"p{p_subfolder:05d}"
    s_segment_str = f"s{s_segment:02d}"

    # Construct file names
    atr_file = f"{p_folder_str}/{p_subfolder_str}/{p_subfolder_str}_{s_segment_str}.atr"
    dat_file = f"{p_folder_str}/{p_subfolder_str}/{p_subfolder_str}_{s_segment_str}.dat"
    hea_file = f"{p_folder_str}/{p_subfolder_str}/{p_subfolder_str}_{s_segment_str}.hea"

    # Construct download paths
    atr_url = urljoin(directory_url, atr_file)
    dat_url = urljoin(directory_url, dat_file)
    hea_url = urljoin(directory_url, hea_file)

    # Download the files
    print(f'Download {atr_url} to {output_folder}')
    download_file(atr_url, os.path.join(output_folder, atr_file))
    download_file(dat_url, os.path.join(output_folder, dat_file))
    download_file(hea_url, os.path.join(output_folder, hea_file))
    print('Download Sucessful')

def process_record(i):
    record_path = '.\\data\\p00'
    print(f'Patient {i}')
    for j in range(0, 50):
        pacient_num = f'p{i:05d}'
        annotation_path = f"{record_path}\\{pacient_num}\\{pacient_num}_s{j:02d}"

        try:
            # Load annotations
            annotation = wfdb.rdann(annotation_path, extension='atr')
            ann_sample = annotation.sample
            ann_symbol = annotation.symbol

            # Load ECG signal
            try:
                record = wfdb.rdrecord(annotation_path)
                fs = record.fs  # Sampling frequency
                ecg_signal = record.p_signal[:, 0]

                # Calculate window size based on window duration and sampling frequency
                window_start = int(0.4 * fs)
                window_end = int(0.7 * fs)

                # Iterate over each heartbeat and segment it
                for k, (peak, beat_type) in enumerate(zip(ann_sample[2:-2], ann_symbol[2:-2])):
                    if beat_type not in ["S", "V", "N"]:  # Ignore | and " beat_type
                        continue

                    # Centralize the segment around the peak
                    start = peak - window_start // 2
                    end = peak + window_end // 2

                    # Ensure the segment boundaries are within the signal range
                    start = max(0, start)
                    end = min(len(ecg_signal), end)

                    segment = ecg_signal[start:end]

                    # Check if the segment directory exists
                    segment_dir = os.path.join(record_path, 'segmented_data', beat_type, pacient_num)
                    if not os.path.exists(segment_dir):
                        os.makedirs(segment_dir)

                    # Check if the segment file already exists
                    file_name = f"{pacient_num}_s{j:02d}_beat_{k:05d}_{beat_type}.csv"
                    file_path = os.path.join(segment_dir, file_name)
                    if not os.path.exists(file_path):
                        # Save the segment as a new file in the segmented folder
                        np.savetxt(file_path, segment, delimiter=",")
                        print(f"Segment {k} ({beat_type}) as {file_name}")
                    # else:
                    #     print(f"SKIPPING - Segment {k} ({beat_type}) as it already exists.")
            except:
                print(f'{annotation_path} cannot load.')
                # url = 'https://physionet.org/files/icentia11k-continuous-ecg/1.0/'
                # output_folder = 'C:/Users/andre/1JUPYTER/IC/ecg_data/4_icentia11k/data/'

                # # Download the files
                # download_files('00', i, j, url, output_folder)

        except FileNotFoundError:
            print(f"FileNotFoundError: {annotation_path} not found. Skipping...")

if __name__ == "__main__":
    num_processes = cpu_count()
    # Create a pool of processes
    with Pool(num_processes) as pool:
        # Map the processing function to the range of patient indices
        pool.map(process_record, range(1000))
        pool.map(process_record, range(1000))
    print('Complete')