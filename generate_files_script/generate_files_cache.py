import os
import random


NUM_FILES = 10000
FILE_SIZE = 4 * 1024  # 4KB


def generate_dataset(num_files, file_size, directory):
    os.makedirs(directory, exist_ok=True)
    for i in range(num_files):
        with open(f"{directory}/file_{i}.dat", "wb") as f:
            f.write(os.urandom(file_size))

if __name__ == "__main__":
    generate_dataset(NUM_FILES, FILE_SIZE, "report_dataset_cache")
