import generate_files_cache
import os

def generate_file(filename, size_bytes, directory):
    os.makedirs(directory, exist_ok=True)
    with open(directory + "/" + filename, 'wb') as f:
        f.write(os.urandom(size_bytes))
    print(f"Generated {filename} of size {size_bytes / 1024:.2f} KB")

if __name__ == "__main__":
    # 10 MB
    generate_file('file_10MB.dat', 10 * 1024 * 1024, "report_dataset_merge")

    # 4KB
    generate_file('file_4KB.dat', 4 * 1024, "report_dataset_merge")

    # 40KB
    generate_file('file_40KB.dat', 40 * 1024, "report_dataset_merge")
