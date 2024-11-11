from cli import createfile
import os

HOME=os.getenv("HOME")

# create files called file_i.dat in hydfs
# ad files into hydfs
if __name__ == "__main__":
    for i in range(10000):
        filename = f"{HOME}/cs425MPs/g58_mp3/report_dataset_cache/file_{i}.dat"
        command = filename + f" file_{i}.dat"
        print(command)
        #createfile(["localhost"], filename)
