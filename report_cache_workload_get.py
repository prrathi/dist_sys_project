from cli import getfile, appendfile
import os
import sys
import random
import numpy as np
from time import time


HOME=os.getenv("HOME")
loc_4kb = f"{HOME}/cs425MPs/g58_mp3/report_dataset_merge/file_4KB.dat"

def uniform_read_workload(file_count, read_count):
    return [f"file_{random.randint(0, file_count - 1)}.dat" for _ in range(read_count)]

def zipfian_read_workload(file_count, read_count, alpha=1.2):
    zipf_distribution = np.random.zipf(alpha, read_count)
    return [f"file_{min(x - 1, file_count - 1)}.dat" for x in zipf_distribution if x <= file_count]

def generate_work(file_count, read_count, workload_type, get_probability):
    workload = []
    if workload_type == "uniform":
        workload_func = uniform_read_workload
    elif workload_type == "zipfian":
        workload_func = zipfian_read_workload
    file_indices = workload_func(file_count, read_count)

    for file_index in file_indices:
        if random.random() <= get_probability:
            workload.append(('get', file_index))
        else:  # 10% probability of `append`
            workload.append(('append', file_index))
    
    return workload

def callCommands(work_load):
    for i, j in work_load:
        if i == 'get':
            command = f"{j} garbage.dat"
            #print(command)
            getfile(['localhost'], command)
        else:
            # Using the 4kb file for append
            command = f"{loc_4kb} {j}"
            #print(command)
            appendfile(['localhost'], command)

# ad files into hydfs
if __name__ == "__main__":
    # manually change cache size ranging from 4MB 8MB 20MB and 40MB

    workload_type, get_prob = os.sys.argv[1], float(os.sys.argv[2])
    file_count = 10000
    num_reads= 25000
    work_load = generate_work(file_count, num_reads, workload_type, get_prob)
    callCommands(work_load)