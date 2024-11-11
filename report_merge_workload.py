from cli import multiappend
import os
import sys
import subprocess
from time import sleep

TOTAL_AMOUNT = 1000

HOME=os.getenv("HOME")

loc_4kb = f"{HOME}/cs425MPs/g58_mp3/report_dataset_merge/file_4KB.dat"
loc_40kb = f"{HOME}/cs425MPs/g58_mp3/report_dataset_merge/file_40KB.dat"


# tun in report data set dir?
def launch_multiappend(hydfs_filename, vm_list, local_filenames):
    args = f"{hydfs_filename},{','.join(vm_list)},{','.join(local_filenames)}"
    print(args)
    multiappend([], args)
    return []

if __name__ == "__main__":
    # num concurrent clients, append size (4 or 40)
    num_concurrent_clients, append_size = int(os.sys.argv[1]), int(os.sys.argv[2])
    if num_concurrent_clients == 1:
        per_vm = TOTAL_AMOUNT // num_concurrent_clients
        for i in range(num_concurrent_clients):
            hydfs_filename = f"file_10MB.dat"
            vm_list = [f"fa24-cs425-5801.cs.illinois.edu"]
            if append_size == 4:
                local_filenames = [loc_4kb] * len(vm_list)
            else:
                local_filenames = [loc_40kb] * len(vm_list)
            for i in range(per_vm):
                launch_multiappend(hydfs_filename, vm_list, local_filenames)
            # sleep(0.1)
    elif num_concurrent_clients == 2:
        per_vm = TOTAL_AMOUNT // num_concurrent_clients
        for i in range(num_concurrent_clients):
            hydfs_filename = f"file_10MB.dat"
            vm_list = [f"fa24-cs425-5801.cs.illinois.edu", "fa24-cs425-5802.cs.illinois.edu"]
            local_filenames = []
            if append_size == 4:
                local_filenames = [loc_4kb] * len(vm_list)
            else:
                local_filenames = [loc_40kb] * len(vm_list)
            for i in range(per_vm):
                launch_multiappend(hydfs_filename, vm_list, local_filenames)
            # sleep(0.1)
    elif num_concurrent_clients == 5:
        per_vm = TOTAL_AMOUNT // num_concurrent_clients
        for i in range(num_concurrent_clients):
            hydfs_filename = f"file_10MB.dat"
            vm_list = [f"fa24-cs425-5801.cs.illinois.edu", "fa24-cs425-5802.cs.illinois.edu", "fa24-cs425-5803.cs.illinois.edu" , "fa24-cs425-5804.cs.illinois.edu" , "fa24-cs425-5805.cs.illinois.edu"]
            local_filenames = []
            if append_size == 4:
                local_filenames = [loc_4kb] * len(vm_list)
            else:
                local_filenames = [loc_40kb] * len(vm_list)
            for i in range(per_vm):
                launch_multiappend(hydfs_filename, vm_list, local_filenames)
            # sleep(0.1)
    elif num_concurrent_clients == 10:
        per_vm = TOTAL_AMOUNT // num_concurrent_clients
        for i in range(num_concurrent_clients):
            hydfs_filename = f"file_10MB.dat"
            vm_list = [f"fa24-cs425-5801.cs.illinois.edu", "fa24-cs425-5802.cs.illinois.edu", "fa24-cs425-5803.cs.illinois.edu" , "fa24-cs425-5804.cs.illinois.edu" , "fa24-cs425-5805.cs.illinois.edu", "fa24-cs425-5806.cs.illinois.edu", "fa24-cs425-5807.cs.illinois.edu" , "fa24-cs425-5808.cs.illinois.edu" , "fa24-cs425-5809.cs.illinois.edu" , "fa24-cs425-5810.cs.illinois.edu"]
            local_filenames = []
            if append_size == 4:
                local_filenames = [loc_4kb] * len(vm_list)
            else:
                local_filenames = [loc_40kb] * len(vm_list)
            for i in range(per_vm):
                launch_multiappend(hydfs_filename, vm_list, local_filenames)
            # sleep(0.1)
 


