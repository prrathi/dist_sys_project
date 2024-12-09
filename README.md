```markdown
# RainStorm: A Stream Processing Framework

RainStorm is a high-performance stream processing framework designed to handle real-time analytics on continuous data streams. This README provides comprehensive instructions for setting up, building, and running RainStorm on your cluster of virtual machines (VMs).

## Table of Contents

1. [Requirements](#requirements)
2. [Setup](#setup)
   - [1. Clone the Repository](#1-clone-the-repository)
   - [2. Build the Project](#2-build-the-project)
   - [3. Run RainStorm](#3-run-rainstorm)
   - [4. Join All RainStorm Nodes to the Cluster](#4-join-all-rainstorm-nodes-to-the-cluster)
   - [5. Upload Dataset to HyDFS](#5-upload-dataset-to-hydfs)
   - [6. Submit a RainStorm Job](#6-submit-a-rainstorm-job)
3. [Usage](#usage)
4. [Additional Notes](#additional-notes)
5. [Troubleshooting](#troubleshooting)
6. [Contact](#contact)

---

## Requirements

Before setting up RainStorm, ensure that the following dependencies are installed on your system:

1. **CMake**: A cross-platform build system generator.
   - [CMake Installation Guide](https://cmake.org/install/)

2. **gRPC**: A high-performance, open-source universal RPC framework.
   - [gRPC C++ Quickstart](https://grpc.io/docs/languages/cpp/quickstart/)

Ensure that both **CMake** and **gRPC** are properly installed and accessible in your system's PATH.

---

## Setup

Follow these steps to set up and deploy RainStorm on your cluster of VMs.

### 1. Clone the Repository

Begin by cloning the RainStorm repository from GitLab and navigating to the project directory.

```bash
git clone https://gitlab.engr.illinois.edu/prathi3/g58_mp4.git
cd g58_mp4
```

### 2. Build the Project

Compile the RainStorm project using CMake and Make.

1. **Create a Build Directory and Navigate Into It:**

   ```bash
   mkdir build
   cd build
   ```

2. **Configure the Project with CMake:**

   ```bash
   cmake ..
   ```

3. **Compile the Code:**

   ```bash
   make -j4
   ```

   The `-j4` flag tells Make to use 4 parallel jobs. Adjust this number based on the number of CPU cores available on your machine for faster compilation.

### 3. Run RainStorm

Start the RainStorm executable on **each VM** in your cluster.

```bash
./Rainstorm {type}
```

Ensure that RainStorm is running on all intended VMs before proceeding to the next steps. `type` should be `leader` for the lead node or `factory` for non-lead node

### 4. Join All RainStorm Nodes to the Cluster

On the **master node**, execute the following command to join all RainStorm nodes to the cluster. Replace the email and VM addresses with your specific details if they differ.

```bash
python3 cli.py -c join -m \
kornj2@fa24-cs425-5801.cs.illinois.edu \
kornj2@fa24-cs425-5802.cs.illinois.edu \
kornj2@fa24-cs425-5803.cs.illinois.edu \
kornj2@fa24-cs425-5804.cs.illinois.edu \
kornj2@fa24-cs425-5805.cs.illinois.edu \
kornj2@fa24-cs425-5806.cs.illinois.edu \
kornj2@fa24-cs425-5807.cs.illinois.edu \
kornj2@fa24-cs425-5808.cs.illinois.edu \
kornj2@fa24-cs425-5809.cs.illinois.edu \
kornj2@fa24-cs425-5810.cs.illinois.edu
```

Ensure that all VM addresses are correct and that SSH access is properly configured between the master and worker nodes.

### 5. Upload Dataset to HyDFS

On the **master node**, upload the `TrafficSigns_1000.csv` dataset to HyDFS using the following command:

```bash
python3 cli.py -c create datasets/TrafficSigns_1000.csv TrafficSigns_1000
```

**Parameters Explanation:**

- `-c create`: Specifies the command to upload/create a dataset in HyDFS.
- `datasets/TrafficSigns_1000.csv`: Path to the local CSV file containing the dataset.
- `TrafficSigns_1000`: The name assigned to the dataset in HyDFS.

### 6. Submit a RainStorm Job

Submit your RainStorm job from the **master node** using the following command:

```bash
python3 cli.py -c rainstorm -r ./datasets/test1_1 ./datasets/test1_2 TrafficSigns_1000 TrafficSigns_1000_Rainstorm 3
```
**Parameters Explanation:**

- `-c rainstorm`: Specifies the command to run a RainStorm job.
- `-r ./datasets/test1_1 ./datasets/test1_2`: op1 and op2 (executables)
- `TrafficSigns_1000`: The source file in HyDFS.
- `TrafficSigns_1000_Rainstorm`: The destination file in HyDFS for output.
- `3`: Number of tasks per stage.

Ensure that the executables `./datasets/test1_1` and `./datasets/test1_2` exist and have the necessary perms.
---


---

```