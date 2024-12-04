## Requirements

1. **CMake** and **gRPC** must be installed.
   - Follow installation instructions for your system:
     - [CMake installation](https://cmake.org/install/)
     - [gRPC installation](https://grpc.io/docs/languages/cpp/quickstart/)

## Setup

1. **Clone the repository** and navigate to the project directory:
   ```bash
   git clone https://gitlab.engr.illinois.edu/prathi3/g58_mp4
   cd g58_mp4
   ```

2. **Build the project**:
   - Create a build directory and navigate into it:
     ```bash
     mkdir build
     cd build
     ```
   - Run `cmake` to configure the project:
     ```bash
     cmake ..
     ```
   - Compile the code with:
     ```bash
     make -j4
     ```

3. **Run HyDFS**:
   - Start the `HYDFS` executable on each VM:
     ```bash
     ./HYDFS
     ```

## Usage

HyDFS uses a command-line interface (CLI) to interact with the distributed file system. Commands include joining nodes, creating files, retrieving files, appending content, merging files, and more.

### Initializing the HyDFS Ring

To set up the distributed file system, ensure `HYDFS` is running on all participating VMs. Use `cli.py` to send a `join` command across all nodes:

```bash
python3 cli.py -c join -m kornj2@fa24-cs425-5801.cs.illinois.edu kornj2@fa24-cs425-5802.cs.illinois.edu kornj2@fa24-cs425-5803.cs.illinois.edu kornj2@fa24-cs425-5804.cs.illinois.edu kornj2@fa24-cs425-5805.cs.illinois.edu kornj2@fa24-cs425-5806.cs.illinois.edu kornj2@fa24-cs425-5807.cs.illinois.edu kornj2@fa24-cs425-5808.cs.illinois.edu kornj2@fa24-cs425-5809.cs.illinois.edu kornj2@fa24-cs425-5810.cs.illinois.edu
```

### Command Examples

To issue commands, use `cli.py` with specific options. Here are a few examples:
