#!/bin/bash

# Assuming this is run after the first run_test2.sh
# Assuming that all vms are already running Rainstorm 
# Run this script on master server (prathi3@fa24-cs425-5801.cs.illinois.edu)
# Modify for the right csv file 


# start crashed ones again
TARGET_VMS=(
    "prathi3@fa24-cs425-5806.cs.illinois.edu"
    "prathi3@fa24-cs425-5809.cs.illinois.edu"
)

# path to executable make sure right
RAINSTORM_PATH="~/cs425MPs/g58_mp4/build/Rainstorm"

# Iterate over each target VM and start RainStorm
for VM in "${TARGET_VMS[@]}"; do
    ssh "$VM" "nohup $RAINSTORM_PATH"
done

sleep 0.5

# join cluster
echo "Step 1: Joining the RainStorm cluster..."
python3 cli.py -c join -m prathi3@fa24-cs425-5806.cs.illinois.edu prathi3@fa24-cs425-5809.cs.illinois.edu 
echo "Cluster joined successfully."

# stabilize after joining
sleep 30

# store dataset into hydfs
python3 cli.py -c create datasets/TrafficSigns_1000.csv TrafficSigns_1000
echo "Dataset TrafficSigns_1000 created successfully."

sleep 0.5

# start rainstorm job
echo "Step 3: Starting RainStorm application..."
python3 cli.py -c rainstorm -r ./datasets/test2_1 ./datasets/test2_2 TrafficSigns_1000 TrafficSigns_1000_Rainstorm 3 

# Wait for 1.5 seconds
sleep 1.5