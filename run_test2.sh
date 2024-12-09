#!/bin/bash


# Assuming that all vms are already running Rainstorm
# Run this script on master server (prathi3@fa24-cs425-5801.cs.illinois.edu)
# Modify for the right csv file 

# join cluster
echo "Step 1: Joining the RainStorm cluster..."
python3 cli.py -c join -m prathi3@fa24-cs425-5801.cs.illinois.edu \
                          prathi3@fa24-cs425-5802.cs.illinois.edu \
                          prathi3@fa24-cs425-5803.cs.illinois.edu \
                          prathi3@fa24-cs425-5804.cs.illinois.edu \
                          prathi3@fa24-cs425-5805.cs.illinois.edu \
                          prathi3@fa24-cs425-5806.cs.illinois.edu \
                          prathi3@fa24-cs425-5807.cs.illinois.edu \
                          prathi3@fa24-cs425-5808.cs.illinois.edu \
                          prathi3@fa24-cs425-5809.cs.illinois.edu \
                          prathi3@fa24-cs425-5810.cs.illinois.edu
echo "Cluster joined successfully."

# store dataset into hydfs
python3 cli.py -c create datasets/TrafficSigns_1000.csv TrafficSigns_1000
echo "Dataset TrafficSigns_1000 created successfully."

# start rainstorm job
echo "Step 3: Starting RainStorm application..."
python3 cli.py -c rainstorm -r ./datasets/test1_1 ./datasets/test1_2 TrafficSigns_1000 TrafficSigns_1000_Rainstorm 3 

# Wait for 1.5 seconds
sleep 1.5

# kill stage 1 and stage 2 tasks

# Define the target VMs
TARGET_VMS=("prathi3@fa24-cs425-5806.cs.illinois.edu" "prathi3@fa24-cs425-5809.cs.illinois.edu")

# Loop through each target VM and kill the 'rainstorm' process
for VM in "${TARGET_VMS[@]}"
do
    echo "Connecting to $VM to kill 'Rainstorm' process"
    ssh "$VM" "pkill -f Rainstorm" && echo "Successfully killed 'rainstorm' on $VM." || echo "Failed to kill 'rainstorm' on $VM."
done

echo "Failures introduced successfully."