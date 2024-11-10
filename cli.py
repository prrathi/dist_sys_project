import os
import sys
from time import sleep
import time
import argparse
import subprocess
import socket

FIFO = '/tmp/mp3'  # Named pipe path. The server should have already made this pipe when it starts (check if already exists)
import os
user = os.getenv("USER")
if user in ["praneet", "prathi3"]:
    FIFO = '/tmp/mp3-prathi3'

# the server should read from pipe and execute whatever command it recieves

def execute_local_command(command):
    print(command)
    ssh_command = f"echo {command} > {FIFO}"
    subprocess.run(ssh_command, shell=True, check=True)
    print(f"Sent {command} to local pipe")

def execute_remote_command(machines, command):
    for machine in machines:
        ssh_command = f"ssh {machine} 'echo {command} > {FIFO}'"
        subprocess.run(ssh_command, shell=True, check=True)
        print(f"Sent {command} to {machine} pipe")

def list_mem(machines):
    if machines[0] == "localhost" or machines[0] == socket.gethostname():
        execute_local_command("list_mem")
        return
    execute_remote_command(machines, "list_mem")

def list_self(machines):
    if machines[0] == "localhost" or machines[0] == socket.gethostname():
        execute_local_command("list_self")
        return
    execute_remote_command(machines, "list_self")

def join(machines):
    if machines[0] == "localhost" or machines[0] == socket.gethostname():
        execute_local_command("join")
        return
    execute_remote_command(machines, "join")

def leave(machines):
    if machines[0] == "localhost" or machines[0] == socket.gethostname():
        execute_local_command("leave")
        return
    execute_remote_command(machines, "leave")

def enable_sus(machines):
    if machines[0] == "localhost" or machines[0] == socket.gethostname():
        execute_local_command("enable_sus")
        return
    execute_remote_command(machines, "enable_sus")

def disable_sus(machines):
    if machines[0] == "localhost" or machines[0] == socket.gethostname():
        execute_local_command("disable_sus")
        return
    execute_remote_command(machines, "disable_sus")

def status_sus(machines):
    if machines[0] == "localhost" or machines[0] == socket.gethostname():
        execute_local_command("status_sus")
        return
    execute_remote_command(machines, "status_sus")

def list_suspected(machines):
    if machines[0] == "localhost" or machines[0] == socket.gethostname():
        execute_local_command("list_suspected")
        return
    execute_remote_command(machines, "list_suspected")


# MP3

def createfile(machines, args):
    if machines[0] == "localhost" or machines[0] == socket.gethostname():
        execute_local_command("create " + args)
        return
    execute_remote_command(machines, "create " + args)

def getfile(machines, args):
    if machines[0] == "localhost" or machines[0] == socket.gethostname():
        execute_local_command("get " + args)
        return
    execute_remote_command(machines, "get " + args)

def appendfile(machines, args):
    if machines[0] == "localhost" or machines[0] == socket.gethostname():
        execute_local_command("append " + args)
        return
    execute_remote_command(machines, "append " + args)

def mergefile(machines, args):
    if machines[0] == "localhost" or machines[0] == socket.gethostname():
        execute_local_command("merge " + args)
        return
    execute_remote_command(machines, "merge " + args)

def listfiles(machines, args):
    if machines[0] == "localhost" or machines[0] == socket.gethostname():
        execute_local_command("ls " + args)
        return
    execute_remote_command(machines, "ls " + args)

def storefile(machines):
    if machines[0] == "localhost" or machines[0] == socket.gethostname():
        execute_local_command("store")
        return
    execute_remote_command(machines, "store")

def getfromreplica(machines, args):
    if machines[0] == "localhost" or machines[0] == socket.gethostname():
        execute_local_command("getfromreplica " + args)
        return
    execute_remote_command(machines, "getfromreplica " + args)

def list_mem_ids(machines):
    if machines[0] == "localhost" or machines[0] == socket.gethostname():
        execute_local_command("list_mem_ids")
        return
    execute_remote_command(machines, "list_mem_ids")


# use like: -c multiappend hydfsfilename,vm1,vm2,localfilename1,localfilename2    -> assuming the args are comma separated in this case
def multiappend(machines, args):
    print("Executing multiappend")
    filename, arguments = args.split(",")[0], args.split(",")[1:]
    vm_list, command_list = arguments[:len(arguments)//2], arguments[len(arguments)//2:]
    # print(filename)
    # print(vm_list)
    # print(command_list)
    for vm, command in zip(vm_list, command_list):
        appendfile([vm], command)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SWIM Protocol CLI")
    parser.add_argument("-c", "--command", choices=["list_mem", "list_self", "join", "leave", "enable_sus"
                                                    , "disable_sus", "status_sus", "list_suspected", "create", 
                                                    "get", "append", "merge", "ls", "store", "getfromreplica", "list_mem_ids", "multiappend"])
    parser.add_argument("-m", "--machines", nargs="*")
    parser.add_argument("remaining_args", nargs="*", help="Arguments for the command")

    args = parser.parse_args()
    extra_args = args.remaining_args

    machine_input = args.machines if args.machines and len(args.machines) > 0 else ["localhost"]
    print(args.command)
    if args.command == "join":
        join(machine_input)
    elif args.command == "leave":
        leave(machine_input)
    elif args.command == "list_mem":
        list_mem(machine_input)
    elif args.command == "list_self":
        list_self(machine_input)
    elif args.command == "enable_sus":
        enable_sus(machine_input)
    elif args.command == "disable_sus":
        disable_sus(machine_input)
    elif args.command == "status_sus":
        status_sus(machine_input)
    elif args.command == "list_suspected":
        list_suspected(machine_input)
    elif args.command == "create":
        createfile(machine_input, " ".join(extra_args)) # create localfilename HyDFSfilename
    elif args.command == "get":
        getfile(machine_input, " ".join(extra_args)) # get HyDFSfilename localfilename
    elif args.command == "append":
        appendfile(machine_input, " ".join(extra_args)) # append localfilename HyDFSfilename 
    elif args.command == "merge":
        mergefile(machine_input, " ".join(extra_args)) # merge HyDFSfilename 
    elif args.command == "ls":
        listfiles(machine_input, " ".join(extra_args)) # ls HyDFSfilename
    elif args.command == "store":
        storefile(machine_input) # store
    elif args.command == "getfromreplica":
        getfromreplica(machine_input, " ".join(extra_args)) # getfromreplica VMaddress HyDFSfilename localfilename
    elif args.command == "list_mem_ids":
        list_mem_ids(machine_input) # list_mem_ids
    elif args.command == "multiappend":
        multiappend(machine_input, " ".join(extra_args)) # use like: -c multiappend hydfsfilename,vm1,vm2,localfilename1,localfilename2
        # multiappend(filename, VMi, … VMj, localfilenamei,....localfilenamej) - launches appends from VMi… VMj simultaneously to the filename. VMi appends localfilenamei
    else:
        print("Invalid command")