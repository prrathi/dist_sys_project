import os
import sys
from time import sleep
import time
import argparse
import subprocess

FIFO = '/tmp/mp2'  # Named pipe path. The server should have already made this pipe when it starts (check if already exists)
import os
user = os.getenv("USER")
if user in ["praneet", "prathi3"]:
    FIFO = '/tmp/mp2-prathi3'

# the server should read from pipe and execute whatever command it recieves

def execute_local_command(command):
    ssh_command = f"echo {command} > {FIFO}"
    subprocess.run(ssh_command, shell=True, check=True)
    print(f"Sent {command} to local pipe")

def execute_remote_command(machines, command):

    for machine in machines:
        ssh_command = f"ssh {machine} 'echo {command} > {FIFO}'"
        subprocess.run(ssh_command, shell=True, check=True)
        if command == "join":
            sleep(2.5)
        print(f"Sent {command} to {machine} pipe")

def list_mem(machines):
    if machines[0] == "localhost":
        execute_local_command("list_mem")
        return
    execute_remote_command(machines, "list_mem")

def list_self(machines):
    if machines[0] == "localhost":
        execute_local_command("list_self")
        return
    execute_remote_command(machines, "list_self")

def join(machines):
    if machines[0] == "localhost":
        execute_local_command("join")
        return
    execute_remote_command(machines, "join")

def leave(machines):
    if machines[0] == "localhost":
        execute_local_command("leave")
        return
    execute_remote_command(machines, "leave")

def enable_sus(machines):
    if machines[0] == "localhost":
        execute_local_command("enable_sus")
        return
    execute_remote_command(machines, "enable_sus")

def disable_sus(machines):
    if machines[0] == "localhost":
        execute_local_command("disable_sus")
        return
    execute_remote_command(machines, "disable_sus")

def status_sus(machines):
    if machines[0] == "localhost":
        execute_local_command("status_sus")
        return
    execute_remote_command(machines, "status_sus")

def list_suspected(machines):
    if machines[0] == "localhost":
        execute_local_command("list_suspected")
        return
    execute_remote_command(machines, "list_suspected")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SWIM Protocol CLI")
    parser.add_argument("-c", "--command", choices=["list_mem", "list_self", "join", "leave", "enable_sus", "disable_sus", "status_sus", "list_suspected"])
    parser.add_argument("-m", "--machines", nargs="*")

    args = parser.parse_args()

    input = args.machines if args.machines and len(args.machines) > 0 else ["localhost"]

    if args.command == "join":
        join(input)
    elif args.command == "leave":
        leave(input)
    elif args.command == "list_mem":
        list_mem(input)
    elif args.command == "list_self":
        list_self(input)
    elif args.command == "enable_sus":
        enable_sus(input)
    elif args.command == "disable_sus":
        disable_sus(input)
    elif args.command == "status_sus":
        status_sus(input)
    elif args.command == "list_suspected":
        list_suspected(input)