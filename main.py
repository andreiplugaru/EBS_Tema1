import argparse
import ast
import json
import math
import os
import random
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from concurrent.futures import ThreadPoolExecutor
import threading

from filelock import FileLock

from FIFOPriorityQueue import FIFOPriorityQueue
from Subscription import Subscription

with open("input.json", "r") as f:
    config = json.load(f)

# Set up the argument parser
parser = argparse.ArgumentParser(description="Process a thread number parameter.")
parser.add_argument('--threads', type=int, help='Number of threads', required=True)

# Parse the arguments
args = parser.parse_args()

# Access the thread number
NUM_THREADS = args.threads

numbers = config["numbers"]
structure = config["structure"]
PUBLICATIONS = numbers["publications"]
SUBSCRIPTIONS = numbers["subscriptions"]

FIELD_WEIGHTS = numbers.get("field_weights", {})
precise_field_number = {}
precise_field_number_lock = threading.Lock()
for field in FIELD_WEIGHTS.keys():
    precise_field_number[field] = round(SUBSCRIPTIONS * FIELD_WEIGHTS[field])
    if precise_field_number[field] == 0:
        del precise_field_number[field]

EQUALITY_WEIGHTS = numbers.get("equality_weights", {})
precise_field_equality_number = {}
precise_field_equality_number_lock = threading.Lock()
for field in EQUALITY_WEIGHTS.keys():
    precise_field_equality_number[field] = math.ceil(precise_field_number[field] * EQUALITY_WEIGHTS[field])

FIELD_STRUCTURE = {}
for field, details in structure.items():
    FIELD_STRUCTURE[field] = {
        "is_interval": details["is_interval"],
        "values": details["values"],
        "operators": details["operators"]
    }

PUBLICATIONS_OUTPUT_FILE="publications.txt"
PUBLICATIONS_OUTPUT_LOCK_FILE="publications.lock"
publication_file_lock = FileLock(PUBLICATIONS_OUTPUT_LOCK_FILE)


SUBSCRIPTIONS_OUTPUT_FILE="subscriptions.txt"
CHECK_OUTPUT_FILE="check-output.txt"

subscriptions = FIFOPriorityQueue()
subscriptions_list = []


def generate_random_value_for_field(field_name):
    global FIELD_STRUCTURE
    interval = FIELD_STRUCTURE[field_name]["values"]
    if FIELD_STRUCTURE[field_name]["is_interval"] == True:
        if field_name == "rain":
            return random.uniform(interval[0], interval[1])
        else:
            return random.randint(interval[0], interval[1])
    else:
        return random.choice(interval)


def generate_publication(num_publications = 1):
    bulk_publications = []
    for _ in range(num_publications):
        current_publication = []
        for field_name in FIELD_STRUCTURE.keys():
            value = generate_random_value_for_field(field_name)
            current_publication.append({field_name: value})
        bulk_publications.append(current_publication)

    write_output(bulk_publications, PUBLICATIONS_OUTPUT_FILE, "a")

def generate_publications():
    start_time = time.perf_counter()

    publications_per_task = PUBLICATIONS // NUM_THREADS
    remainder = PUBLICATIONS % NUM_THREADS

    try:
        os.remove(PUBLICATIONS_OUTPUT_FILE)
    except FileNotFoundError as e:
        pass

    with ProcessPoolExecutor(max_workers=NUM_THREADS) as executor:
        # Submit tasks to the pool
        for i in range(NUM_THREADS):
            if i == NUM_THREADS - 1:
                publications_per_task += remainder
            
            executor.submit(generate_publication, publications_per_task)
    
    end_time = time.perf_counter()
    elapsed = end_time - start_time
    print(f"\nExecution time for generating publications: {elapsed:.4f} seconds")

def add_field_to_subscription(batch_size):
    for _ in range(batch_size):
        popped_subscriptions = []
        current_subscription = subscriptions.pop()
        popped_subscriptions.append(current_subscription)
        with precise_field_number_lock:
            fields_from_map = set(precise_field_number.keys())
            fields_from_map = fields_from_map.difference(current_subscription.get_used_fields())

            min_field = min(fields_from_map, key=lambda k: precise_field_number[k], default=None)
            if precise_field_number[min_field] == 1:
                del precise_field_number[min_field]
            else:
                precise_field_number[min_field] -= 1

        with precise_field_equality_number_lock:
            if min_field in precise_field_equality_number:
                operator = "="

                if precise_field_equality_number[min_field] == 1:
                    del precise_field_equality_number[min_field]
                else:
                    precise_field_equality_number[min_field] -= 1

            else:
                operator = random.choice(FIELD_STRUCTURE[min_field]["operators"])

        current_subscription.add_value((min_field, operator, generate_random_value_for_field(min_field)))

        subscriptions.push(current_subscription, current_subscription.get_length())


def generate_subscriptions():
    start_time = time.perf_counter()

    for _ in range(SUBSCRIPTIONS):
        subscriptions.push(Subscription(), 0)

    fields_per_task = sum(precise_field_number.values()) // NUM_THREADS
    remainder = sum(precise_field_number.values()) % NUM_THREADS

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = []
        for i in range(NUM_THREADS):
            if i == NUM_THREADS - 1:
                fields_per_task += remainder

            futures.append(executor.submit(add_field_to_subscription, fields_per_task))

        for future in as_completed(futures):
            try:
                future.result()  
            except Exception as e:
                print(f"Exception in thread: {e}")

    while not subscriptions.is_empty():
        subscriptions_list.append(subscriptions.pop().values)
    
    write_output(subscriptions_list, SUBSCRIPTIONS_OUTPUT_FILE)

    end_time = time.perf_counter()
    elapsed = end_time - start_time
    print(f"\nExecution time for generating subscriptions: {elapsed:.4f} seconds")


def write_output(messages, file_path, mode="w"):
    
    if (file_path == PUBLICATIONS_OUTPUT_FILE):
        
        with publication_file_lock:
            try:
                with open(file_path, mode) as file:
                    for message in messages:
                        file.write(f"{message}\n")
            except Exception as e:
                print(f"Error writing to file {file_path}: {e}")

            
    else:
        with open(file_path, mode) as file:
            for message in messages:
                file.write(f"{message}\n")

def check_output():

    generated_publications = 0
    with open(PUBLICATIONS_OUTPUT_FILE, "r") as file:
        for line in file.readlines():
            generated_publications += 1

    for field in FIELD_WEIGHTS.keys():
        precise_field_number[field] = round(SUBSCRIPTIONS * FIELD_WEIGHTS[field])
    for field in EQUALITY_WEIGHTS.keys():
        precise_field_equality_number[field] = math.ceil(precise_field_number[field] * EQUALITY_WEIGHTS[field])

    generated_subscriptions = 0    
    restricted_fields = {key: 0 for key in precise_field_number}
    restricted_equality_fields = {key: 0 for key in precise_field_equality_number}

    with open(SUBSCRIPTIONS_OUTPUT_FILE, "r") as file:
        for line in file.readlines():
            generated_subscriptions += 1
            try:
                tuple_list = ast.literal_eval(line.strip())
            except (SyntaxError, ValueError):
                continue 
            for item in tuple_list:
                key = item[0]
                operator = item[1]
                if key in restricted_fields:
                    restricted_fields[key] += 1
                if key in restricted_equality_fields and operator == "=":
                    restricted_equality_fields[key] += 1
                
    
   
    with open(CHECK_OUTPUT_FILE, "w") as file:
        file.write(f"Expected Publications: {PUBLICATIONS}\n")    
        file.write(f"Generated Publications: {generated_publications}\n")
        
        file.write(f"Expected subscriptions: {SUBSCRIPTIONS}\n")
        file.write(f"generated_subscriptions: {generated_subscriptions}\n")
        for key, value in restricted_fields.items():
            file.write(f"Field {key} expected times : {precise_field_number[key]}\n")
            file.write(f"Field {key} generated times : {value} \n")
        for key, value in restricted_equality_fields.items():
            file.write(f"Field {key} expected at least  equality times : {precise_field_equality_number[key]}\n")
            file.write(f"Field {key} generated equality times : {value} \n")

     

def main():
    generate_publications()
    generate_subscriptions()
    check_output()



if __name__ == "__main__":
    main()

