import argparse
import json
import math
import os
import random
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from concurrent.futures import ThreadPoolExecutor
import threading
import queue

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
SUBSCRIPTIONS_OUTPUT_FILE="subscriptions.txt"

subscriptions = queue.PriorityQueue()
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
        while True:
            current_subscription = subscriptions.get()
            popped_subscriptions.append(current_subscription)
            with precise_field_number_lock:
                fields_from_map = set(precise_field_number.keys())
                fields_from_map = fields_from_map.difference(current_subscription.get_used_fields())
                if len(fields_from_map) == 0:
                    continue
                
                max_field = min(fields_from_map, key=lambda k: precise_field_number[k], default=None)
                if precise_field_number[max_field] == 1:
                    del precise_field_number[max_field]
                else:
                    precise_field_number[max_field] -= 1

            with precise_field_equality_number_lock:
                if max_field in precise_field_equality_number:
                    operator = "="

                    if precise_field_equality_number[max_field] == 1:
                        del precise_field_equality_number[max_field]
                    else:
                        precise_field_equality_number[max_field] -= 1

                else:
                    operator = random.choice(FIELD_STRUCTURE[max_field]["operators"])

            current_subscription.add_value((max_field, operator, generate_random_value_for_field(max_field)))

            break

        [subscriptions.put(subscription) for subscription in popped_subscriptions]


def generate_subscriptions():
    start_time = time.perf_counter()

    for _ in range(SUBSCRIPTIONS):
        subscriptions.put(Subscription())

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [
            executor.submit(add_field_to_subscription, sum(precise_field_number.values()) // NUM_THREADS)
            for _ in range(NUM_THREADS)
        ]

        for future in as_completed(futures):
            try:
                future.result()  
            except Exception as e:
                print(f"Exception in thread: {e}")

    while not subscriptions.empty():
        subscriptions_list.append(subscriptions.get().values)
    
    write_output(subscriptions_list, SUBSCRIPTIONS_OUTPUT_FILE)

    end_time = time.perf_counter()
    elapsed = end_time - start_time
    print(f"\nExecution time for generating subscriptions: {elapsed:.4f} seconds")


def write_output(messages, file_path, mode="w"):
    with open(file_path, mode) as file:
        for message in messages:
            file.write(f"{message}\n")

def main():
    generate_publications()
    generate_subscriptions()



if __name__ == "__main__":
    main()

