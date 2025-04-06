import json
import math
import random
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from concurrent.futures import ThreadPoolExecutor
import threading
import queue

from Subscription import Subscription

with open("input.json", "r") as f:
    config = json.load(f)

# TODO: read from cmd options
NUM_THREADS = 1

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

    write_output(bulk_publications, "a")

def generate_publications():
    publications_per_task = PUBLICATIONS//NUM_THREADS
    with ProcessPoolExecutor(max_workers=NUM_THREADS) as executor:
        # Submit tasks to the pool
        [
            executor.submit(generate_publication, publications_per_task)
            for i in range(NUM_THREADS)
        ]

        # Process completed tasks
        # for future in as_completed(futures):
        #     result = future.result()
        #     print(f">> {result}")


def write_output(messages, mode):
    with open("output.txt", mode) as file:
        for message in messages:
            file.write(f"{message}\n")


def add_field_to_subscription():
    popped_subscriptions = []
    while True:
        current_subscription = subscriptions.get()
        popped_subscriptions.append(current_subscription)
        with precise_field_number_lock:
            fields_from_map = set(precise_field_number.keys())
            fields_from_map = fields_from_map.difference(current_subscription.get_used_fields())
            if len(fields_from_map) == 0:
                # print("IN CONTINUE")
                continue
            
            max_field = min(fields_from_map, key=lambda k: precise_field_number[k], default=None)
            # random_field = random.choice(list(fields_from_map))
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
    for i in range(SUBSCRIPTIONS):
        subscriptions.put(Subscription())

    print(sum(precise_field_number.values()))
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [
            executor.submit(add_field_to_subscription)
            for _ in range(sum(precise_field_number.values()))
        ]

        for future in as_completed(futures):
            try:
                result = future.result()  # This will raise the exception if any
            except Exception as e:
                print(f"❌ Exception in thread: {e}")

    while not subscriptions.empty():
        subscriptions_list.append(subscriptions.get().values)


def main():
    generate_publications()
    # generate_subscriptions()
    # write_output(subscriptions_list)


if __name__ == "__main__":
    start_time = time.perf_counter()

    main()

    end_time = time.perf_counter()
    elapsed = end_time - start_time
    print(f"\n⏱️ Execution time: {elapsed:.4f} seconds")
