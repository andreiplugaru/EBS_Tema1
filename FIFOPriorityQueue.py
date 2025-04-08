import queue
import itertools


class FIFOPriorityQueue:
    """
    A thread-safe priority queue that uses FIFO (First-In, First-Out) behavior
    for items with equal priority. Lower priority values indicate higher priority.

    This implementation uses Python's built-in queue.PriorityQueue which is already thread-safe.
    """

    def __init__(self):
        self._queue = queue.PriorityQueue()
        self._counter = itertools.count()

    def is_empty(self):
        return self._queue.empty()

    def size(self):
        return self._queue.qsize()

    def push(self, item, priority):
        count = next(self._counter)

        self._queue.put((priority, count, item))

    def pop(self):
        try:
            _, _, item = self._queue.get(block=False)
            return item
        except queue.Empty:
            raise IndexError("pop from an empty FIFO priority queue")

    def peek(self):
        try:
            priority, count, item = self._queue.get(block=False)

            self._queue.put((priority, count, item))

            return item
        except queue.Empty:
            raise IndexError("peek from an empty FIFO priority queue")
