import queue
import itertools


class FIFOPriorityQueue:
    """
    A thread-safe priority queue that uses FIFO (First-In, First-Out) behavior
    for items with equal priority. Lower priority values indicate higher priority.

    This implementation uses Python's built-in queue.PriorityQueue which is already thread-safe.
    """

    def __init__(self):
        """Initialize an empty thread-safe FIFO priority queue."""
        self._queue = queue.PriorityQueue()
        # Counter to maintain FIFO order for items with same priority
        self._counter = itertools.count()

    def is_empty(self):
        """Return True if the queue is empty, False otherwise."""
        return self._queue.empty()

    def size(self):
        """Return the number of items in the queue."""
        return self._queue.qsize()

    def push(self, item, priority):
        """
        Add an item to the queue with the given priority.

        Args:
            item: The item to add to the queue
            priority: The priority of the item (lower values indicate higher priority)
        """
        # Get the next sequence number to ensure FIFO ordering of equal priority items
        count = next(self._counter)

        # Add the item to the queue with the priority and count
        # The count ensures FIFO order for items with the same priority
        self._queue.put((priority, count, item))

    def pop(self):
        """
        Remove and return the highest priority item that was inserted earliest.
        Raises queue.Empty if the queue is empty.

        Returns:
            The item with the highest priority that was added earliest
        """
        try:
            # Get the highest priority item (queue.PriorityQueue already sorts by first element)
            priority, count, item = self._queue.get(block=False)
            return item
        except queue.Empty:
            raise IndexError("pop from an empty FIFO priority queue")

    def peek(self):
        """
        Return the highest priority item that was inserted earliest without removing it.
        Raises IndexError if the queue is empty.

        Note: This operation is not standard for queue.PriorityQueue and requires
        temporary removal and reinsertion, which can affect thread safety if used carelessly.
        """
        try:
            # Get the item (temporarily removing it)
            priority, count, item = self._queue.get(block=False)

            # Put it back
            self._queue.put((priority, count, item))

            return item
        except queue.Empty:
            raise IndexError("peek from an empty FIFO priority queue")
