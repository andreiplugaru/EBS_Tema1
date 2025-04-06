from dataclasses import dataclass, field
from typing import Any


class Subscription:
    def __init__(self):
        # list of tuples(field, operator, value)
        self.values = []

    def add_value(self, value):
        self.values.append(value)

    def get_length(self):
        return len(self.values)

    def get_used_fields(self):
        return set([value[0] for value in self.values])

    def __gt__(self, other):
        return self.get_length() > other.get_length()

    def __lt__(self, other):
        return self.get_length() < other.get_length()

