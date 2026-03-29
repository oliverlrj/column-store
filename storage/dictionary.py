"""
dictionary.py — Dictionary encoder for string columns.

Converts strings to compact integer IDs before storage, and back when reading.

Two modes:
  - Closed-set: pre-populate with a known list (town, flat_type, etc.)
                IDs are stable and order-matched to the list.
  - Open-set:   start empty and auto-assign IDs as new strings appear
                (block, street_name).
"""

import json


class Dictionary:
    """
    Two-way mapping between string values and integer IDs.

      encode("PASIR RIS") -> 6
      decode(6)           -> "PASIR RIS"
    """

    def __init__(self, prepopulated: list[str] = None):
        self._str_to_id: dict[str, int] = {}
        self._id_to_str: list[str] = []

        if prepopulated:
            for value in prepopulated:
                self._assign(value)

    def _assign(self, value: str) -> int:
        """Assign the next available ID to a new string value."""
        new_id = len(self._id_to_str)
        self._str_to_id[value] = new_id
        self._id_to_str.append(value)
        return new_id

    def encode(self, value: str) -> int:
        """Return the integer ID for a string, auto-assigning if unseen."""
        if value not in self._str_to_id:
            return self._assign(value)
        return self._str_to_id[value]

    def decode(self, id: int) -> str:
        """Return the original string for an integer ID."""
        return self._id_to_str[id]

    def save(self, path: str) -> None:
        """Write the dictionary to a JSON array file (index == ID)."""
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self._id_to_str, f, ensure_ascii=False)

    def load(self, path: str) -> None:
        """Load a previously saved dictionary JSON file."""
        with open(path, "r", encoding="utf-8") as f:
            values = json.load(f)
        self._id_to_str = values
        self._str_to_id = {v: i for i, v in enumerate(values)}
