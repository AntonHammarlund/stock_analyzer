from typing import Protocol


class DataSource(Protocol):
    name: str

    def is_available(self) -> bool:
        ...

    def fetch(self):
        ...
