class Registry:
    def __init__(self) -> None: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...
    def encode(self) -> bytes: ...

class Histogram:
    def __init__(
        self,
        name: str,
        documentation: str,
        buckets: list[float],
        registry: Registry | None = None,
    ) -> None: ...
    def observe(
        self,
        labels: dict[str, str] | list[tuple[str, str]],
        value: float,
    ) -> None: ...

class Counter:
    def __init__(
        self,
        name: str,
        documentation: str,
        registry: Registry | None = None,
    ) -> None: ...
    def inc(
        self,
        lables: dict[str, str] | list[tuple[str, str]],
    ) -> int: ...

class Gauge:
    def __init__(
        self,
        name: str,
        documentation: str,
        registry: Registry | None = None,
    ) -> None: ...
    def set(
        self,
        lables: dict[str, str] | list[tuple[str, str]],
        value: int,
    ) -> int: ...

def init_tracing(level: str) -> None: ...
def encode_global_registry() -> bytes: ...
