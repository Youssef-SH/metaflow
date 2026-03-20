from collections import Counter
from threading import Lock
import time

from metaflow.metadata_provider.metadata import active_tracer


class MetadataTracer:
    """
    Record semantic metadata queries with opt-in tracing via context manager.

    Each record stores provider, obj_type, sub_type, depth, attempt, and path.
    Total requests = len(self._records).

    Example
    -------
    with MetadataTracer() as tracer:
        ...
    tracer.summary()
    """

    def __init__(self):
        self._records = []
        self._lock = Lock()
        self._tokens = []

    def __enter__(self):
        self._tokens.append(active_tracer.set(self))
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        active_tracer.reset(self._tokens.pop())

    def _record(self, provider_type, obj_type, sub_type, depth, attempt, args):
        record = {
            "provider": provider_type,
            "obj_type": obj_type,
            "sub_type": sub_type,
            "depth": depth,
            "attempt": attempt,
            "path": "/".join(str(arg) for arg in args if arg),
            "timestamp": time.time(),
        }
        with self._lock:
            self._records.append(record)

    def summary(self):
        with self._lock:
            counts = Counter(record["obj_type"] for record in self._records)
            total = len(self._records)
        return {"total": total, "by_type": dict(counts)}

    def report(self):
        with self._lock:
            records = list(self._records)

        lines = []
        for record in records:
            lines.append(
                "[%s] provider=%s %s -> %s depth=%s attempt=%s path=%s"
                % (
                    time.strftime(
                        "%Y-%m-%d %H:%M:%S", time.localtime(record["timestamp"])
                    ),
                    record["provider"],
                    record["obj_type"],
                    record["sub_type"],
                    record["depth"],
                    record["attempt"],
                    record["path"],
                )
            )

        summary = self.summary()
        lines.append(
            "Total requests: %d (%s)"
            % (
                summary["total"],
                ", ".join(
                    "%s=%d" % (obj_type, count)
                    for obj_type, count in sorted(summary["by_type"].items())
                ),
            )
        )

        output = "\n".join(lines)
        print(output)
        return output
