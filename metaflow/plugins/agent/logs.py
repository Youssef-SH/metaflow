from __future__ import annotations

import os
from typing import TYPE_CHECKING

from metaflow.client.filecache import FileCache
from metaflow.mflog import TASK_LOG_SOURCE, mflog

if TYPE_CHECKING:
    from metaflow.client.core import Task


def get_log_tail(
    task: "Task",
    stream: str = "stderr",
    n_lines: int = 50,
    max_bytes: int = 65536,
) -> str:
    if stream not in ("stderr", "stdout"):
        raise ValueError("Invalid stream '%s'. Expected 'stderr' or 'stdout'." % stream)
    if n_lines <= 0:
        return ""
    if max_bytes <= 0:
        return ""

    task_datastore = getattr(task, "_datastore", None)
    if task_datastore is None:
        task_datastore = _resolve_task_datastore(task)

    datastore_type = getattr(task_datastore, "TYPE", None) or getattr(
        task_datastore._storage_impl, "TYPE", None
    )
    if datastore_type != "local":
        raise NotImplementedError(
            "get_log_tail currently supports only local datastore backends. "
            "Cloud datastore support (S3/Azure/GCS) requires range-read integration."
        )

    log_path = _build_log_path(task, task_datastore, stream)
    log_blob, read_started_mid_file = _read_local_tail(log_path, max_bytes)
    return _decode_log_tail(log_blob, n_lines, read_started_mid_file)


def _resolve_task_datastore(task: "Task"):
    metadata = task.metadata_dict
    file_cache = FileCache()
    return file_cache._get_task_datastore(
        metadata["ds-type"],
        metadata["ds-root"],
        *task.path_components,
        task.current_attempt,
    )


def _build_log_path(task: "Task", task_datastore, stream: str) -> str:
    attempt = getattr(task, "current_attempt", None) or getattr(
        task_datastore, "_attempt", None
    )
    log_name = task_datastore._get_log_location(TASK_LOG_SOURCE, stream)
    metadata_name = task_datastore._metadata_name_for_attempt(
        log_name,
        attempt_override=attempt,
    )
    datastore_path = task_datastore._storage_impl.path_join(
        task_datastore._path,
        metadata_name,
    )
    return task_datastore._storage_impl.full_uri(datastore_path)


def _read_local_tail(log_path: str, max_bytes: int):
    try:
        with open(log_path, "rb") as log_file:
            # Read only the tail window to avoid loading the full log into memory.
            log_file.seek(0, os.SEEK_END)
            file_size = log_file.tell()
            read_offset = max(0, file_size - max_bytes)
            log_file.seek(read_offset)
            return log_file.read(max_bytes), read_offset > 0
    except OSError:
        return b"", False


def _decode_log_tail(log_blob: bytes, n_lines: int, read_started_mid_file: bool) -> str:
    log_lines = log_blob.splitlines()
    if read_started_mid_file and log_lines:
        log_lines = log_lines[1:]

    decoded_lines = []
    for line in log_lines[-n_lines:]:
        parsed_line = mflog.parse(line)
        raw_line = parsed_line.msg if parsed_line else line
        decoded_lines.append(raw_line.decode("utf-8", errors="replace").rstrip("\n"))
    return "\n".join(decoded_lines)
