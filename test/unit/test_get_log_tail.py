import builtins
import os
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

from metaflow.mflog import TASK_LOG_SOURCE, mflog
import metaflow.plugins.agent.logs as logs


class FakeDatastore:
    def __init__(self, root_dir, datastore_type="local"):
        self.TYPE = datastore_type
        self._attempt = 3
        self._path = "F/R/S/T"
        self._storage_impl = SimpleNamespace(
            TYPE=datastore_type,
            path_join=lambda *parts: "/".join(parts),
            full_uri=lambda path: os.path.join(root_dir, path),
        )

    @staticmethod
    def _get_log_location(log_prefix, stream):
        return "%s_%s.log" % (log_prefix, stream)

    @staticmethod
    def _metadata_name_for_attempt(name, attempt_override=None):
        return "%s.%s" % (attempt_override, name)


class FakeTask:
    def __init__(self, datastore):
        self._datastore = datastore
        self.current_attempt = 3


def _make_log_dir(payload, stream="stderr"):
    temp_dir = tempfile.TemporaryDirectory()
    log_path = os.path.join(
        temp_dir.name,
        "F/R/S/T/3.%s_%s.log" % (TASK_LOG_SOURCE, stream),
    )
    os.makedirs(os.path.dirname(log_path))
    with open(log_path, "wb") as log_file:
        log_file.write(payload)
    return temp_dir


def test_reads_only_max_bytes_from_local_file():
    temp_dir = _make_log_dir(b"a\nb\nc\nd\n")
    task = FakeTask(FakeDatastore(temp_dir.name))
    read_sizes = []
    real_open = builtins.open

    class TrackingFile:
        def __init__(self, wrapped_file):
            self._wrapped_file = wrapped_file

        def seek(self, *args):
            return self._wrapped_file.seek(*args)

        def tell(self):
            return self._wrapped_file.tell()

        def read(self, size=-1):
            read_sizes.append(size)
            return self._wrapped_file.read(size)

        def __enter__(self):
            self._wrapped_file.__enter__()
            return self

        def __exit__(self, *args):
            return self._wrapped_file.__exit__(*args)

    try:
        with patch.object(
            logs,
            "open",
            lambda *args, **kwargs: TrackingFile(real_open(*args, **kwargs)),
            create=True,
        ):
            logs.get_log_tail(task, stream="stderr", n_lines=2, max_bytes=6)
        assert read_sizes == [6]
    finally:
        temp_dir.cleanup()


def test_returns_last_n_lines_for_stderr():
    temp_dir = _make_log_dir(b"a\nb\nc\nd\n")
    task = FakeTask(FakeDatastore(temp_dir.name))

    try:
        result = logs.get_log_tail(task, stream="stderr", n_lines=2, max_bytes=6)
        assert result == "c\nd"
    finally:
        temp_dir.cleanup()


def test_returns_full_small_stdout_file():
    temp_dir = _make_log_dir(b"x\ny\n", stream="stdout")
    task = FakeTask(FakeDatastore(temp_dir.name))

    try:
        result = logs.get_log_tail(task, stream="stdout", n_lines=5, max_bytes=99)
        assert result == "x\ny"
    finally:
        temp_dir.cleanup()


def test_decodes_mflog_lines():
    payload = (
        mflog.decorate("task", "one") + b"\n" + mflog.decorate("task", "two") + b"\n"
    )
    temp_dir = _make_log_dir(payload)
    task = FakeTask(FakeDatastore(temp_dir.name))

    try:
        result = logs.get_log_tail(task, n_lines=1)
        assert result == "two"
    finally:
        temp_dir.cleanup()


def test_drops_partial_first_line_when_read_starts_mid_file():
    temp_dir = _make_log_dir(b"abcdef\ncomplete\n")
    task = FakeTask(FakeDatastore(temp_dir.name))

    try:
        result = logs.get_log_tail(task, n_lines=5, max_bytes=10)
        assert result == "complete"
    finally:
        temp_dir.cleanup()


def test_returns_empty_string_for_missing_file():
    temp_dir = tempfile.TemporaryDirectory()
    task = FakeTask(FakeDatastore(temp_dir.name))

    try:
        result = logs.get_log_tail(task, stream="stdout")
        assert result == ""
    finally:
        temp_dir.cleanup()


def test_constructs_attempt_specific_log_path():
    datastore = FakeDatastore("/tmp/root")

    log_path = datastore._storage_impl.full_uri(
        datastore._storage_impl.path_join(
            datastore._path,
            datastore._metadata_name_for_attempt(
                datastore._get_log_location(TASK_LOG_SOURCE, "stderr"),
                attempt_override=3,
            ),
        )
    )

    assert log_path.endswith("3.%s_stderr.log" % TASK_LOG_SOURCE)


def test_raises_for_invalid_stream():
    task = FakeTask(FakeDatastore("/tmp/root"))

    try:
        logs.get_log_tail(task, stream="logs")
        assert False, "Expected ValueError"
    except ValueError as err:
        assert "Expected 'stderr' or 'stdout'" in str(err)


def test_raises_for_non_local_datastore():
    task = FakeTask(FakeDatastore("/tmp/root", datastore_type="s3"))

    try:
        logs.get_log_tail(task)
        assert False, "Expected NotImplementedError"
    except NotImplementedError as err:
        assert "range-read integration" in str(err)
