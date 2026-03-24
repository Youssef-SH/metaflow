from metaflow.plugins.agent.failures import find_failed_tasks


class FakeArtifact(object):
    @property
    def data(self):
        raise AssertionError("artifact data should not be accessed")


class FakeTask(object):
    def __init__(self, task_id, has_success=True):
        self.id = task_id
        self.has_success = has_success
        self.getitem_calls = []

    @property
    def successful(self):
        raise AssertionError("task.successful should not be accessed")

    def __getitem__(self, name):
        self.getitem_calls.append(name)
        if name != "_success":
            raise KeyError(name)
        if not self.has_success:
            raise KeyError(name)
        return FakeArtifact()


class FakeStep(object):
    def __init__(self, name, tasks):
        self.id = name
        self._tasks = list(tasks)

    def __iter__(self):
        return iter(self._tasks)


class FakeRun(object):
    def __init__(self, steps):
        self._steps = list(steps)
        self._steps_by_name = {step.id: step for step in self._steps}

    def __iter__(self):
        return iter(self._steps)

    def __getitem__(self, step_name):
        return self._steps_by_name[step_name]


def test_finds_failed_tasks_when_success_descriptor_missing():
    failed = FakeTask("failed", has_success=False)
    run = FakeRun([FakeStep("train", [failed])])

    assert find_failed_tasks(run) == [failed]


def test_does_not_call_task_successful():
    run = FakeRun([FakeStep("train", [FakeTask("failed", has_success=False)])])

    failed_tasks = find_failed_tasks(run)

    assert [task.id for task in failed_tasks] == ["failed"]


def test_does_not_access_artifact_data():
    successful = FakeTask("ok", has_success=True)
    run = FakeRun([FakeStep("train", [successful])])

    assert find_failed_tasks(run) == []
    assert successful.getitem_calls == ["_success"]


def test_respects_stop_after():
    tasks = [
        FakeTask("failed-1", has_success=False),
        FakeTask("failed-2", has_success=False),
        FakeTask("failed-3", has_success=False),
    ]
    run = FakeRun([FakeStep("train", tasks)])

    failed_tasks = find_failed_tasks(run, stop_after=2)

    assert [task.id for task in failed_tasks] == ["failed-1", "failed-2"]
    assert tasks[2].getitem_calls == []


def test_respects_max_tasks():
    tasks = [
        FakeTask("ok-1", has_success=True),
        FakeTask("ok-2", has_success=True),
        FakeTask("failed-3", has_success=False),
    ]
    run = FakeRun([FakeStep("train", tasks)])

    failed_tasks = find_failed_tasks(run, max_tasks=2, stop_after=1)

    assert failed_tasks == []
    assert tasks[0].getitem_calls == ["_success"]
    assert tasks[1].getitem_calls == ["_success"]
    assert tasks[2].getitem_calls == []


def test_respects_step_filter():
    skipped_failed = FakeTask("skipped-failed", has_success=False)
    selected_failed = FakeTask("selected-failed", has_success=False)
    run = FakeRun(
        [
            FakeStep("start", [skipped_failed]),
            FakeStep("train", [selected_failed]),
        ]
    )

    failed_tasks = find_failed_tasks(run, step="train")

    assert failed_tasks == [selected_failed]
    assert skipped_failed.getitem_calls == []


def test_returns_empty_list_when_no_failed_tasks_are_found():
    run = FakeRun(
        [FakeStep("train", [FakeTask("ok-1", has_success=True), FakeTask("ok-2")])]
    )

    assert find_failed_tasks(run) == []
