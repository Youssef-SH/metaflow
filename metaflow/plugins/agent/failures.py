from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from metaflow.client.core import Run, Step, Task


def _iter_steps(run: "Run", step):
    if step is None:
        return list(run)
    if isinstance(step, str):
        return [run[step]]
    return [step]


def find_failed_tasks(
    run: "Run", step=None, max_tasks: int = 200, stop_after: int = 1
) -> List["Task"]:
    """
    Return failed tasks from a completed run with bounded iteration and early exit.

    Failure is detected by the absence of the `_success` artifact descriptor only.
    This helper never deserializes `_success` blob data.

    NOTE:
    This utility assumes tasks belong to a completed run. Tasks without `_success`
    are treated as failed. Running tasks may also lack `_success` and are not
    distinguished here.
    """
    if max_tasks <= 0 or stop_after <= 0:
        return []

    failed_tasks = []
    inspected_tasks = 0

    for current_step in _iter_steps(run, step):
        for task in current_step:
            if inspected_tasks >= max_tasks:
                return failed_tasks
            inspected_tasks += 1

            try:
                task["_success"]
            except KeyError:
                failed_tasks.append(task)
                if len(failed_tasks) >= stop_after:
                    return failed_tasks

    return failed_tasks
