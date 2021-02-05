import asyncio
import time
from logging import Logger, getLogger, root as logging_root
from dataclasses import dataclass
from typing import List, Callable, Awaitable, Coroutine, Set, Any, Optional


async def run_supervised(target, args=(), kwargs=None, *,
                         name: str = '<unnamed>',
                         log_lifecycle: bool = True,
                         log_exception: bool = False,
                         logger: Logger = logging_root,
                         restart_always: bool = True,
                         restart_time: float = 1.0,
                         cancel_event: Optional[asyncio.Event] = None) -> Any:
    """Runs a coroutine supervised to ensure it is restarted when it fails or that it runs successfuly once.

    The `name`, `log_lifecycle`, `log_exception` and `logger` parameters are used for application maintenance, it will
    help you identify who failed when problems happens.

    The `restart_always` and `restart_time` allows you to control the supervision mode.

    `cancel_event` may help with bad behaving tasks which doesn't raise CancelledError when cancelled.

    :param target: callable object to be invoked
    :param args: argument tuple for the target invocation, defaults to ()
    :param kwargs: dictionary of keyword arguments for the target invocation, defaults to {}
    :param name: name used in logs so you can differenciate which function is running or throwed exception
    :param log_lifecycle: whether to log the 'enter' or 'leave' changes
    :param log_exception: if the callable throws exception, whether to log the stacktrace (True) or just a warning
     (False, default)
    :param logger: the logger to use for messages, defauls to the root logger
    :param restart_always: whether to restart always (True, default) or just run once successfully (False)
    :param restart_time: minimum time between invocation starts.
    :param cancel_event: optional event used to check whether to continue restarting or raise CancelledError.
    :return: when restart_always is False, will return the result of the successfull invocation of target, otherwise
     this function never returns.
    """
    if kwargs is None:
        kwargs = {}

    last_run = 0
    loop = asyncio.get_running_loop()
    result: Any

    try:
        if log_lifecycle:
            logger.info('%s task enter', name)

        while True:
            if cancel_event is not None and cancel_event.is_set():
                raise asyncio.CancelledError()

            try:
                now = loop.time()
                dif = now - last_run
                if dif < restart_time:
                    await asyncio.sleep(restart_time - dif)

                last_run = loop.time()
                result = await target(*args, **kwargs)

                # restart_always is there in case we want to run 'once' successfully (without exceptions) then exit
                if not restart_always:
                    return result

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                if log_exception:
                    logger.exception('%s raised exception: %s', name, exc, exc_info=exc)
                else:
                    logger.warning('%s raised exception: %s', name, exc)
    finally:
        if log_lifecycle:
            logger.info('%s task leave', name)


class CancelTaskWarning(Exception):
    """Exception thrown when something happened cancelling a task"""


async def cancel_task(task: Optional[asyncio.Task], timeout: Optional[float] = None) -> None:
    """Tryes to cancel a task, optionally with a timeout.

    If there was no task or it was already done, then nothing happens.

    If we do actually cancel the task, we expect it to behave and cancel properly, otherwise `CancelTaskWarning`
    will be raised.

    :param task: the task to cancel
    :param timeout: optional max time to wait for the cancellation
    """
    if task is not None and not task.done():
        task.cancel()

        if len((await asyncio.wait((task,), timeout=timeout))[0]) == 1:
            if not task.cancelled():
                exc = task.exception()
                if exc is None:
                    raise CancelTaskWarning("Cancelled task finished normally (should be cancelled)")
                else:
                    raise CancelTaskWarning(f"Cancelled task threw exception: {repr(exc)}") from exc
        else:
            raise CancelTaskWarning("Cancel timeout, I give up.")


@dataclass
class AioTaskDef:
    name: str
    """The name of the task"""

    fun: Callable[[int], Awaitable]
    """The task launcher, taking the task id, returning the awaitable fun."""

    restart_time: float = 1.0
    """Minimun time to wait before restarting the task"""


@dataclass
class WorkerData:
    task_id: int
    """The internal task id"""

    start_time: float = 0
    """`os.time()` when the task was launched"""

    start_delay: float = 0
    """The delay applied when the task was launched"""

    last_run_time: float = 0.0
    """The total run time of the task the last time it was launched"""

    task: asyncio.Task = None
    """The real task"""


def cleanup_workers(workers: List[WorkerData], count: int, logger: Logger):
    for task_index in range(len(workers)-1, count-1, -1):
        if workers[task_index].task is not None or workers[task_index].task.done() is False:
            logger.info('obsolete task %d still running', task_index+1)
        else:
            logger.info('removing finished task %d', task_index+1)
            del workers[task_index]


async def supervise(tasks: List[AioTaskDef], logger: Logger):
    """Supervises a predefined list of Tasks"""

    workers: List[WorkerData] = [
        WorkerData(task_id=i+1,
                   last_run_time=t.restart_time)
        for i, t in enumerate(tasks)
    ]

    logger.info('start')
    try:
        running_tasks: Set[asyncio.Task] = set()
        now = time.time()
        while True:

            # 1- start tasks which needs to be started
            for i, w in enumerate(workers):
                if w.task is None:
                    last_run_time = w.last_run_time
                    w.start_delay = start_delay = (0.0 if last_run_time >= tasks[i].restart_time
                                                   else tasks[i].restart_time - last_run_time)
                    w.start_time = now
                    task_fun = tasks[i].fun(w.task_id)

                    if start_delay > 0.0:
                        logger.info('starting task %s in %s s', tasks[i].name, start_delay)

                    # note: always use _run_after because in case of exception
                    # on the task_fun, the exception dump will be the same
                    # note2: we don't use scheduled tasks because adds complexity for maintenance
                    w.task = t = asyncio.create_task(_run_after(start_delay, task_fun))

                    running_tasks.add(t)

            # 2- wait for them
            completed_tasks, running_tasks = await asyncio.wait(running_tasks,
                                                                return_when=asyncio.FIRST_COMPLETED)
            now = time.time()

            # 3- check which task finished and set its timing values
            for i, w in enumerate(workers):
                t = w.task
                if t in completed_tasks:
                    w.task = None
                    w.last_run_time = max(0.0, (now - w.start_time) - w.start_delay)
                    completed_tasks.remove(t)

                    if not t.cancelled():
                        exc = t.exception()
                        if exc is not None:
                            logger.exception('Task %s exit due to exception: %s',
                                             tasks[i].name, exc, exc_info=exc)
                        else:
                            logger.warning('Task %s exitted normally', tasks[i].name)

                    if not completed_tasks:
                        break  # don't need to continue checking

    except asyncio.CancelledError:
        raise

    except Exception as exc:
        logger.exception('supervisor died because: %s', exc, exc_info=exc)

    finally:
        towait = set()
        for w in workers:
            if w.task is not None and not w.task.done():
                w.task.cancel()
                towait.add(w.task)

        if towait:
            await asyncio.wait(towait)

            for i, w in enumerate(workers):
                t = w.task
                if t is not None and not t.cancelled():
                    exc = t.exception()
                    if exc is not None:
                        logger.exception('Task %s final exit threw exception: %s',
                                         tasks[i].name, exc, exc_info=exc)

        logger.info('end')


async def _run_after(start_delay: float, fun: Awaitable):
    try:
        if start_delay > 0.0:
            await asyncio.sleep(start_delay)
    except asyncio.CancelledError:
        if isinstance(fun, Coroutine):
            fun.close()

        raise

    await fun


async def stop_supervisor(supervisor_task: asyncio.Task, logger: Logger):
    if not supervisor_task.done():
        supervisor_task.cancel()

        done, _ = await asyncio.wait([supervisor_task])

    if not supervisor_task.done():
        logger.warning("unable to stop")
    else:
        if not supervisor_task.cancelled():
            exc = supervisor_task.exception()
            if exc is not None:
                logger.exception('exception thrown on stop: %s', exc, exc_info=exc)


class AioSupervisor:
    """Helper class to supervise"""

    def __init__(self, name: str, tasks: List[AioTaskDef], logger: Logger = None):
        if logger is None:
            logger = getLogger(f'aiosupervisor.{name}')

        self._suptask = asyncio.create_task(supervise(tasks, logger))
        self._logger = logger

    async def stop(self):
        if self._suptask is not None:
            await stop_supervisor(self._suptask, self._logger)
            self._suptask = None

    def __aenter__(self):
        return self

    def __aexit__(self, exc_type, exc_val, exc_tb):
        self.stop()
