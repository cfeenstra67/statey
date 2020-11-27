import asyncio
import dataclasses as dc
import logging
import traceback
import time
from typing import Optional, Callable

import click

import statey as st
from statey.cli.inspect import color_for_status
from statey.task import Task, TaskStatus


class TaskWrapper(Task):
    """
    Base class for tasks. A task is a unit of computation
    """

    task: Task

    @property
    def description(self) -> Optional[str]:
        return self.task.description

    def always_eager(self) -> bool:
        return self.task.always_eager()

    def is_metatask(self) -> bool:
        return self.task.is_metatask()


@dc.dataclass(frozen=True)
class TaskLogger(TaskWrapper):
    """
    Log events and heartbeats for tasks
    """

    task: Task
    key: str
    heartbeat_interval: float = 15.0

    async def run(self) -> None:
        start = time.time()
        asyncio_task = asyncio.ensure_future(self.task.run())

        while True:
            shielded = asyncio.shield(asyncio_task)
            try:
                await asyncio.wait_for(shielded, timeout=self.heartbeat_interval)
            except asyncio.TimeoutError:
                click.echo(
                    f"Task {click.style(self.key, fg='yellow')} still running "
                    f"({time.time() - start:.2f}s elapsed)."
                )
                continue
            except asyncio.CancelledError:
                asyncio_task.cancel()
                raise
            return


class ExecutorLoggingPlugin:
    """
    Plugin to log signals when caught
    """

    def __init__(
        self, show_metatasks: bool = False, heartbeat_interval: float = 15.0
    ) -> None:
        self.task_starts = {}
        self.show_metatasks = show_metatasks
        self.heartbeat_interval = heartbeat_interval

    @st.hookimpl
    def caught_signal(
        self, signals: int, max_signals: int, executor: "TaskGraphExecutor"
    ) -> None:
        if signals >= max_signals:
            click.secho(
                f"Caught {signals} signals(s) >= max_signals = {max_signals}. "
                f"Exiting forcefully, this may cause data loss.",
                fg="red",
            )
        else:
            color = "yellow" if signals == 1 else "red"
            click.secho(
                f"Caught {signals} signal(s), exiting gracefully. Additional signals may cause data loss.",
                fg=color,
            )

    def should_show(self, task: Task) -> bool:
        return self.show_metatasks or not task.is_metatask()

    @st.hookimpl
    def before_run(
        self, key: str, task: st.Task, executor: "TaskGraphExecutor"
    ) -> None:
        self.task_starts[key] = time.time()
        if self.should_show(task):
            click.echo(f'Task {click.style(key, fg="yellow")} is running.')

    @st.hookimpl
    def after_run(
        self, key: str, task: st.Task, status: TaskStatus, executor: "TaskGraphExecutor"
    ) -> None:
        duration = time.time() - self.task_starts[key]
        color = color_for_status(status)
        styled_status = click.style(status.name, fg=color, bold=True)
        if self.should_show(task):
            click.echo(
                f'Task {click.style(key, fg="yellow")} finished after {duration:.2f}s with status {styled_status}.'
            )

    @st.hookimpl
    def task_wrapper(
        self, key: str, task: st.Task, executor: "TaskGraphExecutor"
    ) -> Callable[[st.Task], st.Task]:
        if not self.should_show(task):
            return None
        return lambda input_task: TaskLogger(input_task, key, self.heartbeat_interval)


class CLILoggingHandler(logging.Handler):
    """

    """

    def __init__(self, fulltrace: bool = False) -> None:
        """

        """
        self.fulltrace = fulltrace
        super().__init__()

    def emit(self, record):

        printer = click.echo

        if record.levelno == logging.WARNING:
            printer = lambda x: click.secho(x, fg="yellow")
        elif record.levelno >= logging.ERROR:
            printer = lambda x: click.secho(x, fg="red")

        printer(record.getMessage())

        if record.exc_info is not None and self.fulltrace:
            formatted = traceback.format_exception(*record.exc_info)
            click.secho("".join(formatted), fg="red")
