from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from inspect import isawaitable
from typing import Any

import anyio

from .._exceptions import JobTimedOutError
from .._structures import Job
from ..abc import JobExecutor


class AsyncJobExecutor(JobExecutor):
    """
    Executes functions directly on the event loop thread.

    If the function returns a coroutine object (or another kind of awaitable), that is
    awaited on and its return value is used as the job's return value.
    """

    async def run_job(self, func: Callable[..., Any], job: Job) -> Any:
        # Convert timeout to seconds if it's a timedelta
        timeout_seconds = (
            job.timeout.total_seconds()
            if isinstance(job.timeout, timedelta)
            else job.timeout
        )

        async def wrapper():
            retval = func(*job.args, **job.kwargs)
            if isawaitable(retval):
                retval = await retval
            return retval

        try:
            with anyio.fail_after(timeout_seconds):
                return await wrapper()
        except TimeoutError:
            raise JobTimedOutError from None
