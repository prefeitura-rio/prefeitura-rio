# -*- coding: utf-8 -*-
from typing import Any, Callable, Iterable, List, Set

try:
    from prefect import Flow
    from prefect import Flow as PrefectFlow
    from prefect.core.edge import Edge
    from prefect.core.task import Task
    from prefect.engine.result import Result
    from prefect.engine.state import State
    from prefect.executors import Executor, LocalDaskExecutor
    from prefect.run_configs import RunConfig
    from prefect.schedules import Schedule
    from prefect.storage import Storage
except ImportError:
    from prefeitura_rio.utils import base_assert_dependencies

    base_assert_dependencies(["prefect"], extras=["pipelines"])

from prefeitura_rio.pipelines_utils.state_handlers import handler_skip_if_running


class Flow(PrefectFlow):
    def __init__(
        self,
        name: str,
        schedule: Schedule = None,
        executor: Executor = None,
        run_config: RunConfig = None,
        storage: Storage = None,
        tasks: Iterable[Task] = None,
        edges: Iterable[Edge] = None,
        reference_tasks: Iterable[Task] = None,
        state_handlers: List[Callable[..., Any]] = None,
        on_failure: Callable[..., Any] = None,
        validate: bool = None,
        result: Result | None = None,
        terminal_state_handler: Callable[[Flow, State, Set[State]], State | None] | None = None,
        skip_if_running: bool = False,
        parallelism: int | None = None,
    ):
        if skip_if_running:
            if state_handlers is None:
                state_handlers = []
            state_handlers.append(handler_skip_if_running)
        if parallelism is not None:
            if isinstance(parallelism, int):
                new_executor = LocalDaskExecutor(num_workers=parallelism)
            else:
                raise ValueError(f"parallelism must be an integer, not {type(parallelism)}")
            executor = executor or new_executor
        super().__init__(
            name=name,
            schedule=schedule,
            executor=executor,
            run_config=run_config,
            storage=storage,
            tasks=tasks,
            edges=edges,
            reference_tasks=reference_tasks,
            state_handlers=state_handlers,
            on_failure=on_failure,
            validate=validate,
            result=result,
            terminal_state_handler=terminal_state_handler,
        )
