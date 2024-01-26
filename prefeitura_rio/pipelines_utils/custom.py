# -*- coding: utf-8 -*-
import ftplib
import re
from typing import Any, Callable, Iterable, List, Set, Optional

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
        threaded_heartbeat: bool = True,
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
        self._threaded_heartbeat = threaded_heartbeat
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

    def __setattr__(self, __name: str, __value: Any) -> None:
        super().__setattr__(__name, __value)
        if __name == "run_config":
            if self._threaded_heartbeat and self.run_config is not None:
                if self.run_config.env is None:
                    self.run_config.env = {}
                self.run_config.env["PREFECT__CLOUD__HEARTBEAT_MODE"] = "thread"


class FTPClient:  # pylint: disable=too-many-instance-attributes
    """
    FTP client implementation
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        hostname: str,
        username: str,
        password: Optional[str] = None,
        port: Optional[int] = 21,
        passive: Optional[bool] = True,
        timeout: Optional[int] = None,
    ):
        """
        Initialize the FTP client.

        Args:
            hostname (str): The hostname of the FTP server.
            username (str): The username to use for the FTP server.
            password (str): The password to use for the FTP server.
            port (int, optional): The port to use for the FTP server. Defaults to 21.
            passive (bool, optional): Whether to use passive mode. Defaults to True.
            timeout (int, optional): The timeout to use for the FTP server. Defaults to None.
        """
        self._hostname = hostname
        self._username = username
        self._password = password
        self._port = port
        self._passive = passive
        self._timeout = timeout
        self._ftp = ftplib.FTP()
        self._connected: bool = False

    @property
    def ftp(self) -> ftplib.FTP:
        """
        Returns the underlying FTP object.
        """
        return self._ftp

    @property
    def connected(self) -> bool:
        """
        Returns whether the FTP client is connected.
        """
        return self._connected

    def connect(self) -> None:
        """
        Connect to the FTP server.
        """
        self._ftp.connect(host=self._hostname, port=self._port, timeout=self._timeout)
        self._ftp.login(user=self._username, passwd=self._password)
        self._ftp.set_pasv(val=self._passive)
        self._connected = True

    def close(self) -> None:
        """
        Close the FTP connection.
        """
        self._ftp.quit()
        self._connected = False

    @assert_connected
    def chdir(self, path: str) -> None:
        """
        Change the current working directory.

        Args:
            path (str): The path to change to.
        """
        self._ftp.cwd(path)

    @assert_connected
    def download(self, remote_path: str, local_path: str) -> None:
        """
        Download a file from the FTP server.

        Args:
            remote_path (str): The path to the file on the FTP server.
            local_path (str): The path to the file on the local machine.
        """
        with open(local_path, "wb") as file_handler:
            self._ftp.retrbinary("RETR " + remote_path, file_handler.write)

    @assert_connected
    def list_files(self, path: str = "", pattern: str = None) -> List[str]:
        """
        List the files in a directory on the FTP server.

        Args:
            path (str): The path to the directory on the FTP server.
            pattern (str, optional): A regex pattern to filter the files by. Defaults to None.
        """
        if path == ".":
            path = ""
        files = self._ftp.nlst(path)
        if pattern:
            pattern = re.compile(pattern)
            files = [f for f in files if pattern.match(f)]
        return files

    @assert_connected
    def upload(self, local_path: str, remote_path: str):  # noqa
        """
        Upload a file to the FTP server.

        Args:
            local_path (str): The path to the file on the local machine.
            remote_path (str): The path to the file on the FTP server.
        """
        raise NotImplementedError("Not implemented as we won't be using this yet.")
