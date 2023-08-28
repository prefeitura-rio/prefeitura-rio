# -*- coding: utf-8 -*-
try:
    import prefect
    import sentry_sdk
    from prefect.client import Client
    from prefect.engine.state import Skipped, State
except ImportError:
    from prefeitura_rio.utils import base_assert_dependencies

    base_assert_dependencies(["prefect", "sentry_sdk"], extras=["pipelines"])

from prefeitura_rio.pipelines_utils.infisical import get_secret, inject_bd_credentials
from prefeitura_rio.pipelines_utils.prefect import get_flow_run_mode


def handler_initialize_sentry(obj, old_state: State, new_state: State) -> State:
    """
    State handler that will set up Sentry.
    """
    if new_state.is_running():
        sentry_dsn = get_secret("SENTRY_DSN")
        environment = get_flow_run_mode()
        sentry_sdk.init(
            dsn=sentry_dsn,
            traces_sample_rate=0,
            environment=environment,
        )
    return new_state


def handler_inject_bd_credentials(obj, old_state: State, new_state: State) -> State:
    """
    State handler that will inject BD credentials into the environment.
    """
    if new_state.is_running():
        inject_bd_credentials()
    return new_state


def handler_skip_if_running(obj, old_state: State, new_state: State) -> State:
    """
    State handler that will skip a flow run if another instance of the flow is already running.

    Adapted from Prefect Discourse:
    https://tinyurl.com/4hn5uz2w
    """
    if new_state.is_running():
        client = Client()
        query = """
            query($flow_id: uuid) {
                flow_run(
                    where: {
                        _and: [
                            {state: {_eq: "Running"}},
                            {flow_id: {_eq: $flow_id}}
                        ]
                    }
                ) {
                    id
                }
            }
        """
        # pylint: disable=no-member
        response = client.graphql(
            query=query,
            variables=dict(flow_id=prefect.context.flow_id),
        )
        active_flow_runs = response["data"]["flow_run"]
        if active_flow_runs:
            logger = prefect.context.get("logger")
            message = "Skipping this flow run since there are already some flow runs in progress"
            logger.info(message)
            return Skipped(message)
    return new_state
