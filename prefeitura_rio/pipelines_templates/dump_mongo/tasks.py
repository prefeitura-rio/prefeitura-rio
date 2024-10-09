# -*- coding: utf-8 -*-
from datetime import timedelta

try:
    from prefect import task
except ImportError:
    from prefeitura_rio.utils import base_assert_dependencies

    base_assert_dependencies(["prefect"], extras=["pipelines"])
from prefeitura_rio.core import settings


@task(
    checkpoint=False,
    max_retries=settings.TASK_MAX_RETRIES_DEFAULT,
    retry_delay=timedelta(seconds=settings.TASK_RETRY_DELAY_DEFAULT),
)
def database_get_mongo(
    connection_string: str,
    database: str,
    collection: str,
):
    """
    Returns a Mongo object.

    Args:
        connection_string (str): MongoDB connection string.
        database (str): Database name.
        collection (str): Collection name.

    Returns:
        A database object.
    """
    from prefeitura_rio.pipelines_utils.database_mongo import Mongo

    return Mongo(
        connection_string=connection_string,
        database=database,
        collection=collection,
    )
