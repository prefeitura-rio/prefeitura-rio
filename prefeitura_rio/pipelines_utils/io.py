# -*- coding: utf-8 -*-
import re
import textwrap
from datetime import datetime
from pathlib import Path
from typing import List, Union

try:
    import croniter
except ImportError:
    pass

import ruamel.yaml as ryaml

from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.utils import assert_dependencies


@assert_dependencies(["croniter"], extras=["pipelines-templates"])
def determine_whether_to_execute_or_not(
    cron_expression: str, datetime_now: datetime, datetime_last_execution: datetime
) -> bool:
    """
    Determines whether the cron expression is currently valid.

    Args:
        cron_expression: The cron expression to check.
        datetime_now: The current datetime.
        datetime_last_execution: The last datetime the cron expression was executed.

    Returns:
        True if the cron expression should trigger, False otherwise.
    """
    cron_expression_iterator = croniter.croniter(cron_expression, datetime_last_execution)
    next_cron_expression_time = cron_expression_iterator.get_next(datetime)
    return next_cron_expression_time <= datetime_now


def extract_last_partition_date(partitions_dict: dict, date_format: str):
    """
    Extract last date from partitions folders
    """
    last_partition_date = None
    for partition, values in partitions_dict.items():
        new_values = [date for date in values if is_date(date_string=date, date_format=date_format)]
        try:
            last_partition_date = datetime.strptime(max(new_values), date_format).strftime(
                date_format
            )
            log(
                f"last partition from {partition} is in date format "
                f"{date_format}: {last_partition_date}"
            )
        except ValueError:
            log(f"partition {partition} is not a date or not in correct format {date_format}")
    return last_partition_date


def get_root_path() -> Path:
    """
    Returns the root path of the project.
    """
    try:
        import pipelines
    except ImportError as exc:
        raise ImportError("pipelines package not found") from exc
    root_path = Path(pipelines.__file__).parent.parent
    # If the root path is site-packages, we're running in a Docker container. Thus, we
    # need to change the root path to /app
    if str(root_path).endswith("site-packages"):
        root_path = Path("/app")
    return root_path


def human_readable(
    value: Union[int, float],
    unit: str = "",
    unit_prefixes: List[str] = None,
    unit_divider: int = 1000,
    decimal_places: int = 2,
):
    """
    Formats a value in a human readable way.
    """
    if unit_prefixes is None:
        unit_prefixes = ["", "k", "M", "G", "T", "P", "E", "Z", "Y"]
    if value == 0:
        return f"{value}{unit}"
    unit_prefix = unit_prefixes[0]
    for prefix in unit_prefixes[1:]:
        if value < unit_divider:
            break
        unit_prefix = prefix
        value /= unit_divider
    return f"{value:.{decimal_places}f}{unit_prefix}{unit}"


def is_date(date_string: str, date_format: str = "%Y-%m-%d") -> Union[datetime, bool]:
    """
    Checks whether a string is a valid date.
    """
    try:
        return datetime.strptime(date_string, date_format).strftime(date_format)
    except ValueError:
        return False


def query_to_line(query: str) -> str:
    """
    Converts a query to a line.
    """
    query = textwrap.dedent(query)
    return " ".join([line.strip() for line in query.split("\n")])


def remove_tabs_from_query(query: str) -> str:
    """
    Removes tabs from a query.
    """
    query = query_to_line(query)
    return re.sub(r"\s+", " ", query).strip()


def untuple_clocks(clocks):
    """
    Converts a list of tuples to a list of clocks.
    """
    return [clock[0] if isinstance(clock, tuple) else clock for clock in clocks]


def load_ruamel():
    """
    Loads a YAML file.
    """
    ruamel = ryaml.YAML()
    ruamel.default_flow_style = False
    ruamel.top_level_colon_align = True
    ruamel.indent(mapping=2, sequence=4, offset=2)
    return ruamel


def load_yaml_file(filepath: str) -> dict:
    """
    Loads the file that contains path to the models' metadata.
    """
    ruamel = load_ruamel()
    return ruamel.load((Path(filepath)).open(encoding="utf-8"))
