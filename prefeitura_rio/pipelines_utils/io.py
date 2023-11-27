# -*- coding: utf-8 -*-
import re
import textwrap
from datetime import datetime
from os import walk
from os.path import join
from pathlib import Path
from typing import List, Union
from uuid import uuid4

try:
    import croniter
    import pandas as pd
except ImportError:
    pass

from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.utils import assert_dependencies


@assert_dependencies(["pandas"], extras=["pipelines"])
def dataframe_to_csv(
    dataframe: "pd.DataFrame",
    path: Union[str, Path],
    build_json_dataframe: bool = False,
    dataframe_key_column: str = None,
) -> None:
    """
    Writes a dataframe to CSV file.
    """
    if build_json_dataframe:
        dataframe = to_json_dataframe(dataframe, key_column=dataframe_key_column)

    # Remove filename from path
    path = Path(path)
    # Create directory if it doesn't exist
    path.parent.mkdir(parents=True, exist_ok=True)

    # Write dataframe to CSV
    dataframe.to_csv(path, index=False, encoding="utf-8")


@assert_dependencies(["pandas"], extras=["pipelines"])
def dataframe_to_parquet(
    dataframe: "pd.DataFrame",
    path: Union[str, Path],
    build_json_dataframe: bool = False,
    dataframe_key_column: str = None,
):
    """
    Writes a dataframe to Parquet file with Schema as STRING.
    """
    # Code adapted from
    # https://stackoverflow.com/a/70817689/9944075

    if build_json_dataframe:
        dataframe = to_json_dataframe(dataframe, key_column=dataframe_key_column)

    # If the file already exists, we:
    # - Load it
    # - Merge the new dataframe with the existing one
    if Path(path).exists():
        # Load it
        original_df = pd.read_parquet(path)
        # Merge the new dataframe with the existing one
        dataframe = pd.concat([original_df, dataframe], sort=False)

    # Write dataframe to Parquet
    dataframe.to_parquet(path, engine="pyarrow")


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


@assert_dependencies(["pandas"], extras=["pipelines"])
def dump_header_to_file(data_path: Union[str, Path], data_type: str = "csv"):
    """
    Writes a header to a CSV file.
    """
    try:
        assert data_type in ["csv", "parquet"]
    except AssertionError as exc:
        raise ValueError(f"Invalid data type: {data_type}") from exc
    # Remove filename from path
    path = Path(data_path)
    if not path.is_dir():
        path = path.parent
    # Grab first `data_type` file found
    found: bool = False
    file: str = None
    for subdir, _, filenames in walk(str(path)):
        for fname in filenames:
            if fname.endswith(f".{data_type}"):
                file = join(subdir, fname)
                log(f"Found {data_type.upper()} file: {file}")
                found = True
                break
        if found:
            break

    save_header_path = f"data/{uuid4()}"
    # discover if it's a partitioned table
    if partition_folders := [folder for folder in file.split("/") if "=" in folder]:
        partition_path = "/".join(partition_folders)
        save_header_file_path = Path(f"{save_header_path}/{partition_path}/header.{data_type}")
        log(f"Found partition path: {save_header_file_path}")

    else:
        save_header_file_path = Path(f"{save_header_path}/header.{data_type}")
        log(f"Do not found partition path: {save_header_file_path}")

    # Create directory if it doesn't exist
    save_header_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Read just first row and write dataframe to file
    if data_type == "csv":
        dataframe = pd.read_csv(file, nrows=1)
        dataframe_to_csv(dataframe=dataframe, path=save_header_file_path)
    elif data_type == "parquet":
        dataframe = pd.read_parquet(file)[:1]
        dataframe_to_parquet(dataframe=dataframe, path=save_header_file_path)

    log(f"Wrote {data_type.upper()} header at {save_header_file_path}")

    return save_header_path


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


@assert_dependencies(["pandas"], extras=["pipelines"])
def to_json_dataframe(
    dataframe: "pd.DataFrame" = None,
    csv_path: Union[str, Path] = None,
    key_column: str = None,
    read_csv_kwargs: dict = None,
    save_to: Union[str, Path] = None,
) -> "pd.DataFrame":
    """
    Manipulates a dataframe by keeping key_column and moving every other column
    data to a "content" column in JSON format. Example:

    - Input dataframe: pd.DataFrame({"key": ["a", "b", "c"], "col1": [1, 2, 3], "col2": [4, 5, 6]})
    - Output dataframe: pd.DataFrame({
        "key": ["a", "b", "c"],
        "content": [{"col1": 1, "col2": 4}, {"col1": 2, "col2": 5}, {"col1": 3, "col2": 6}]
    })
    """
    if dataframe is None and not csv_path:
        raise ValueError("dataframe or dataframe_path is required")
    if csv_path:
        dataframe = pd.read_csv(csv_path, **read_csv_kwargs)
    if key_column:
        dataframe["content"] = dataframe.drop(columns=[key_column]).to_dict(orient="records")
        dataframe = dataframe[["key", "content"]]
    else:
        dataframe["content"] = dataframe.to_dict(orient="records")
        dataframe = dataframe[["content"]]
    if save_to:
        dataframe.to_csv(save_to, index=False)
    return dataframe


def to_partitions(
    data: pd.DataFrame,
    partition_columns: List[str],
    savepath: str,
    data_type: str = "csv",
    suffix: str = None,
    build_json_dataframe: bool = False,
    dataframe_key_column: str = None,
) -> List[Path]:  # sourcery skip: raise-specific-error
    """Save data in to hive patitions schema, given a dataframe and a list of partition columns.
    Args:
        data (pandas.core.frame.DataFrame): Dataframe to be partitioned.
        partition_columns (list): List of columns to be used as partitions.
        savepath (str, pathlib.PosixPath): folder path to save the partitions
    Exemple:
        data = {
            "ano": [2020, 2021, 2020, 2021, 2020, 2021, 2021,2025],
            "mes": [1, 2, 3, 4, 5, 6, 6,9],
            "sigla_uf": ["SP", "SP", "RJ", "RJ", "PR", "PR", "PR","PR"],
            "dado": ["a", "b", "c", "d", "e", "f", "g",'h'],
        }
        to_partitions(
            data=pd.DataFrame(data),
            partition_columns=['ano','mes','sigla_uf'],
            savepath='partitions/'
        )
    """
    saved_files = []
    if isinstance(data, (pd.core.frame.DataFrame)):
        savepath = Path(savepath)

        # create unique combinations between partition columns
        unique_combinations = (
            data[partition_columns]
            .drop_duplicates(subset=partition_columns)
            .to_dict(orient="records")
        )

        for filter_combination in unique_combinations:
            patitions_values = [
                f"{partition}={value}" for partition, value in filter_combination.items()
            ]

            # get filtered data
            df_filter = data.loc[
                data[filter_combination.keys()].isin(filter_combination.values()).all(axis=1),
                :,
            ]
            df_filter = df_filter.drop(columns=partition_columns).reset_index(drop=True)

            # create folder tree
            filter_save_path = Path(savepath / "/".join(patitions_values))
            filter_save_path.mkdir(parents=True, exist_ok=True)
            if suffix is not None:
                file_filter_save_path = Path(filter_save_path) / f"data_{suffix}.{data_type}"
            else:
                file_filter_save_path = Path(filter_save_path) / f"data.{data_type}"

            if build_json_dataframe:
                df_filter = to_json_dataframe(df_filter, key_column=dataframe_key_column)

            if data_type == "csv":
                # append data to csv
                df_filter.to_csv(
                    file_filter_save_path,
                    index=False,
                    mode="a",
                    header=not file_filter_save_path.exists(),
                )
                saved_files.append(file_filter_save_path)
            elif data_type == "parquet":
                dataframe_to_parquet(dataframe=df_filter, path=file_filter_save_path)
                saved_files.append(file_filter_save_path)
            else:
                raise ValueError(f"Invalid data type: {data_type}")
    else:
        raise BaseException("Data need to be a pandas DataFrame")

    return saved_files
