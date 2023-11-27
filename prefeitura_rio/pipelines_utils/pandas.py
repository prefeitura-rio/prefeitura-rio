# -*- coding: utf-8 -*-
import re
from os import walk
from os.path import join
from pathlib import Path
from typing import List, Tuple
from uuid import uuid4

try:
    import numpy as np
    import pandas as pd
except ImportError:
    from prefeitura_rio.utils import base_assert_dependencies

    base_assert_dependencies(["pandas"], extras=["pipelines"])

from prefeitura_rio.pipelines_utils.logging import log


def batch_to_dataframe(batch: Tuple[Tuple], columns: List[str]) -> pd.DataFrame:
    """
    Converts a batch of rows to a dataframe.
    """
    return pd.DataFrame(batch, columns=columns)


def build_query_new_columns(table_columns: List[str]) -> List[str]:
    """ "
    Creates the query without accents.
    """
    new_cols = remove_columns_accents(pd.DataFrame(columns=table_columns))
    return "\n".join(
        [f"{old_col} AS {new_col}," for old_col, new_col in zip(table_columns, new_cols)]
    )


def clean_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans a dataframe.
    """
    for col in dataframe.columns.tolist():
        if dataframe[col].dtype == object:
            try:
                dataframe[col] = (
                    dataframe[col].astype(str).str.replace("\x00", "").replace("None", np.nan)
                )
            except Exception as exc:
                print(
                    "Column: ",
                    col,
                    "\nData: ",
                    dataframe[col].tolist(),
                    "\n",
                    exc,
                )
                raise
    return dataframe


def dataframe_to_parquet(
    dataframe: pd.DataFrame,
    path: str | Path,
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


def dump_header_to_file(data_path: str | Path, data_type: str = "csv"):
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
        dataframe.to_csv(save_header_file_path, index=False, encoding="utf-8")
    elif data_type == "parquet":
        dataframe = pd.read_parquet(file)[:1]
        dataframe_to_parquet(dataframe=dataframe, path=save_header_file_path)

    log(f"Wrote {data_type.upper()} header at {save_header_file_path}")

    return save_header_path


def final_column_treatment(column: str) -> str:
    """
    Adds an underline before column name if it only has numbers or remove all non alpha numeric
    characters besides underlines ("_").
    """
    try:
        int(column)
        return f"_{column}"
    except ValueError:  # pylint: disable=bare-except
        non_alpha_removed = re.sub(r"[\W]+", "", column)
        return non_alpha_removed


def parse_date_columns(
    dataframe: pd.DataFrame, partition_date_column: str
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Parses the date columns to the partition format.
    """
    ano_col = "ano_particao"
    mes_col = "mes_particao"
    data_col = "data_particao"
    cols = [ano_col, mes_col, data_col]
    for col in cols:
        if col in dataframe.columns:
            raise ValueError(f"Column {col} already exists, please review your model.")

    dataframe[partition_date_column] = dataframe[partition_date_column].astype(str)
    dataframe[data_col] = pd.to_datetime(dataframe[partition_date_column], errors="coerce")

    dataframe[ano_col] = (
        dataframe[data_col].dt.year.fillna(-1).astype(int).astype(str).replace("-1", np.nan)
    )

    dataframe[mes_col] = (
        dataframe[data_col].dt.month.fillna(-1).astype(int).astype(str).replace("-1", np.nan)
    )

    dataframe[data_col] = dataframe[data_col].dt.date

    return dataframe, [ano_col, mes_col, data_col]


def remove_columns_accents(dataframe: pd.DataFrame) -> list:
    """
    Remove accents from dataframe columns.
    """
    columns = [str(column) for column in dataframe.columns]
    dataframe.columns = columns
    return list(
        dataframe.columns.str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .map(lambda x: x.strip())
        .str.replace(" ", "_")
        .str.replace("/", "_")
        .str.replace("-", "_")
        .str.replace("\a", "_")
        .str.replace("\b", "_")
        .str.replace("\n", "_")
        .str.replace("\t", "_")
        .str.replace("\v", "_")
        .str.replace("\f", "_")
        .str.replace("\r", "_")
        .str.lower()
        .map(final_column_treatment)
    )


def to_json_dataframe(
    dataframe: pd.DataFrame = None,
    csv_path: str | Path = None,
    key_column: str = None,
    read_csv_kwargs: dict = None,
    save_to: str | Path = None,
) -> pd.DataFrame:
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
