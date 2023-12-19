# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from pathlib import Path

try:
    from prefect import task
except ImportError:
    from prefeitura_rio.utils import base_assert_dependencies

    base_assert_dependencies(["prefect"], extras=["pipelines"])

try:
    import geopandas as gpd
    from geojsplit import geojsplit
except ImportError:
    pass

from prefeitura_rio.core import settings
from prefeitura_rio.pipelines_utils.geo import load_wkt, remove_third_dimension
from prefeitura_rio.pipelines_utils.logging import log, log_mod
from prefeitura_rio.pipelines_utils.pandas import remove_columns_accents


@task(
    max_retries=settings.TASK_MAX_RETRIES_DEFAULT,
    retry_delay=timedelta(seconds=settings.TASK_RETRY_DELAY_DEFAULT),
)
def transform_geodataframe(
    file_path: str | Path,
    batch_size: int = 50000,
    geometry_column: str = "geometry",
    convert_to_crs_4326: bool = False,
    geometry_3d_to_2d: bool = False,
):  # sourcery skip: convert-to-enumerate
    """ "
    Transform a CSV from data.rio API

    Parameters:
        - file_path (Union[str, Path]): Path to the geojson file to be transformed.
        - batch_size (int): Number of rows to process at once.
        - geometry_column (str): Column containing the geometry data.
        - convert_to_crs_4326 (bool): Convert the geometry data to the crs 4326 projection.
        - geometry_3d_to_2d (bool): Convert the geometry data from 3D to 2D.
    """
    base_assert_dependencies(["geojsplit", "geopandas"], extras=["pipelines-templates"])
    eventid = datetime.now().strftime("%Y%m%d-%H%M%S")

    # move to path file since file_path is path / "geo_data" / "data.geojson"
    save_path = file_path.parent.parent / "csv_data" / f"{eventid}.csv"
    save_path.parent.mkdir(parents=True, exist_ok=True)

    geojson = geojsplit.GeoJSONBatchStreamer(file_path)

    # only print every print_mod batches
    mod = 1000
    count = 1
    for feature_collection in geojson.stream(batch=batch_size):
        geodataframe = gpd.GeoDataFrame.from_features(feature_collection["features"])
        log_mod(
            msg=f"{count} x {batch_size} rows: geodataframe loaded",
            index=count,
            mod=mod,
        )

        # move geometry column to the end
        cols = geodataframe.columns.tolist()
        cols.remove(geometry_column)
        cols.append(geometry_column)
        geodataframe = geodataframe[cols]

        # remove accents from columns
        geodataframe.columns = remove_columns_accents(geodataframe)
        geodataframe["geometry_wkt"] = geodataframe[geometry_column].copy()

        # convert geometry to crs 4326
        if convert_to_crs_4326:
            try:
                geodataframe.crs = "epsg:4326"
                geodataframe[geometry_column] = geodataframe[geometry_column].to_crs("epsg:4326")
            except Exception as err:
                log(f"{count}: error converting to crs 4326: {err}")
                raise err

            log_mod(
                msg=f"{count}: geometry converted to crs 4326",
                index=count,
                mod=mod,
            )

        # convert geometry 3d to 2d
        if geometry_3d_to_2d:
            try:
                geodataframe[geometry_column] = (
                    geodataframe[geometry_column].astype(str).apply(load_wkt)
                )

                geodataframe[geometry_column] = geodataframe[geometry_column].apply(
                    remove_third_dimension
                )
            except Exception as err:
                log(f"{count}: error converting 3d to 2d: {err}")
                raise err

            log_mod(
                msg=f"{count}: geometry converted 3D to 2D",
                index=count,
                mod=mod,
            )

        log_mod(
            msg=f"{count}: new columns: {geodataframe.columns.tolist()}",
            index=count,
            mod=mod,
        )

        # save geodataframe to csv
        geodataframe.to_csv(
            save_path,
            index=False,
            encoding="utf-8",
            mode="a",
            header=not save_path.exists(),
        )

        # clear memory
        del geodataframe

        log_mod(
            msg=f"{count} x {batch_size} rows: Data saved",
            index=count,
            mod=mod,
        )
        count += 1
    log(f"{count} x {batch_size} DATA TRANSFORMED!!!")
    return save_path
