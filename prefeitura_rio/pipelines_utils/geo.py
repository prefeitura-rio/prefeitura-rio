# -*- coding: utf-8 -*-
try:
    from geobr import read_municipality
    from shapely import wkt
    from shapely.geometry import (
        GeometryCollection,
        LinearRing,
        LineString,
        MultiLineString,
        MultiPoint,
        MultiPolygon,
        Point,
        Polygon,
    )


except ImportError:
    pass

from prefeitura_rio.utils import assert_dependencies


@assert_dependencies(["shapely"], extras=["pipelines-templates"])
def load_wkt(x):
    """
    Fromt text to geometry
    """
    try:
        return wkt.loads(x)
    except Exception:
        return None


@assert_dependencies(["shapely"], extras=["pipelines-templates"])
def remove_third_dimension(geom):
    """
    Remove third dimension from geometry
    """
    if geom is None:
        return None

    if geom.is_empty:
        return geom

    if isinstance(geom, Polygon):
        exterior = geom.exterior
        new_exterior = remove_third_dimension(exterior)

        interiors = geom.interiors
        new_interiors = [remove_third_dimension(int) for int in interiors]
        return Polygon(new_exterior, new_interiors)

    elif isinstance(geom, LinearRing):
        return LinearRing([xy[:2] for xy in list(geom.coords)])

    elif isinstance(geom, LineString):
        return LineString([xy[:2] for xy in list(geom.coords)])

    elif isinstance(geom, Point):
        return Point([xy[:2] for xy in list(geom.coords)])

    elif isinstance(geom, MultiPoint):
        points = list(geom.geoms)
        new_points = [remove_third_dimension(point) for point in points]
        return MultiPoint(new_points)

    elif isinstance(geom, MultiLineString):
        lines = list(geom.geoms)
        new_lines = [remove_third_dimension(line) for line in lines]
        return MultiLineString(new_lines)

    elif isinstance(geom, MultiPolygon):
        pols = list(geom.geoms)

        new_pols = [remove_third_dimension(pol) for pol in pols]
        return MultiPolygon(new_pols)

    elif isinstance(geom, GeometryCollection):
        geoms = list(geom.geoms)

        new_geoms = [remove_third_dimension(geom) for geom in geoms]
        return GeometryCollection(new_geoms)

    else:
        raise RuntimeError(f"Currently this type of geometry is not supported: {type(geom)}")


@assert_dependencies(["shapely"], extras=["pipelines-templates"])
def check_if_belongs_to_rio(lat: float, long: float) -> list:
    """
    Verifica se o lat/long retornado pela API do Waze pertence ao geometry
    da cidade do Rio de Janeiro. Se pertencer retorna o lat, lon, se não retorna None.
    """

    # Acessa dados da cidade do Rio de Janeiro
    rio = read_municipality(code_muni=3304557, year=2020)

    # Cria point com a latitude e longitude
    point = Point(long, lat)

    # Cria polígono com o geometry do Rio de Janeiro
    polygon = rio.geometry

    # Use polygon.contains(point) to test if point is inside (True) or outside (False) the polygon.
    pertence = polygon.contains(point)

    # Se pertence retorna lat, lon. Do contrário retorna nan
    if pertence.iloc[0]:
        lat_lon = [lat, long]
    else:
        lat_lon = [None, None]
    return lat_lon
