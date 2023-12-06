# -*- coding: utf-8 -*-
try:
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
