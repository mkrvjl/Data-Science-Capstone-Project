from typing import Optional

import geopandas as gpd
import matplotlib.pyplot as plt
from shapely import geometry
from haversine import inverse_haversine, Unit
from config.settings import ProjectSettings

settings = ProjectSettings()


def create_grid(
    geo_data: gpd,
    distance: float,
    units: Unit = None,
    remove_unused_grids: bool = False,
    save_as_file: bool = False,
) -> gpd:
    # Get the extent of the shapefile
    total_bounds = geo_data.total_bounds

    # Get minX, minY, maxX, maxY
    min_x, min_y, max_x, max_y = total_bounds

    # Create a fishnet
    x, y = (min_x, min_y)
    geom_array = []

    #
    if units is None:
        grid_size = distance
    else:
        # calculates distance with haversine formula
        # from (min_x, min_y) to a distance in units
        # to direction 0 * pi rads
        end_pt = inverse_haversine(
            point=(min_x, min_y),
            distance=distance,
            direction=0,
            unit=units,
        )
        grid_size = end_pt[0] - min_x

    # Start Idxs
    row_id = 0
    col_id = 0

    while y <= max_y:
        col_id = 0

        while x <= max_x:
            geom = geometry.Polygon(
                [
                    (x, y),
                    (x, y + grid_size),
                    (x + grid_size, y + grid_size),
                    (x + grid_size, y),
                    (x, y),
                ]
            )
            geom_array.append([geom, row_id, col_id])
            x += grid_size
            col_id += 1

        x = min_x
        y += grid_size
        row_id += 1

    grid = gpd.GeoDataFrame(geom_array, columns=["geometry", "grid_row_id", "grid_col_id"])

    print(f"Grid size is: ({row_id} x {col_id})")

    # standardize crs projections
    grid.crs = geo_data.crs

    if save_as_file:
        fig, ax = plt.subplots(figsize=(15, 15))
        gpd.GeoSeries(grid["geometry"]).boundary.plot(ax=ax)
        gpd.GeoSeries(geo_data["geometry"]).boundary.plot(ax=ax, color="red")
        fig.savefig(f"out/pdf/mtl_grid_{str(distance)}_{str(units.value)}.pdf")
        grid.to_file(f"out/data/grid/mtl_grid_{str(distance)}_{str(units.value)}.shp")

    if remove_unused_grids:
        cols_names = grid.columns.values
        grid = grid.sjoin(geo_data, how="inner", rsuffix="right")
        grid = grid[cols_names]

    grid['grid_id'] = grid.index.tolist()

    grid["Grid Name"] = (
        "(" + grid["grid_row_id"].astype(str) + "," + grid["grid_col_id"].astype(str) + ")"
    )

    return grid


def gridding_test():
    # mtl geodata
    lim_admin_mtl = gpd.read_file(settings.lim_admin_mtl.local.shp)

    grid_distance = 500
    grid_units = Unit.METERS

    # create grid
    grid = create_grid(
        geo_data=lim_admin_mtl,
        distance=grid_distance,
        units=grid_units,
        save_as_file=True,
    )

    # remove unused grids
    # grid_overlay = grid.sjoin(mtl_geo_shp, how="inner", rsuffix="right")
    # grid_overlay = grid_overlay[grid.columns.values]

    grid.drop('geometry', axis=1).to_csv(
        f"src/data/grid/mtl_grid_{grid_distance}{grid_units.value}.csv")

    # fig, ax = plt.subplots(figsize=(15, 15))
    # gpd.GeoSeries(grid_overlay["geometry"]).boundary.plot(ax=ax)
    # gpd.GeoSeries(mtl_geo_shp["geometry"]).boundary.plot(ax=ax, color="red")

    print("test")


def get_grid(distance: int, units: Unit) -> gpd:

    # universe
    lim_admin_mtl = gpd.read_file(settings.lim_admin_mtl.local.shp)

    # create grid
    grid = create_grid(
        geo_data=lim_admin_mtl,
        distance=distance,
        units=units,
    )

    return grid

