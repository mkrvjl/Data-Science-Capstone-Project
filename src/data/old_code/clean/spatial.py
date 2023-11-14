import os
import folium
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import config.logs as logs

from statistics import mean
from shapely import geometry
from config.settings import ProjectSettings
from haversine import inverse_haversine, Unit
from data.types.geospatial_dataset import GeoSpatialDataset
from utils.enums.databases import ExternalDatabases

settings = ProjectSettings()
logger = logs.get_logger(__name__)


def create_grid(
    gpd_ds: GeoSpatialDataset,
    distance: float,
    units: Unit = None,
    remove_unused_grids: bool = False,
    remove_water_grids: bool = True,
    add_col_row_ids: bool = False,
    add_grid_name: bool = False,
    save_as_file: str = None,
) -> GeoSpatialDataset:
    """
    Generates a new grid.

    Args:
        gpd_ds: geospatial data of the universe,
        distance: grid measure,
        units: grid measure units,
        remove_unused_grids:
        remove_water_grids:
        add_col_row_ids:
        add_grid_name:
        save_as_file:

    Returns:
        grid as a geospatial dataset
    """

    # Get the extent of the shapefile
    total_bounds = gpd_ds.data.total_bounds

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
            point=(min_x, min_y), distance=distance, direction=0, unit=units
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

    grid = gpd.GeoDataFrame(
        geom_array, columns=["geometry", "grid_row_id", "grid_col_id"]
    )

    print(f"Grid size is: ({row_id} x {col_id})")

    grid["grid_id"] = grid.index.tolist()

    if not add_col_row_ids:
        grid = grid.drop(labels=["grid_row_id", "grid_col_id"], axis=1)

    # standardize crs projections
    grid.crs = gpd_ds.data.crs

    if remove_unused_grids:
        cols_names = grid.columns.values
        grid = grid.sjoin(gpd_ds.data, how="inner", rsuffix="right")
        grid = grid[cols_names]

    if remove_water_grids:
        census = gpd.read_file(settings.databases_census_2021.local.shp)
        census = census.to_crs(epsg=4269)
        grid.crs = census.crs
        cols_names = grid.columns.values
        grid = grid.sjoin(census, how="inner", rsuffix="census_right")
        grid = grid[cols_names]
        grid = grid.sort_values(by="grid_id", ascending=False)

    if add_grid_name:
        grid["Grid Name"] = (
            "("
            + grid["grid_row_id"].astype(str)
            + ","
            + grid["grid_col_id"].astype(str)
            + ")"
        )

    if save_as_file:
        fig, ax = plt.subplots(figsize=(15, 15))
        gpd.GeoSeries(grid["geometry"]).boundary.plot(ax=ax)
        gpd.GeoSeries(gpd_ds.data["geometry"]).boundary.plot(ax=ax, color="red")
        fig.savefig(save_as_file + ".pdf")
        grid.to_file(save_as_file)

    grid = grid.drop_duplicates()

    return GeoSpatialDataset(data=grid)


def get_grid(
    distance: int,
    units: Unit,
    remove_unused_grids: bool = False,
    remove_water_grids: bool = True,
    add_col_row_ids: bool = False,
    add_grid_name: bool = False,
) -> GeoSpatialDataset:
    grid_mtl = GeoSpatialDataset()
    filepath = settings.grid_shp_filepath.format(
        distance=str(distance), units=str(units.value)
    )

    if os.path.exists(filepath):
        logger.info("Reading grid....")
        grid_mtl.load_from_path(filepath)
    else:
        # universe
        lim_admin_mtl = GeoSpatialDataset()
        lim_admin_mtl.load_from_path(
            settings.databases[ExternalDatabases.LIM_ADMIN_MTL].local.shp
        )

        # create grid
        grid_mtl = create_grid(
            gpd_ds=lim_admin_mtl,
            distance=distance,
            units=units,
            remove_unused_grids=remove_unused_grids,
            remove_water_grids=remove_water_grids,
            add_col_row_ids=add_col_row_ids,
            add_grid_name=add_grid_name,
            save_as_file=filepath,
        )

    return grid_mtl


def get_grid_with_dates(
    distance: int, units: Unit, start_date=None, end_date=None, freq=None
) -> gpd:
    grid_mtl = get_grid(distance=distance, units=units, remove_unused_grids=True)

    dates = pd.date_range(start=start_date, end=end_date, freq=freq)

    master_grid = grid_mtl.assign(DATE=[dates] * len(grid_mtl)).explode(
        "DATE", ignore_index=True
    )
    master_grid["YEAR"] = master_grid["DATE"].dt.year
    master_grid["QUARTER"] = master_grid["DATE"].dt.quarter

    # df_grid_date = df_grid_date.drop(["DATE"], axis=1)
    master_grid = master_grid.sort_values(["grid_id", "DATE"], ascending=[True, True])

    # df_grid_date.to_file(f"out/data/grid/mtl_grid_dates_{str(distance)}_{str(units.value)}.shp")
    master_grid.drop(["geometry"], axis=1).to_csv(
        f"out/model_data/aggregated/grid/grid_dates_quarterly_{str(distance)}{str(units.value)}.csv",
        index=False,
    )

    return master_grid


def visualize_grid(
    distance: int, units: Unit, out_dir: str = "resources/docs/grid/"
) -> None:
    # universe
    lim_admin_mtl = gpd.read_file(settings.lim_admin_mtl.local.shp)

    # create grid
    grid = create_grid(
        geo_data=lim_admin_mtl,
        distance=distance,
        units=units,
        remove_water_grids=True,
        remove_unused_grids=True,
    )

    # create a new map
    min_x, min_y, max_x, max_y = lim_admin_mtl.total_bounds

    x_map = mean([max_x, min_x])
    y_map = mean([max_y, min_y])

    mtl_grid = folium.Map(location=[y_map, x_map], zoom_start=12, tiles="OpenStreetMap")

    # visualize
    style_function = lambda x: {
        "fillColor": "#ffffff",
        "color": "#000000",
        "fillOpacity": 0.0,
        "weight": 0.3,
    }
    highlight_function = lambda x: {
        "fillColor": "#000000",
        "color": "#000000",
        "fillOpacity": 0.50,
        "weight": 0.1,
    }

    geo_tooltip = folium.features.GeoJson(
        grid,
        style_function=style_function,
        control=False,
        highlight_function=highlight_function,
        tooltip=folium.features.GeoJsonTooltip(
            fields=["Grid Name", "grid_id", "NOM"],
            aliases=["Grid Name: ", "Grid ID: ", "Borough: "],
            style=(
                "background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;"
            ),
        ),
    )
    mtl_grid.add_child(geo_tooltip)
    mtl_grid.keep_in_front(geo_tooltip)

    mtl_grid.save(f"{out_dir}/mtl_only_grid_500m.html")
