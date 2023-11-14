import geopandas as gpd
import pandas as pd
from haversine import Unit
from config.settings import ProjectSettings
from data.old_code.fetch import fire_incidents_data
from data.old_code.clean.spatial import get_grid

settings = ProjectSettings()


def clean_fire_data(
    add_time_categories: bool = True,
    remove_unrelevant: bool = True,
    grid_distance: int = 500,
    grid_units: Unit = Unit.METERS,
) -> pd.DataFrame:
    # incidents mtl
    fire_inc_mtl = fire_incidents_data(
        remove_unrelevant=remove_unrelevant, add_time_categories=add_time_categories
    )

    # create lim_admin_mtl df
    fires_inc_gpd = gpd.GeoDataFrame(
        fire_inc_mtl,
        geometry=gpd.points_from_xy(fire_inc_mtl.LONGITUDE, fire_inc_mtl.LATITUDE),
    )

    fires_inc_gpd.drop(columns=["LONGITUDE", "LATITUDE"], axis=1, inplace=True)

    # get mtl with grid
    grid_mtl = get_grid(
        distance=grid_distance,
        units=grid_units,
        remove_unused_grids=True,
        add_col_row_ids=False,
        add_grid_name=False,
    )

    # main fire dataframe
    fires_inc_gpd.crs = grid_mtl.crs
    fires_with_grid = fires_inc_gpd.sjoin(grid_mtl, how="left", rsuffix="grid")
    fires_with_grid = fires_with_grid.drop_duplicates(
        subset=["INCIDENT_NBR"], keep="first"
    )

    fires_with_grid = fires_with_grid.dropna(subset=["grid_id"])

    fires_with_grid.drop(columns=["index_grid"], axis=1, inplace=True)

    # preserve dtypes
    fires_with_grid = fires_with_grid.astype({"grid_id": "int64"})

    # save files:
    fires_with_grid = fires_with_grid.drop("geometry", axis=1)

    fires_with_grid.to_csv(
        f"out/model_data/clean/fires/fire_incidents_mtl_clean_{grid_distance}{grid_units.value}.csv"
    )

    return pd.DataFrame(fires_with_grid)
