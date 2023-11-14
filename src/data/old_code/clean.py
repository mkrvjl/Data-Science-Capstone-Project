import geopandas as gpd
import numpy as np
import pandas as pd
from haversine import Unit
from config.settings import ProjectSettings
from data.old_code.fetch import fire_incidents_data, crime_data
from data.old_code.clean import create_grid, get_grid

settings = ProjectSettings()


def clean_fire_incidents(
    remove_unrelevant: bool = False,
    add_time_categories: bool = False,
    aggregate_data: bool = True,
    grid_distance: float = 500,
    grid_units: Unit = Unit.METERS,
    save_as_file: bool = False,
):
    # ------------------------------------------------------------------------------------------------------------------
    # 1. Read dataframes
    # ------------------------------------------------------------------------------------------------------------------

    # incidents mtl
    fire_inc_mtl = fire_incidents_data(
        remove_unrelevant=remove_unrelevant,
        add_time_categories=add_time_categories,
        start_year=2018,
    )

    # mtl geodata
    lim_admin_mtl = gpd.read_file(settings.lim_admin_mtl.local.shp)

    # ------------------------------------------------------------------------------------------------------------------
    # 2. Convert to GeoPandas
    # ------------------------------------------------------------------------------------------------------------------
    # create lim_admin_mtl df
    fires_inc_gpd = gpd.GeoDataFrame(
        fire_inc_mtl,
        geometry=gpd.points_from_xy(fire_inc_mtl.LONGITUDE, fire_inc_mtl.LATITUDE),
    )

    fires_inc_gpd.drop(columns=["LONGITUDE", "LATITUDE"], axis=1, inplace=True)

    # create grid
    grid = create_grid(
        geo_data=lim_admin_mtl,
        distance=grid_distance,
        units=grid_units,
        save_as_file=save_as_file,
    )

    # ------------------------------------------------------------------------------------------------------------------
    # 3. Convert / Join spatial
    # ------------------------------------------------------------------------------------------------------------------
    # main fire dataframe
    fires_inc_gpd.crs = grid.crs
    fires_with_grid = fires_inc_gpd.sjoin(grid, how="left", rsuffix="grid")
    # preserve dtypes
    #  Alternative -> fires_with_grid.astype({'index_grid': 'int64'})
    fires_with_grid.astype(grid.dtypes)

    # save files:
    # print(fires_with_grid.columns.values)
    # fires_with_grid.drop('geometry', axis=1).to_csv(f"src/data/interventions_sim/fire_incidents_mtl_grid_{grid_distance}{grid_units.value}.csv")

    # fires_with_grid.to_file(f"src/data/interventions_sim/fire_incidents_mtl_grid_{grid_distance}{grid_units.value}.geoJSON", driver='GeoJSON')

    # fires_with_grid['CREATION_DATE_TIME'] = fires_with_grid['CREATION_DATE_TIME'].astype(str)
    # fires_with_grid.to_file(f"src/data/interventions_sim/fire_incidents_mtl_grid_{grid_distance}{grid_units.value}.shp")

    # ------------------------------------------------------------------------------------------------------------------
    # 4. Convert / Join spatial
    # ------------------------------------------------------------------------------------------------------------------
    if aggregate_data:
        # grouped_multiple = fires_with_grid.groupby(['Grid Name', 'YEAR', 'MONTH', 'DAY', 'QUART', 'DESCRIPTION_GROUPE', 'CASERNE', 'NOMBRE_UNITES'])['INCIDENT_NBR'].count().reset_index()
        grouped_multiple = (
            fires_with_grid.groupby(
                ["index_grid", "Grid Name", "DATE", "QUART", "DESCRIPTION_GROUPE"]
            )["INCIDENT_NBR"]
            .count()
            .reset_index()
        )

    one_hot = pd.get_dummies(grouped_multiple["DESCRIPTION_GROUPE"])
    one_hot = one_hot.multiply(grouped_multiple["INCIDENT_NBR"], axis="index")
    grouped_multiple = grouped_multiple.join(one_hot)
    grouped_multiple = grouped_multiple.sort_values(
        by="INCIDENT_NBR", ascending=False
    ).reset_index(drop=True)
    grouped_multiple = grouped_multiple.drop(
        ["DESCRIPTION_GROUPE", "INCIDENT_NBR"], axis=1
    )
    grouped_multiple.to_csv(f"{settings.out_dir}/data/fire-insidents-clean.csv")

    # grouped_multiple.head(10)


def clean_fire_incidents_v2(
    file_path: str,
    remove_unrelevant: bool = True,
    add_time_categories: bool = True,
    aggregate_data: bool = True,
    grid_distance: float = 500,
    grid_units: Unit = Unit.METERS,
    save_as_file: bool = False,
):
    # incidents mtl
    fire_inc_mtl = fire_incidents_data(
        remove_unrelevant=remove_unrelevant,
        add_time_categories=add_time_categories,
        start_year=2018,
    )

    fire_inc_mtl_cols = np.append(fire_inc_mtl.columns.values, "IS_FIRE")

    #
    fire_description = pd.read_csv(
        "resources/integrated/CleanIncidentType.csv", encoding="ISO-8859-1"
    )

    fire_inc_mtl = fire_inc_mtl.merge(
        fire_description,
        how="left",
        left_on="INCIDENT_TYPE_DESC",
        right_on="INCIDENT_TYPE_DESCRIPTION",
    )

    fire_inc_mtl = fire_inc_mtl[fire_inc_mtl_cols]

    fire_inc_mtl = fire_inc_mtl[fire_inc_mtl["IS_FIRE"].notna()]
    fire_inc_mtl["IS_FIRE"].astype("int32")

    # filter out
    fire_inc_mtl = fire_inc_mtl[fire_inc_mtl["IS_FIRE"] == 1]

    # mtl geodata
    lim_admin_mtl = gpd.read_file(settings.lim_admin_mtl.local.shp)

    # create lim_admin_mtl df
    fires_inc_gpd = gpd.GeoDataFrame(
        fire_inc_mtl,
        geometry=gpd.points_from_xy(fire_inc_mtl.LONGITUDE, fire_inc_mtl.LATITUDE),
    )

    fires_inc_gpd.drop(columns=["LONGITUDE", "LATITUDE"], axis=1, inplace=True)

    # create grid
    grid = create_grid(
        geo_data=lim_admin_mtl,
        distance=grid_distance,
        units=grid_units,
        save_as_file=save_as_file,
    )

    # main fire dataframe
    fires_inc_gpd.crs = grid.crs
    fires_with_grid = fires_inc_gpd.sjoin(grid, how="left", rsuffix="grid")
    # preserve dtypes
    #  Alternative -> fires_with_grid.astype({'index_grid': 'int64'})
    fires_with_grid.astype(grid.dtypes)

    fires_with_grid.drop(
        [
            "geometry",
            "CASERNE",
            "DIVISION",
            "NOMBRE_UNITES",
            "grid_row_id",
            "grid_col_id",
            "index_grid",
            "Grid Name",
        ],
        axis=1,
    ).to_csv(f"{settings.out_dir}/data/fire-with-definition.csv")

    # save files:
    # print(fires_with_grid.columns.values)
    # fires_with_grid.drop('geometry', axis=1).to_csv(f"src/data/interventions_sim/fire_incidents_mtl_grid_{grid_distance}{grid_units.value}.csv")

    # fires_with_grid.to_file(f"src/data/interventions_sim/fire_incidents_mtl_grid_{grid_distance}{grid_units.value}.geoJSON", driver='GeoJSON')

    # fires_with_grid['CREATION_DATE_TIME'] = fires_with_grid['CREATION_DATE_TIME'].astype(str)
    # fires_with_grid.to_file(f"src/data/interventions_sim/fire_incidents_mtl_grid_{grid_distance}{grid_units.value}.shp")

    # ------------------------------------------------------------------------------------------------------------------
    # 4. Convert / Join spatial
    # ------------------------------------------------------------------------------------------------------------------

    grouped_multiple = (
        fires_with_grid.groupby(["index_grid", "YEAR", "MONTH"])["INCIDENT_NBR"]
        .count()
        .reset_index()
    )

    grouped_multiple = grouped_multiple.sort_values(
        by="INCIDENT_NBR", ascending=False
    ).reset_index(drop=True)
    grouped_multiple.to_csv(file_path)

    # grouped_multiple.head(10)


def clean_crime_dataset(
    remove_not_required: bool = False,
    add_categories: bool = True,
    grid_distance: int = 500,
    grid_units: Unit = Unit.METERS,
    save_as_file: bool = False,
):
    # incidents mtl
    crime_mtl = crime_data()
    crime_mtl["DATE"] = pd.to_datetime(crime_mtl["DATE"])

    crime_mtl = crime_mtl.drop(["PDQ", "X", "Y"], axis=1)

    crime_mtl.to_csv(f"{settings.out_dir}/data/fire-with-definition.csv")

    # create grid
    grid = get_grid(distance=grid_distance, units=grid_units)

    # main fire dataframe
    crime_mtl.crs = grid.crs
    crimes_with_grid = crime_mtl.sjoin(grid, how="left", rsuffix="grid")
    # preserve dtypes
    #  Alternative -> fires_with_grid.astype({'index_grid': 'int64'})
    # crimes_with_grid.astype(grid.dtypes)

    if add_categories:
        # add date categories
        crimes_with_grid["YEAR"] = crimes_with_grid["DATE"].dt.year
        crimes_with_grid["MONTH"] = crimes_with_grid["DATE"].dt.month

    crimes_with_grid.drop("geometry", axis=1).to_csv(
        f"{settings.out_dir}/crime_mtl_grid_{grid_distance}{grid_units.value}.csv"
    )

    grouped_multiple = (
        crimes_with_grid.groupby(["index_grid", "YEAR", "MONTH"])["DATE"]
        .count()
        .reset_index()
    )

    grouped_multiple = grouped_multiple.rename(columns={"DATE": "total"})

    grouped_multiple = grouped_multiple.sort_values(
        by="total", ascending=False
    ).reset_index(drop=True)
    grouped_multiple.to_csv(
        f"{settings.out_dir}/data/to_integrate/crime-aggregated.csv"
    )


def clean_unite_evaluation_fonciere():
    # evaluation = gpd.read_file("src/data/unit_eval_fonciere/uniteevaluationfonciere.geojson", rows=5)
    evaluation = gpd.read_file(
        "src/data/unit_eval_fonciere/uniteevaluationfonciere.geojson"
    )
    data_proj = evaluation.to_crs(epsg=3035)
    data_proj["centroid"] = data_proj["geometry"].centroid
    evaluation["geometry"] = data_proj["centroid"].to_crs(epsg=4326)

    evaluation = evaluation.drop(
        columns=[
            "ID_UEV",
            "CIVIQUE_DEBUT",
            "CIVIQUE_FIN",
            "NOM_RUE",
            "MUNICIPALITE",
            "MATRICULE83",
            "NO_ARROND_ILE_CUM",
        ]
    )
    evaluation.to_file(
        "src/data/unit_eval_fonciere/clean_uniteevaluationfonciere.geojson",
        driver="GeoJSON",
    )


def calculate_risk(type_a_count: int = 0, type_b_count: int = 0, type_c_count: int = 0):
    if type_a_count >= 35 and type_b_count >= 98:
        risk = "high"
    elif 24 <= type_a_count < 35 and 36 <= type_b_count < 98 and type_c_count >= 3844:
        risk = "medium"
    else:
        risk = "Low"

    return risk


def clean_fire_incidents_v3(
    remove_unrelevant: bool = True,
    add_time_categories: bool = False,
    aggregate_data: bool = True,
    grid_distance: float = 500,
    grid_units: Unit = Unit.METERS,
    save_as_file: bool = False,
):
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

    # mtl with grid only
    grid_mtl = get_grid(distance=500, units=Unit.METERS, remove_unused_grids=True)

    # main fire dataframe
    fires_inc_gpd.crs = grid_mtl.crs
    fires_with_grid = fires_inc_gpd.sjoin(grid_mtl, how="left", rsuffix="grid")
    fires_with_grid = fires_with_grid.drop_duplicates(
        subset=["INCIDENT_NBR"], keep="first"
    )

    fires_with_grid = fires_with_grid.dropna(subset=["grid_id"])

    fires_with_grid.drop(
        columns=["index_grid", "grid_row_id", "grid_col_id", "NOM", "Grid Name"],
        axis=1,
        inplace=True,
    )

    # preserve dtypes
    #  Alternative -> fires_with_grid.astype(grid_mtl.dtypes)
    fires_with_grid = fires_with_grid.astype({"grid_id": "int64"})

    # save files:
    # print(fires_with_grid.columns.values)
    # fires_with_grid.drop('geometry', axis=1).to_csv(f"out/data/clean/fire_incidents_mtl_grid_{grid_distance}{grid_units.value}.csv")

    # fires_with_grid.to_file(f"src/data/interventions_sim/fire_incidents_mtl_grid_{grid_distance}{grid_units.value}.geoJSON", driver='GeoJSON')

    # fires_with_grid['CREATION_DATE_TIME'] = fires_with_grid['CREATION_DATE_TIME'].astype(str)
    # fires_with_grid.to_file(f"src/data/interventions_sim/fire_incidents_mtl_grid_{grid_distance}{grid_units.value}.shp")

    # groupby and count
    grouped_multiple = (
        fires_with_grid.groupby(["grid_id", "YEAR", "MONTH", "TYPE"])["INCIDENT_NBR"]
        .count()
        .reset_index()
    )

    grouped_multiple = grouped_multiple.rename(
        columns={"INCIDENT_NBR": "INCIDENT_COUNT"}
    )

    one_hot = pd.get_dummies(grouped_multiple["TYPE"])
    one_hot = one_hot.multiply(grouped_multiple["INCIDENT_COUNT"], axis="index")
    grouped_multiple = grouped_multiple.join(one_hot)

    grouped_multiple["incident_diff"] = grouped_multiple.INCIDENT_COUNT.diff()
    grouped_multiple = grouped_multiple.dropna()

    grouped_multiple.to_csv(
        f"{settings.out_dir}/data/clean/fire-incidents-agg-month.csv"
    )

    grouped_multiple.head(10)
