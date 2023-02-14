import geopandas as gpd
from haversine import Unit
from config.settings import ProjectSettings
from data.fetch import fire_incidents_data, crime_data
from data.spatial import create_grid

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
        fire_inc_mtl, geometry=gpd.points_from_xy(fire_inc_mtl.LONGITUDE, fire_inc_mtl.LATITUDE)
    )
    fires_inc_gpd.drop(
        columns=['LONGITUDE', 'LATITUDE'],
        axis=1,
        inplace=True,
    )

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

    #fires_with_grid.to_file(f"src/data/interventions_sim/fire_incidents_mtl_grid_{grid_distance}{grid_units.value}.geoJSON", driver='GeoJSON')

    #fires_with_grid['CREATION_DATE_TIME'] = fires_with_grid['CREATION_DATE_TIME'].astype(str)
    #fires_with_grid.to_file(f"src/data/interventions_sim/fire_incidents_mtl_grid_{grid_distance}{grid_units.value}.shp")

    # ------------------------------------------------------------------------------------------------------------------
    # 4. Convert / Join spatial
    # ------------------------------------------------------------------------------------------------------------------
    if aggregate_data:
        #grouped_multiple = fires_with_grid.groupby(['Grid Name', 'YEAR', 'MONTH', 'DAY', 'QUART', 'DESCRIPTION_GROUPE', 'CASERNE', 'NOMBRE_UNITES'])['INCIDENT_NBR'].count().reset_index()
        grouped_multiple = fires_with_grid.groupby(
            ['index_grid', 'Grid Name', 'YEAR', 'MONTH', 'DAY', 'QUART', 'DESCRIPTION_GROUPE'])[
            #['index_grid', 'Grid Name', 'YEAR', 'MONTH', 'QUART', 'DESCRIPTION_GROUPE'])[
            'INCIDENT_NBR'].count().reset_index()

    grouped_multiple = grouped_multiple.sort_values(by='INCIDENT_NBR', ascending=False).reset_index(drop=True)
    grouped_multiple.to_csv(f"{settings.out_dir}/data/fire-insidents-clean.csv")

    #grouped_multiple.head(10)

def clean_crime_dataset(
    remove_not_required: bool = False,
    add_categories: bool = False,
    grid_distance: float = 500,
    grid_units: Unit = Unit.METERS,
    save_as_file: bool = False,
):
    # ------------------------------------------------------------------------------------------------------------------
    # 1. Read dataframes
    # ------------------------------------------------------------------------------------------------------------------

    # incidents mtl
    crime_mtl = crime_data()

    # mtl geodata
    lim_admin_mtl = gpd.read_file(settings.lim_admin_mtl.local.shp)

    # ------------------------------------------------------------------------------------------------------------------
    # 2. Convert to GeoPandas
    # ------------------------------------------------------------------------------------------------------------------
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
    crime_mtl.crs = grid.crs
    crimes_with_grid = crime_mtl.sjoin(grid, how="left", rsuffix="grid")
    # preserve dtypes
    #  Alternative -> fires_with_grid.astype({'index_grid': 'int64'})
    #crimes_with_grid.astype(grid.dtypes)

    if add_categories:
        # add date categories
        crimes_with_grid["YEAR"] = crimes_with_grid["DATE"].dt.year
        crimes_with_grid["MONTH"] = crimes_with_grid["DATE"].dt.month
        crimes_with_grid["DAY"] = crimes_with_grid["DATE"].dt.day

    # crimes_with_grid.drop('geometry', axis=1).to_csv(
    #     f"src/data/crime/crime_mtl_grid_{grid_distance}{grid_units.value}.csv")
    #
    # crimes_with_grid.to_file(f"src/data/crime/crime_mtl_grid_{grid_distance}{grid_units.value}.geoJSON", driver='GeoJSON')

    # crimes_with_grid['DATE'] = crimes_with_grid['DATE'].astype(str)
    # crimes_with_grid.to_file(f"src/data/crime/crime_mtl_grid_{grid_distance}{grid_units.value}.shp")
