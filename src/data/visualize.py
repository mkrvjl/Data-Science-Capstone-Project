import folium
import geopandas as gpd
import utils.columns as dt_cols
import utils.constants as coord
from haversine import Unit
from config.settings import ProjectSettings
from data.fetch import firefighter_stations_data, fire_incidents_data
from data.spatial import create_grid

settings = ProjectSettings()


def create_fire_stations_graph() -> None:

    # read firefighters dataset
    fire_stations = firefighter_stations_data()

    # open a new map
    mtl_map = folium.Map(
        location=[coord.mtl[dt_cols.LAT_COL], coord.mtl[dt_cols.LONG_COL]],
        zoom_start=11,
        tiles="Stamen Toner",
    )

    for idx in range(len(fire_stations)):
        if not fire_stations.loc[idx, "DATE_FIN"]:
            folium.Circle(
                location=(
                    fire_stations.loc[idx, "LATITUDE"],
                    fire_stations.loc[idx, "LONGITUDE"],
                ),
                radius=200,
                color="#3189FF",
                fill=True,
                fill_opacity=1,
            ).add_to(mtl_map)
        else:
            folium.Circle(
                location=(
                    fire_stations.loc[idx, "LATITUDE"],
                    fire_stations.loc[idx, "LONGITUDE"],
                ),
                radius=200,
                color="crimson",
                fill=True,
                fill_opacity=1,
            ).add_to(mtl_map)

    mtl_map.save(f"{settings.out_dir}/html/fire_stations_mtl.html")


def create_choropleth_stations() -> None:
    # read firefighters dataset
    fire_stations = firefighter_stations_data(remove_closed=True)

    # read montreal boroughs
    lim_admin_mtl = gpd.read_file(settings.lim_admin_mtl.local.shp)

    # generate geopandas df
    gdf = gpd.GeoDataFrame(
        data=fire_stations,
        geometry=gpd.points_from_xy(
            fire_stations.LONGITUDE, fire_stations.LATITUDE
        ),
    )

    # Make sure they're using the same projection reference
    gdf.crs = lim_admin_mtl.crs
    gdf = gdf.sjoin(lim_admin_mtl, how="left")

    gdf_occ = gdf.groupby("CODEMAMH").size().to_frame("TOT_CASERNES")
    gdf = gdf.join(gdf_occ, on="CODEMAMH", how="left")

    # open a new map
    mtl_map = folium.Map(
        location=[
            coord.mtl[dt_cols.LAT_COL],
            coord.mtl[dt_cols.LONG_COL],
        ],
        zoom_start=11,
        tiles="Stamen Toner",
    )

    folium.Choropleth(
        geo_data=lim_admin_mtl,
        data=gdf,
        columns=["CODEMAMH", "TOT_CASERNES"],
        key_on="feature.properties.CODEMAMH",
        fill_color="YlOrRd",
        nan_fill_color="White",  # Use white color if there is no data available for the county
        fill_opacity=0.7,
        line_opacity=0.2,
        legend_name="Number of fire stations per borough",  # title of the legend
        highlight=True,
        bins=gdf[["TOT_CASERNES"]].max()[0] - 1,
        line_color="black",
    ).add_to(mtl_map)

    for idx in range(len(fire_stations)):
        folium.Circle(
            location=(gdf.loc[idx, "LATITUDE"], gdf.loc[idx, "LONGITUDE"]),
            radius=200,
            color="#3189FF",
            fill=True,
            line_opacity=0.2,
            fill_opacity=0.7,
            tooltip=f"<b>Fire Station: </b>{gdf.loc[idx,'CASERNE']}<br>"
            f"<b>Borough: </b> {gdf.loc[idx,'NOM']}<br>",
        ).add_to(mtl_map)

    mtl_map.save(f"{settings.out_dir}/html/fire_stations_mtl_choropleth.html")


def create_fire_incidents_yearly():
    # ------------------------------------------------------------------------------------------------------------------
    # 1. Read dataframes
    # ------------------------------------------------------------------------------------------------------------------

    # incidents mtl
    inc_mtl = fire_incidents_data()

    # mtl geodata
    lim_admin_mtl = gpd.read_file(settings.lim_admin_mtl.local.shp)

    # read firefighters dataset
    fire_stations = firefighter_stations_data(remove_closed=True)

    # # select only last year
    # inc_mtl = inc_mtl[inc_mtl['CREATION_DATE_TIME'].dt.year == 2022]

    # add year
    inc_mtl["YEAR"] = inc_mtl["CREATION_DATE_TIME"].dt.year

    # ------------------------------------------------------------------------------------------------------------------
    # 2. Convert to GeoPandas
    # ------------------------------------------------------------------------------------------------------------------
    # create lim_admin_mtl df
    fires_gpd = gpd.GeoDataFrame(
        inc_mtl, geometry=gpd.points_from_xy(inc_mtl.LONGITUDE, inc_mtl.LATITUDE)
    )

    # create grid
    #grid = create_grid(mtl_geo, 0.0025)
    grid = create_grid(
        geo_data=lim_admin_mtl,
        distance=500,
        units=Unit.METERS,
        save_as_file=True,
        )
    grid = grid.reset_index()

    # generate geopandas df
    firestation_geo = gpd.GeoDataFrame(
        data=fire_stations,
        geometry=gpd.points_from_xy(
            fire_stations.LONGITUDE, fire_stations.LATITUDE
        ),
    )

    # ------------------------------------------------------------------------------------------------------------------
    # 3. Convert / Join spatial
    # ------------------------------------------------------------------------------------------------------------------

    # main fire dataframe
    fires_gpd.crs = grid.crs
    fires_gpd = fires_gpd.sjoin(grid, how="left", rsuffix="right")

    #fires_gpd.to_file("src/data/interventions_sim/cleaned_grid_donneesouvertes-interventions-interventions_sim.geojson", driver='GeoJSON')

    # goup by grid and count
    incident_count_by_grid_id = (
        fires_gpd.groupby(["index", "Grid Name"], sort=True)["index"]
        .count()
        .to_frame("TOT_INC_GRID")
        .reset_index()
    )
    incident_count_by_grid_id.sort_values("TOT_INC_GRID", ascending=False, inplace=True)

    incident_by_year = (
        fires_gpd.groupby(["index", "YEAR", "Grid Name"], sort=True)["index"]
        .count()
        .to_frame("TOT_BY_YEAR")
        .reset_index()
    )

    incident_by_year = incident_by_year.pivot(
        index="index", columns="YEAR", values="TOT_BY_YEAR"
    )

    incident_by_year.reset_index()
    incident_by_year = incident_by_year.fillna(0)

    incident_count_by_grid_id = incident_count_by_grid_id.merge(
        incident_by_year, left_on="index", right_on="index", suffixes=("", "_right")
    )

    # convert fire station
    firestation_geo.crs = lim_admin_mtl.crs
    firestation_geo = firestation_geo.sjoin(lim_admin_mtl, how="left")

    firestation_grouped = firestation_geo.groupby("CODEMAMH").size().to_frame("TOT_CASERNES")
    firestation_geo = firestation_geo.join(firestation_grouped, on="CODEMAMH", how="left")

    # ------------------------------------------------------------------------------------------------------------------
    # 4. Visualize
    # ------------------------------------------------------------------------------------------------------------------

    # open a new map
    mtl_map = folium.Map(
        location=[
            coord.mtl[dt_cols.LAT_COL],
            coord.mtl[dt_cols.LONG_COL],
        ],
        zoom_start=11,
        tiles="Stamen Toner",
    )

    folium.Choropleth(
        geo_data=grid,
        name="Total Incidents",
        data=incident_count_by_grid_id,
        columns=["index", "TOT_INC_GRID"],
        key_on="feature.properties.index",
        fill_color="YlOrRd",
        nan_fill_color="White",  # Use white color if there is no data available for the county
        nan_fill_opacity=0.0,
        fill_opacity=0.5,
        line_opacity=0.0,
        legend_name="Number of fires",  # title of the legend
        highlight=True,
        bins=20,
        line_color="white",
    ).add_to(mtl_map)

    #### test
    incident_test = incident_count_by_grid_id.merge(
        grid, left_on="index", right_on="index", suffixes=("", "_right")
    )

    incident_test = gpd.GeoDataFrame(incident_test, geometry=incident_test.geometry)

    style_function = lambda x: {
        "fillColor": "#ffffff",
        "color": "#000000",
        "fillOpacity": 0.0,
        "weight": 0.0,
    }
    highlight_function = lambda x: {
        "fillColor": "#000000",
        "color": "#000000",
        "fillOpacity": 0.50,
        "weight": 0.1,
    }

    fields = ["Grid Name", "TOT_INC_GRID"] + incident_by_year.columns.values.astype(str).tolist()
    aliases = ["Grid Name: ", "Total Fires: "] + incident_by_year.columns.values.astype(str).tolist()

    geo_tooltip = folium.features.GeoJson(
        incident_test,
        style_function=style_function,
        control=False,
        highlight_function=highlight_function,
        tooltip=folium.features.GeoJsonTooltip(
            fields=fields,
            aliases=aliases,
            style=(
                "background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;"
            ),
        ),
    )
    mtl_map.add_child(geo_tooltip)
    mtl_map.keep_in_front(geo_tooltip)

    ##### tests

    for idx in range(len(fire_stations)):
        folium.Circle(
            location=(firestation_geo.loc[idx, "LATITUDE"], firestation_geo.loc[idx, "LONGITUDE"]),
            radius=50,
            color="#3189FF",
            fill=True,
            line_opacity=0.2,
            fill_opacity=0.7,
            tooltip=f"<b>Fire Station: </b>{firestation_geo.loc[idx, 'CASERNE']}<br>"
            f"<b>Borough: </b> {firestation_geo.loc[idx, 'NOM']}<br>",
        ).add_to(mtl_map)

    folium.LayerControl().add_to(mtl_map)

    mtl_map.save(f"{settings.out_dir}/fire_incidents_grid.html")
