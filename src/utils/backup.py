from folium.plugins import FastMarkerCluster
from datetime import datetime
import numpy as np
import json

import geopandas as gpd
import folium

import data.old_code.fetch as get_df
from scipy.spatial import cKDTree
from utils.distances import calculate_distance_vector
from data.old_code.clean import create_grid
import utils.constants as coord
import utils.columns as dt_cols


def create_fire_stations_graph_backup(dt_cols=None, test: bool = True):
    if test:
        raise NotImplementedError("Not implemented!")

    # read incidents dataset
    incidents_new = get_df.fire_incidents()

    # count total incidents
    total_incidents = (
        incidents_new.groupby(dt_cols.BOROUGH_COL)[dt_cols.INC_ID_COL].count().to_dict()
    )

    # count last year only
    total_last_yr = (
        incidents_new[
            incidents_new[dt_cols.INC_DATE_COL].dt.year == datetime.now().year - 1
        ]
        .groupby(dt_cols.BOROUGH_COL)[dt_cols.INC_ID_COL]
        .count()
        .to_dict()
    )

    # read firefighters dataset
    ff_stations_mtl = get_df.firefighter_stations_data()

    #
    mtl_geo_data = get_df.mtl_arrond_geo_data()

    # open a new map
    m = folium.Map(
        location=[coord.mtl[dt_cols.LAT_COL], coord.mtl[dt_cols.LONG_COL]],
        zoom_start=11,
        tiles="Stamen Toner",
    )

    FastMarkerCluster(
        list(zip(incidents_new["LATITUDE"], incidents_new["LONGITUDE"]))
    ).add_to(m)

    for row in ff_stations_mtl.index:
        folium.Marker(
            location=[
                ff_stations_mtl["LATITUDE"][row],
                ff_stations_mtl["LONGITUDE"][row],
            ],
            tooltip=f"Caserne No. {ff_stations_mtl['CASERNE'][row]}",
        ).add_to(m)

    # m.save("src/img/clusterized.html")

    m_arro = folium.Map(
        location=[coord.mtl[dt_cols.LAT_COL], coord.mtl[dt_cols.LONG_COL]],
        zoom_start=11,
        tiles="Stamen Toner",
    )

    folium.GeoJson(
        mtl_geo_data,
        tooltip=folium.GeoJsonTooltip(fields=["NOM"], aliases=["Name: "]),
        style_function=lambda x: {"color": "#255635", "fillColor": "#255635"},
        highlight_function=lambda x: {"fillColor": "red"},
    ).add_to(m_arro)

    for idx in range(len(ff_stations_mtl)):
        if not ff_stations_mtl.loc[idx, "DATE_FIN"]:
            folium.Circle(
                location=(
                    ff_stations_mtl.loc[idx, "LATITUDE"],
                    ff_stations_mtl.loc[idx, "LONGITUDE"],
                ),
                radius=200,
                color="#3189FF",
                fill=True,
                fill_opacity=1,
                # tooltip=f"<b>Caserne No. :</b>{row['CASERNE']}<br>"
                #         f"<b>Incidents Last Yr :</b> {total_last_yr[row['BOROUGH']]}<br>"
                #         f"<b>Total Incidents :</b> {total_incidents[row['BOROUGH']]}<br>"
            ).add_to(m_arro)
        else:
            folium.Circle(
                location=(
                    ff_stations_mtl.loc[idx, "LATITUDE"],
                    ff_stations_mtl.loc[idx, "LONGITUDE"],
                ),
                radius=200,
                color="crimson",
                fill=True,
                fill_opacity=1,
                # tooltip=f"<b>Caserne No. :</b>{ff_stations_mtl.loc[idx,'CASERNE']}<br>"
                #          f"<b>Incidents Last Yr :</b> {total_last_yr[ff_stations_mtl.loc[idx,'BOROUGH']]}<br>"
                #          f"<b>Total Incidents :</b> {total_incidents[ff_stations_mtl.loc[idx,'BOROUGH']]}<br>"
            ).add_to(m_arro)

    file_path = "src/img/mtl_casernes_topo.html"

    m_arro.save(file_path)

    # img_data = m._to_png(5)
    # img = Image.open(io.BytesIO(img_data))
    # img.save('src/img/mtl_casernes_topo.png')


def create_fire_incidents_yearly():
    # generate geopandas df
    inc_mtl = get_df.fire_incidents()

    # select only last year
    inc_mtl = inc_mtl[inc_mtl["CREATION_DATE_TIME"].dt.year == 2022]

    # create lim_admin_mtl df
    mtl_inc_2k22 = gpd.GeoDataFrame(
        inc_mtl, geometry=gpd.points_from_xy(inc_mtl.LONGITUDE, inc_mtl.LATITUDE)
    )

    # read montreal lim_admin_mtl info
    mtl_geo = gpd.read_file(
        "src/data/lim_admin_mtl/limites-administratives-agglomeration.shp"
    )

    # create grid
    grid = create_grid(mtl_geo, 0.005)
    grid = grid.reset_index()

    # join spatial
    mtl_inc_2k22.crs = grid.crs
    mtl_inc_2k22 = mtl_inc_2k22.sjoin(grid, how="left", rsuffix="right")

    ## Visualize
    incident_count_by_grid_id = (
        mtl_inc_2k22.groupby("index")["index"]
        .count()
        .to_frame("TOT_INC_GRID")
        .reset_index()
    )

    # open a new map
    mtl_map = folium.Map(
        location=[coord.mtl[dt_cols.LAT_COL], coord.mtl[dt_cols.LONG_COL]],
        zoom_start=11,
        # tiles=None,
        tiles="Stamen Toner",
    )
    # folium.TileLayer('CartoDB positron', name="Light Map", control=False).add_to(mtl_map)
    # folium.TileLayer('Stamen Toner', name="Dark Map", control=False).add_to(mtl_map)
    # folium.TileLayer('OpenStreetMap', name="Bright Map", control=False).add_to(mtl_map)

    folium.Choropleth(
        geo_data=grid,
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
        name="Total Incidents 2022",
    ).add_to(mtl_map)

    # style_function = lambda x: {'nan_fill_opacity': 0.0,
    #                             'line_opacity': 0.0,
    #                             'weight': 0.0,
    #                             'line_color': 'white'}

    # NIL = folium.features.GeoJson(
    #     grid,
    #     style_function=style_function,
    #     control=False,
    #     tooltip=folium.features.GeoJsonTooltip(
    #         fields=['index'],
    #         aliases=['Grid Index: '],
    #         style=("background-color: white; font-family: arial; font-size: 12px; padding: 10px;")
    #     ),
    # )
    #
    # mtl_map.add_child(NIL)
    # mtl_map.keep_in_front(NIL)

    # read firefighters dataset
    ff_stations_mtl = get_df.firefighter_stations_data(remove_closed=True)

    # generate geopandas df
    gdf = gpd.GeoDataFrame(
        data=ff_stations_mtl,
        geometry=gpd.points_from_xy(
            ff_stations_mtl.LONGITUDE, ff_stations_mtl.LATITUDE
        ),
    )

    # Make sure they're using the same projection reference
    gdf.crs = mtl_geo.crs
    gdf = gdf.sjoin(mtl_geo, how="left")

    gdf_occ = gdf.groupby("CODEMAMH").size().to_frame("TOT_CASERNES")
    gdf = gdf.join(gdf_occ, on="CODEMAMH", how="left")

    for idx in range(len(ff_stations_mtl)):
        folium.Circle(
            location=(gdf.loc[idx, "LATITUDE"], gdf.loc[idx, "LONGITUDE"]),
            radius=50,
            color="#3189FF",
            fill=True,
            line_opacity=0.2,
            fill_opacity=0.7,
            tooltip=f"<b>Fire Station: </b>{gdf.loc[idx, 'CASERNE']}<br>"
            f"<b>Borough: </b> {gdf.loc[idx, 'NOM']}<br>",
        ).add_to(mtl_map)

    folium.LayerControl().add_to(mtl_map)

    mtl_map.save("src/img/test.html")

    print("Done!!!")


def get_nearest_borne(gd_a, gd_b):
    # transform to array
    n_a = np.array(list(gd_a.geometry.apply(lambda x: (x.x, x.y))))
    n_b = np.array(list(gd_b.geometry.apply(lambda x: (x.x, x.y))))

    # get nearest
    btree = cKDTree(n_b)
    dist, idx = btree.query(n_a, k=1)

    # retrieve from df 2
    gd_b_nearest = gd_b.iloc[idx].drop(columns="geometry").reset_index(drop=True)

    # calculate distance
    dis_to_closest = calculate_distance_vector(
        list(zip(gd_a[["LATITUDE"]].to_numpy(), gd_a[["LONGITUDE"]].to_numpy())),
        list(
            zip(
                gd_b_nearest[["LATITUDE"]].to_numpy(),
                gd_b_nearest[["LONGITUDE"]].to_numpy(),
            )
        ),
    )

    # aggregate
    gdf = gd_a.copy()
    gdf["ID_CLOSEST_BORNE"] = gd_b_nearest[["ID_AQ_PNT"]]
    gdf["BORNE_DIST_M"] = dis_to_closest

    return gdf


def geo_test():
    # this merges multiple prep into fire database

    # read incidents dataset
    inc_mtl = get_df.fire_incidents()

    # read firefighters dataset
    ff_stations_mtl = get_df.firefighter_stations_data()

    inc_mtl = inc_mtl.merge(
        ff_stations_mtl[["CASERNE", "LATITUDE", "LONGITUDE"]].rename(
            columns={"LATITUDE": "CASERNE_LATITUDE", "LONGITUDE": "CASERNE_LONGITUDE"}
        ),
        left_on="CASERNE",
        right_on="CASERNE",
    )

    inc_mtl["DIST_CASERNE_M"] = calculate_distance_vector(
        list(zip(inc_mtl[["LATITUDE"]].to_numpy(), inc_mtl[["LONGITUDE"]].to_numpy())),
        list(
            zip(
                inc_mtl[["CASERNE_LATITUDE"]].to_numpy(),
                inc_mtl[["CASERNE_LONGITUDE"]].to_numpy(),
            )
        ),
    )

    # generate geopandas df
    gdf = gpd.GeoDataFrame(
        inc_mtl, geometry=gpd.points_from_xy(inc_mtl.LONGITUDE, inc_mtl.LATITUDE)
    )

    # read montreal boroughs
    mtl = gpd.read_file(
        "src/data/lim_admin_mtl/limites-administratives-agglomeration.shp"
    )

    # Make sure they're using the same projection reference
    # gdf.crs = mtl.crs
    # join_left_df = gdf.sjoin(mtl, how='left')
    # bornes = geopandas.read_file("src/data/borne_incendie/AQU_BORNEINCENDIE_P_J_point.shp", crs={'init': 'epsg:4326'})

    with open(
        "src/data/borne_incendie/ati_geomatique.aqu_borneincendie_p_j.json", "r"
    ) as j:
        json_content = json.loads(j.read())

    gdf_b = gpd.GeoDataFrame.from_features(json_content["features"])
    print(gdf_b.head())

    # Make sure they're using the same projection reference
    gdf_b.crs = gdf.crs
    df_with_bornes = get_nearest_borne(gdf, gdf_b)

    df_with_bornes.drop("geometry", axis=1).to_csv(r"src/out/augmented_data.csv")

    print(df_with_bornes)
