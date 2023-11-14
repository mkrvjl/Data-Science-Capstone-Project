from statistics import mean

import geopandas as gpd
import pandas as pd
from haversine import Unit

from config import settings
from config.settings import ProjectSettings
from data.old_code.fetch import fire_incidents_data
from data.old_code.clean import get_grid

# Interactive maps
import folium
from folium.plugins import HeatMap

settings = ProjectSettings()


def view_heat_map():
    # # read dataset
    # final_dataset_path = "out/model_data/final_dataset/03-dataset-clean_fill-mean_quarterly_grid_500m.csv"
    #
    # # parse dates
    # final_dataset = pd.read_csv(final_dataset_path, parse_dates=["DATE"])

    # incidents mtl
    inc_mtl = fire_incidents_data()

    # select only last year real fires
    inc_mtl = inc_mtl[inc_mtl["CREATION_DATE_TIME"].dt.year == 2022]
    inc_mtl = inc_mtl[inc_mtl["TYPE"] != "C"]

    # mtl geodata
    lim_admin_mtl = gpd.read_file(settings.lim_admin_mtl.local.shp)

    # create grid
    # grid = create_grid(
    #     geo_data=lim_admin_mtl,
    #     distance=500,
    #     units=Unit.METERS,
    #     remove_unused_grids=True,
    # )
    # grid = grid.reset_index()

    # create a new map
    min_x, min_y, max_x, max_y = lim_admin_mtl.total_bounds

    x_map = mean([max_x, min_x])
    y_map = mean([max_y, min_y])

    # Visualizing the fire incidents in a map
    fire_map = folium.Map(location=[y_map, x_map], zoom_start=11, tiles="Stamen Toner")

    heat_df = inc_mtl[["LATITUDE", "LONGITUDE"]]
    heat_df = heat_df.dropna(axis=0, subset=["LATITUDE", "LONGITUDE"])

    # heat_data = [[row['LATITUDE'], row['LONGITUDE']] for index, row in heat_df.iterrows()]
    heat_data = heat_df.values.tolist()

    HeatMap(
        heat_data,
        gradient={0.1: "blue", 0.3: "lime", 0.5: "yellow", 0.7: "orange", 1: "red"},
        min_opacity=0.05,
        max_opacity=0.9,
        radius=25,
        use_local_extrema=False,
    ).add_to(fire_map)

    fire_map.save(f"{settings.out_dir}/maps/fire_incidents_heat_map.html")


def get_prediction_color(values: int) -> int:
    # default value
    ret_val = 99

    # classify value
    if values[0][0] == values[0][1]:
        ret_val = 0
    elif values[0][0] != values[0][1]:
        ret_val = 1

    return ret_val


def view_model_evaluation(data: pd.DataFrame):
    # visualize_grid(
    #     distance=500,
    #     units=Unit.METERS,
    # )

    data["PREDICTION"] = [
        get_prediction_color(x) for x in zip(data[["Y_VAL", "Y_PRED"]].values)
    ]

    # create grid
    grid = get_grid(
        distance=500,
        units=Unit.METERS,
        remove_unused_grids=True,
        remove_water_grids=True,
        add_col_row_ids=False,
        add_grid_name=False,
    )

    min_x, min_y, max_x, max_y = grid.total_bounds

    x_map = mean([max_x, min_x])
    y_map = mean([max_y, min_y])

    grouped = data.groupby("DATE")

    for name, group in grouped:
        print(f"Adding {name.strftime('%Y-%m')}")

        save_maps(
            grid,
            group,
            name,
            x_map,
            y_map,
            plot_feature_name="PREDICTION",
            fill_color="OrRd",
            type_name="diff",
        )

        save_maps(
            grid,
            group,
            name,
            x_map,
            y_map,
            plot_feature_name="Y_PRED",
            type_name="pred",
        )

        save_maps(
            grid,
            group,
            name,
            x_map,
            y_map,
            plot_feature_name="Y_PRED",
            type_name="pred",
        )

        save_maps(
            grid, group, name, x_map, y_map, plot_feature_name="Y_VAL", type_name="true"
        )

    print("Done!")


def save_maps(
    grid,
    group,
    name,
    x_map,
    y_map,
    plot_feature_name: str,
    type_name: str,
    fill_color: str = "YlOrRd",
):
    # open a new map
    mtl_map = folium.Map(location=[y_map, x_map], zoom_start=12, tiles="Stamen Toner")
    folium.TileLayer("openstreetmap").add_to(mtl_map)
    data_plot = grid.merge(group, left_on="grid_id", right_on="grid_id")
    data_plot["DATE"] = data_plot["DATE"].dt.date
    data_plot["DATE"] = data_plot["DATE"].astype(str)
    choropleth = folium.Choropleth(
        geo_data=data_plot,
        name=name.strftime("%Y-%m"),
        data=data_plot.drop("geometry", axis=1),
        columns=["grid_id", plot_feature_name],
        key_on="feature.properties.grid_id",
        fill_color=fill_color,
        nan_fill_color="White",
        nan_fill_opacity=0.0,
        fill_opacity=0.8,
        line_opacity=0.2,
        highlight=True,
        bins=3,
        line_color="white",
    ).add_to(mtl_map)
    # Display Region Label
    choropleth.geojson.add_child(
        folium.features.GeoJsonTooltip(
            fields=["grid_id", "Y_PRED", "Y_VAL"],
            aliases=["Grid: ", "Predicted: ", "Real: "],
            style=(
                "background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;"
            ),
        )
    )
    folium.LayerControl(name=name.strftime("%Y-%m")).add_to(mtl_map)
    mtl_map.save(f"out/html/mtl-fire_{name.strftime('%Y-%m')}_{type_name}.html")
