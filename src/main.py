import json
import urllib
import geopandas as gpd
from haversine import Unit
from config.settings import ProjectSettings
from data.clean import clean_fire_incidents
from data.integrate import integrate_datasets
from data.spatial import create_grid, visualize_grid
from data.visualize import create_choropleth_stations, create_fire_incidents_yearly
import pandas as pd

settings = ProjectSettings()


if __name__ == "__main__":
    #create_fire_stations_graph()
    #create_choropleth_stations()
    # create_fire_incidents_yearly()
    # year = clean_fire_incidents(
    #     remove_unrelevant=True,
    #     add_time_categories=True
    # )

    visualize_grid(
        distance=500,
        units=Unit.METERS
    )

    print("Executed!!!")

