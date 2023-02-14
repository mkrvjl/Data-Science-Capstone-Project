from data.clean import clean_fire_incidents
from data.visualize import create_choropleth_stations, create_fire_incidents_yearly

if __name__ == "__main__":
    #create_fire_stations_graph()
    #create_choropleth_stations()
    #create_fire_incidents_yearly()
    clean_fire_incidents(
        remove_unrelevant=True,
        add_time_categories=True
    )

    print("Done!!!")
