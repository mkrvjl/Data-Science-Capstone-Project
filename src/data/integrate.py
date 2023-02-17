import pandas as pd
from haversine import Unit
from config.settings import ProjectSettings
from data.spatial import get_grid

settings = ProjectSettings()

def integrate_datasets():

    grid = get_grid(distance=500, units=Unit.METERS)

    # get datasets file paths
    crime_agg_path = "src/resources/data/Aggregated/CrimeAggregated_WithGrid.csv"
    fire_agg_path = "src/resources/data/Aggregated/fire-incidents-grouped-by-date-onehot.csv"
    property_agg_path = "src/resources/data/Aggregated/PropertyAggregated_WithGrid.csv"

    # read datasets
    crime_pd = pd.read_csv(crime_agg_path, encoding="ISO-8859-1")
    fire_pd = pd.read_csv(fire_agg_path)
    property_pd = pd.read_csv(property_agg_path)

    # drop unused columns
    crime_pd = crime_pd[crime_pd['YEAR'] > 2017]
    crime_pd = crime_pd.drop(labels=['YEAR', 'MONTH', 'DAY'], axis=1)

    # integrate property into fire dataset
    fire_and_property = fire_pd.merge(
        property_pd,
        how='left',
        left_on="index_grid",
        right_on="grid_id",
    )

    # drop unused columns
    fire_and_property = fire_and_property.drop(['grid_id', 'Grid_Name', 'Unnamed: 0'], axis=1)

    # do a one hot encoding on crime dataset
    one_hot = pd.get_dummies(crime_pd['CATEGORIE'])
    one_hot = one_hot.multiply(crime_pd['Count'], axis="index")

    crime_pd = crime_pd.join(one_hot)
    crime_pd = crime_pd.sort_values(by='Count', ascending=False).reset_index(drop=True)
    crime_pd = crime_pd.drop(['CATEGORIE', 'Count'], axis=1)

    # integrate fire, crime and property datasets
    fire_crime_property = fire_and_property.merge(
        crime_pd,
        how="outer",
        left_on=["index_grid", "DATE", "Grid Name", "QUART"],
        right_on=["index_grid", "DATE", "Grid_Name", "QUART"],
    )

    # cleanse unwanted columns and export
    all_data = fire_crime_property.fillna(0)
    all_data = all_data.astype(fire_and_property.dtypes)
    all_data = all_data.astype(crime_pd.dtypes)
    all_data = all_data.drop(['Grid Name', 'Grid_Name'], axis=1)
    all_data = all_data.sort_values(by="DATE")
    all_data.to_csv(f"{settings.out_dir}/data/all_data_integrated_python.csv")
