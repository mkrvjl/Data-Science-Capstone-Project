import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import KBinsDiscretizer


def integrate_data():
    grid_mtl_path = "out/model_data/aggregated/all/MASTER_GRID_dates_quarterly_500m.csv"
    tax_rolls_path = (
        "out/model_data/aggregated/all/tax_rolls_aggregated_2023_grid_500m.csv"
    )
    fires_path = "out/model_data/aggregated/all/fires_aggregated_grid_500m.csv"
    no_fires_path = "out/model_data/aggregated/all/no_fires_aggregated_grid_500m.csv"
    crimes_path = "out/model_data/aggregated/all/crime_quarterly_agg.csv"
    prop_assmnt_path = "out/model_data/aggregated/all/PropertyAssessment_agg.csv"

    grid_mtl = pd.read_csv(grid_mtl_path, parse_dates=["DATE"])
    tax_rolls = pd.read_csv(tax_rolls_path)
    fires = pd.read_csv(fires_path)
    no_fires = pd.read_csv(no_fires_path)
    prop_asssmnt = pd.read_csv(prop_assmnt_path).drop(["Unnamed: 0"], axis=1)
    crimes = pd.read_csv(crimes_path).drop(["Unnamed: 0"], axis=1)

    # merge with grid
    master_df = grid_mtl.merge(
        fires,
        how="left",
        left_on=["grid_id", "YEAR", "QUARTER"],
        right_on=["grid_id", "YEAR", "QUARTER"],
    )

    master_df["DATE"] = master_df["DATE"].dt.date

    # merge with grid
    master_df = master_df.merge(
        no_fires,
        how="left",
        left_on=["grid_id", "YEAR", "QUARTER"],
        right_on=["grid_id", "YEAR", "QUARTER"],
    )

    master_df = master_df.merge(
        crimes,
        how="left",
        left_on=["grid_id", "YEAR", "QUARTER"],
        right_on=["grid_id", "YEAR", "QUARTER"],
    )

    # master_df = master_df.fillna({"INCIDENT_COUNT": 0})
    master_df = master_df.fillna(0)
    # master_df = master_df.astype({'INCIDENT_COUNT': int})

    master_df = master_df.merge(
        tax_rolls, how="left", left_on="grid_id", right_on="grid_id"
    )
    master_df = master_df.merge(
        prop_asssmnt, how="left", left_on="grid_id", right_on="grid_id"
    )

    master_df = master_df.dropna(
        axis=0,
        how="all",
        subset=[
            "EVAL_SUM",
            "EVAL_MEAN",
            "NB_TAX_PARCELS",
            "N_FLOOR_AVG",
            "N_LOGEMENT_SUM",
            "COMMERCIAL",
            "RESIDENTIAL",
            "AGE_AVG",
            "N_BUILDINGS",
            "LAND_AREA_AVG",
            "BUILD_TOT_AREA_AVG",
        ],
    )

    master_df = master_df.fillna(master_df.median())
    master_df = master_df.reset_index(drop=True)

    data = master_df["INCIDENT_COUNT"].values
    trans = KBinsDiscretizer(n_bins=3, encode="ordinal", strategy="kmeans")
    data = trans.fit_transform(data.reshape(-1, 1))
    dataset = pd.DataFrame(data, columns=["RISK"])
    dataset = dataset.astype({"RISK": "int8"})

    map_dict = {0: "low", 1: "mid", 2: "high"}
    data_cat = dataset["RISK"].map(map_dict)

    master_df["RISK"] = data_cat

    train_dt = master_df[master_df["DATE"] < "2022-10-01"]
    predict_dt = master_df[master_df["DATE"] >= "2022-10-01"]

    y = train_dt.RISK.values.reshape(-1, 1)
    # X = train_dt.drop(['YEAR', 'QUARTER', 'INCIDENT_COUNT', 'RISK'], axis=1)
    X = train_dt.drop(["YEAR", "QUARTER", "DATE", "INCIDENT_COUNT", "RISK"], axis=1)

    # dividing X, y into train and test data
    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=0)

    # fit final model
    model = LogisticRegression()
    model.fit(X_train, y_train)

    print("Hello world!")
