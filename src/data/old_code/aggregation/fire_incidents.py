from data.old_code.clean import clean_fire_data


def aggregate_fire_data() -> None:
    fires_mtl = clean_fire_data()

    fires_only_mtl = fires_mtl[fires_mtl["TYPE"] != "C"]
    no_fires_mtl = fires_mtl[fires_mtl["TYPE"] == "C"]

    # aggregate tax rolls by grid id
    fires_only_agg = (
        fires_only_mtl.groupby(["grid_id", "YEAR", "QUARTER"])["INCIDENT_NBR"]
        .count()
        .reset_index()
        .rename(columns={"INCIDENT_NBR": "INCIDENT_COUNT"})
    )

    no_fires_mtl_agg = (
        no_fires_mtl.groupby(["grid_id", "YEAR", "QUARTER"])["INCIDENT_NBR"]
        .count()
        .reset_index()
        .rename(columns={"INCIDENT_NBR": "OTHER_FIRES_COUNT"})
    )

    # save aggregated data
    fires_only_agg.to_csv(
        f"out/model_data/aggregated/fires/fires_aggregated_grid_500m.csv", index=False
    )
    no_fires_mtl_agg.to_csv(
        f"out/model_data/aggregated/fires/other_fires_aggregated_grid_500m.csv",
        index=False,
    )

    print("Hello world!!")
