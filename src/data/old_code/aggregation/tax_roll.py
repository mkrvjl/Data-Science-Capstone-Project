from pathlib import Path
import pandas as pd

from data.old_code.clean import clean_tax_data

data_raw_dir = Path("resources/data/tax_rolls/raw")


def aggregate_tax_data2() -> None:
    # start by cleaning data
    clean_tax_data()

    # read cleaned unit_eval_fonc with grid dataset
    unit_eval_fonc_id = pd.read_csv(
        "out/model_data/clean/unit_eval_fonc/id_only_unit_eval_fonc_mtl_grid_500m.csv"
    )

    # get all tax roll filles
    files_in_dir = (entry for entry in data_raw_dir.iterdir() if entry.is_file())

    # create empty dataframe (will be concatenated)
    tax_roll_cat = pd.DataFrame()

    # iterate over files and aggreagete for each file
    for item in files_in_dir:
        print(f"Processing file '{item.name}'")

        curr_tax_roll = clean_tax_data2(item.name, item.absolute(), unit_eval_fonc_id)
        tax_roll_cat = pd.concat([tax_roll_cat, curr_tax_roll])

    # save concatenated files containing all tax roles
    tax_roll_cat.to_csv(
        f"out/model_data/clean/tax_rolls/clean/tax_rolls_2023_clean_grid_500m.csv",
        index=False,
    )

    # aggregate tax rolls by grid id
    tax_roll_agg = (
        tax_roll_cat.groupby("grid_id")
        .VAL_IMPOSABLE.agg(["sum", "mean", "count"])
        .reset_index(drop=False)
        .rename(
            columns={"sum": "EVAL_SUM", "mean": "EVAL_MEAN", "count": "NB_TAX_PARCELS"}
        )
    )

    # save aggregated data
    tax_roll_agg.to_csv(
        f"out/model_data/aggregated/tax_rolls/tax_rolls_aggregated_grid_500m.csv",
        index=False,
    )


def clean_tax_data2(
    tax_file_name: str,
    tax_file_path: Path,
    unit_eval_grid: pd.DataFrame,
    get_codes_count: bool = False,
):
    # read file and assign proper types to data
    tax_roll = pd.read_csv(
        tax_file_path,
        dtype={
            "ARRONDISSEMENT": int,
            "NOM_ARRONDISSEMENT": str,
            "ANNEE_EXERCICE": int,
            "ID_CUM": int,
            "NO_COMPTE": str,
            "AD_EMPLAC_CIV1": str,
            "AD_EMPLAC_CIV2": str,
            "AD_EMPLAC_GENER": str,
            "AD_EMPLAC_RUE": str,
            "AD_EMPLAC_ORIENT": str,
            "AD_EMPLAC_SUITE1": str,
            "AD_EMPLAC_SUITE2": str,
            "CODE_DESCR_LONGUE": str,
            "DESCR_LONGUE": str,
            "VAL_IMPOSABLE": float,
            "TAUX_IMPOSI": float,
            "MONTANT_DETAIL": float,
        },
    )

    # for debugging, count code types
    if get_codes_count:
        tax_codes_grouped = (
            tax_roll.groupby(["CODE_DESCR_LONGUE", "DESCR_LONGUE"])["ID_CUM"]
            .count()
            .reset_index()
        )
        print(tax_codes_grouped)

    # only get required codes
    tax_roll = tax_roll.loc[tax_roll["CODE_DESCR_LONGUE"] == "E00"]
    tax_roll = tax_roll.loc[tax_roll["ANNEE_EXERCICE"] == 2023]

    # drop data not required
    tax_roll = tax_roll.drop(
        labels=[
            "ARRONDISSEMENT",
            "NO_COMPTE",
            "NOM_ARRONDISSEMENT",
            "TAUX_IMPOSI",
            "MONTANT_DETAIL",
            "ANNEE_EXERCICE",
        ],
        axis=1,
    )

    # merge with grid
    tax_roll_grid_2023 = tax_roll.merge(
        unit_eval_grid, left_on="ID_CUM", right_on="ID_UEV"
    )

    # save file
    tax_roll_grid_2023.to_csv(
        f"out/model_data/clean/tax_rolls/clean_2023_{tax_file_name}", index=False
    )

    # aggregate individually
    tax_roll_agg = (
        tax_roll_grid_2023.groupby("grid_id")
        .VAL_IMPOSABLE.agg(["sum", "mean", "count"])
        .reset_index(drop=False)
        .rename(
            columns={"sum": "EVAL_SUM", "mean": "EVAL_MEAN", "count": "NB_TAX_PARCELS"}
        )
    )

    # save file
    tax_roll_agg.to_csv(
        f"out/model_data/aggregated/tax_rolls/agg_{tax_file_name}", index=False
    )

    return tax_roll_grid_2023
