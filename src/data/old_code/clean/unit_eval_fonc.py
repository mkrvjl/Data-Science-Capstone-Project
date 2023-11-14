import geopandas as gpd
from haversine import Unit
from data.old_code.clean.spatial import get_grid


def clean_eval_data(grid_distance: int = 500, grid_units: Unit = Unit.METERS) -> None:
    # get mtl with grid
    grid_mtl = get_grid(
        distance=grid_distance,
        units=grid_units,
        remove_unused_grids=True,
        add_col_row_ids=False,
        add_grid_name=False,
    )

    # open full dataset
    unit_eval_fonc = gpd.read_file(
        "resources/data/unit_eval_fonc/uniteevaluationfonciere.geojson"
    )

    # make sure they're using the same projection reference and merge
    unit_eval_fonc.crs = grid_mtl.crs
    unit_eval_fonc_grid = unit_eval_fonc.sjoin(grid_mtl, how="left")

    # drop data not required
    clean_unit_eval_fonc_grid = unit_eval_fonc_grid.drop(
        labels=["geometry", "index_right"], axis=1
    )

    # clean and save data
    clean_unit_eval_fonc_grid = clean_unit_eval_fonc_grid.drop_duplicates()
    clean_unit_eval_fonc_grid.to_csv(
        f"out/model_data/clean/unit_eval_fonc/unit_eval_fonc_mtl_grid_{grid_distance}{grid_units.value}.csv",
        index=False,
    )

    # drop all except grid_id and ID_CUM (for tax-rolls)
    unit_eval_fonc_id = clean_unit_eval_fonc_grid.drop(
        labels=[
            "SUITE_DEBUT",
            "MUNICIPALITE",
            "ETAGE_HORS_SOL",
            "NOMBRE_LOGEMENT",
            "ANNEE_CONSTRUCTION",
            "CODE_UTILISATION",
            "LETTRE_DEBUT",
            "LETTRE_FIN",
            "LIBELLE_UTILISATION",
            "CATEGORIE_UEF",
            "MATRICULE83",
            "SUPERFICIE_TERRAIN",
            "SUPERFICIE_BATIMENT",
            "NO_ARROND_ILE_CUM",
        ],
        axis=1,
    )

    # save cleaned file
    unit_eval_fonc_id.to_csv(
        "out/model_data/clean/unit_eval_fonc/id_only_unit_eval_fonc_mtl_grid_500m.csv",
        index=False,
    )
