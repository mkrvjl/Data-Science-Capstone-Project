import json
import urllib
import geopandas
import pandas as pd
import utils.keys as dt_keys
import utils.operations as ops
import utils.columns as dt_cols
from config.settings import ProjectSettings
from utils.conversions import time_to_category


settings = ProjectSettings()


def fire_incidents_data(
    add_time_categories: bool = False,
    remove_unrelevant: bool = False,
    merge_cols: bool = False,
    start_year: int = 0,
) -> pd.DataFrame:

    # read dataset and parse dates
    mtl_fire_inc = pd.read_csv(
        settings.fire_incidents.local.csv, parse_dates=[dt_cols.INC_DATE_COL]
    )

    if merge_cols:
        # merge columns 'arrond' and 'ville'
        ops.merge_columns(
            df=mtl_fire_inc,
            col_to_mask=dt_cols.INC_ARR_COL,
            other_col=dt_cols.INC_MUN_COL,
            str_to_match=dt_keys.INDETERMINE_KEY,
            new_col=dt_cols.BOROUGH_COL,
        )

    if remove_unrelevant:
        mtl_fire_inc.drop(
            columns=[
                'NOM_VILLE',
                'NOM_ARROND',
                'MTM8_X',
                'MTM8_Y',
            ],
            axis=1,
            inplace=True,
        )

    if add_time_categories:
        # add date categories
        mtl_fire_inc["YEAR"] = mtl_fire_inc["CREATION_DATE_TIME"].dt.year
        mtl_fire_inc["MONTH"] = mtl_fire_inc["CREATION_DATE_TIME"].dt.month
        mtl_fire_inc["DAY"] = mtl_fire_inc["CREATION_DATE_TIME"].dt.day
        mtl_fire_inc["QUART"] = mtl_fire_inc.CREATION_DATE_TIME.dt.time.map(time_to_category)

    # select only last year
    mtl_fire_inc = mtl_fire_inc[mtl_fire_inc["CREATION_DATE_TIME"].dt.year >= start_year]

    return mtl_fire_inc


def firefighter_stations_data(
    merge_cols: bool = True,
    remove_closed: bool = False,
) -> pd.DataFrame:

    # read dataset
    ff_stations_mtl = pd.read_csv(settings.fire_stations.local.csv)

    # replace n\a
    ff_stations_mtl = ff_stations_mtl.fillna(
        value={
            dt_cols.FF_ARR_COL: dt_keys.INDETERMINE_KEY,
            dt_cols.FF_MUN_COL: dt_keys.INDETERMINE_KEY,
            dt_cols.FF_DTEND_COL: "",
        }
    )

    # merge columns 'arrond' and 'ville'
    if merge_cols:
        ops.merge_columns(
            df=ff_stations_mtl,
            col_to_mask=dt_cols.FF_ARR_COL,
            other_col=dt_cols.FF_MUN_COL,
            str_to_match=dt_keys.INDETERMINE_KEY,
            new_col=dt_cols.BOROUGH_COL,
        )

    if remove_closed:
        ff_stations_mtl = ff_stations_mtl[ff_stations_mtl.DATE_FIN == ""]
        ff_stations_mtl = ff_stations_mtl.reset_index()

    return ff_stations_mtl


def lim_admin_mtl_data(
    data_src: str = "remote",
) -> dict:

    if data_src == "remote":
        # read url
        web_url = urllib.request.urlopen(settings.lim_admin_mtl.remote.geojson)
        data = web_url.read()

        # load response
        encoding = web_url.info().get_content_charset("utf-8")
        lim_admin_mtl = json.loads(data.decode(encoding))
    elif data_src == "local":
        lim_admin_mtl = geopandas.read_file(settings.lim_admin_mtl.local.shp)
    else:
        raise TypeError("Data source must be 'local' or 'remote'.")

    return lim_admin_mtl


def crime_data() -> geopandas.GeoDataFrame:

    crimes_gpd = geopandas.read_file(settings.crime_mtl.local.shp)

    return crimes_gpd
