from enum import Enum


class DatasetEnums(str, Enum):
    # ----------------------------------------
    # database raw sources
    # ----------------------------------------
    LOCAL = "local"
    REMOTE = "remote"
    # ----------------------------------------
    # database source files extensions
    # ----------------------------------------
    SHP = "shp"
    GEOJSON = "geojson"
    CSV = "csv"
    # ----------------------------------------
    # custom database options
    # ----------------------------------------
    FIRST_LAYER = "first_layer"
    SECOND_LAYER = "second_layer"
