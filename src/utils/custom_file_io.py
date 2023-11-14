import os


def get_absolute_path(parent_dir_path: str, sub_dir_path: str) -> str:
    file_abs_path = os.path.abspath(os.path.join(parent_dir_path, sub_dir_path))

    return file_abs_path


def override_local_paths(dictionary: dict, root_dir: str):
    shp_path, geojson_path, csv_path = None, None, None

    if "shp" in dictionary:
        shp_path = dictionary["shp"]
    if "geojson" in dictionary:
        geojson_path = dictionary["geojson"]
    if "csv" in dictionary:
        csv_path = dictionary["csv"]

    if shp_path and root_dir:
        shp_path = os.path.join(root_dir, os.path.basename(dictionary["shp"]))
    if geojson_path and root_dir:
        geojson_path = os.path.join(root_dir, os.path.basename(dictionary["geojson"]))
    if csv_path and root_dir:
        csv_path = os.path.join(root_dir, os.path.basename(dictionary["csv"]))

    return {"shp": shp_path, "geojson": geojson_path, "csv": csv_path}
