from data.old_code.fetch import crime_data
import geopandas as gpd


def clean_crime_data(grid_data: gpd) -> None:
    # get dataset
    crimes_mtl = crime_data()

    print(f"Initial crime dataset lenght is : '{len(crimes_mtl)}'")

    # make sure they're using the same projection reference and merge
    crimes_mtl.crs = grid_data.crs
    crimes_mtl_grid = crimes_mtl.sjoin(grid_data, how="left")

    # drop data not required
    crimes_mtl_grid = crimes_mtl_grid.drop_duplicates(subset=["index"], keep="first")
    crimes_mtl_grid = crimes_mtl_grid.drop(labels=["geometry", "index_right"], axis=1)

    # clean and save data
    len_crimes_data = len(crimes_mtl_grid)
    crimes_mtl_grid = crimes_mtl_grid.dropna(axis=0, subset=["grid_id"])
    print(f"Dropped ({len_crimes_data - len(crimes_mtl)}) INCOMPLETE data records!!")

    crimes_mtl_grid.to_csv(
        f"out/model_data/clean/crimes/crime_mtl_grid_{grid_distance}{grid_units.value}.csv",
        index=False,
    )

    print("Hello world!")
