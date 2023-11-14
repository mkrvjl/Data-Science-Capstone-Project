import os
import geopandas as gpd
import matplotlib.pyplot as plt

from shapely import geometry
from config.data_source_info import DataSourceInfo
from config.logs import get_logger
from haversine import inverse_haversine, Unit
from data.prep.abstract_processor import DataProcessor
from data.types.geospatial_dataset import GeoSpatialDataset


logger = get_logger(__name__)


class DataGrid(DataProcessor):
    def __init__(
        self,
        grid_generic_filepath: str,
        first_layer_db_settings: DataSourceInfo,
        second_layer_db_settings: DataSourceInfo,
        grid_distance: int,
        grid_units: Unit,
        expand_data: bool = False,
    ):
        super().__init__(
            processed_root_dir="",
            processed_file_path="",
            grid_distance=grid_distance,
            grid_units=grid_units,
            grid_generic_filepath=grid_generic_filepath,
        )

        self.first_layer_db_settings = first_layer_db_settings
        self.second_layer_db_settings = second_layer_db_settings
        self.grid = GeoSpatialDataset()
        self.first_layer = GeoSpatialDataset()
        self.expand_data = expand_data

    @property
    def dataset_name(self):
        return str("grid")

    def data_load(self):
        """
        Load data for processing.
        """
        if os.path.exists(self.grid_local_path):
            logger.info("Loading existing grid file...")
            self.grid.load_from_path(self.grid_local_path)
        else:
            logger.debug(
                "Grid not found. Getting the layering data from previously validated file..."
            )
            self.first_layer.load_from_path(
                self.first_layer_db_settings.get_local_working_file_path()
            )

    def data_validate(self):
        """
        Validate all the data files.
        """
        self.validate_settings_batch(
            [self.first_layer_db_settings, self.second_layer_db_settings]
        )

    def data_transform(self):
        """
        Transform the data by creating a grid.
        """
        if self.grid.data is None:
            self.grid.data = DataGrid.create_grid(
                first_layer_data=self.first_layer,
                distance=self.grid_distance,
                units=self.grid_units,
                second_layer_local_path=self.second_layer_db_settings.get_local_working_file_path(),
                save_as_file=self.grid_local_path,
                expand_data=self.expand_data,
            )

    def data_aggregate(self):
        """
        Perform data aggregation if required.
        """
        # Not required for this class
        pass

    @staticmethod
    def create_grid(
        first_layer_data: GeoSpatialDataset,
        distance: float,
        units: Unit = None,
        second_layer_local_path: str = None,
        expand_data: bool = False,
        save_as_file: str = None,
    ) -> GeoSpatialDataset:
        """
        Generate a grid based on geospatial data.

        Args:
            first_layer_data (GeoSpatialDataset): Geospatial data of the universe.
            distance (float): Grid measure.
            units (Unit, optional): Grid measure units. Defaults to None.
            second_layer_local_path (str, optional): Path to the second layer data. Defaults to None.
            expand_data (bool, optional): Flag to expand data. Defaults to False.
            save_as_file (str, optional): Path to save the grid as a file. Defaults to None.

        Returns:
            GeoSpatialDataset: Grid as a geospatial dataset.
        """
        # Get the extent of the shapefile
        total_bounds = first_layer_data.data.total_bounds

        # Get minX, minY, maxX, maxY
        min_x, min_y, max_x, max_y = total_bounds

        # Create a fishnet
        x, y = (min_x, min_y)
        geom_array = []

        if units is None:
            grid_size = distance
        else:
            # Calculates distance with the haversine formula
            # from (min_x, min_y) to a distance in units
            # in the direction of 0 * pi radians
            end_pt = inverse_haversine(
                point=(min_x, min_y), distance=distance, direction=0, unit=units
            )
            grid_size = end_pt[0] - min_x

        # Start Idxs
        row_id = 0
        col_id = 0

        while y <= max_y:
            col_id = 0

            while x <= max_x:
                geom = geometry.Polygon(
                    [
                        (x, y),
                        (x, y + grid_size),
                        (x + grid_size, y + grid_size),
                        (x + grid_size, y),
                        (x, y),
                    ]
                )
                grid_name = "(" + str(row_id) + "," + str(col_id) + ")"
                geom_array.append([geom, row_id, col_id, grid_name])
                x += grid_size
                col_id += 1

            x = min_x
            y += grid_size
            row_id += 1

        # Create a new grid with the generated data
        grid = gpd.GeoDataFrame(
            geom_array, columns=["geometry", "grid_row_id", "grid_col_id", "grid_name"]
        )

        logger.debug(f"The generated grid size is ({row_id} x {col_id})")

        # Generate the index
        grid["grid_id"] = grid.index.tolist()

        # Create plot
        fig, ax = plt.subplots(figsize=(15, 15))
        gpd.GeoSeries(first_layer_data.data["geometry"]).boundary.plot(
            ax=ax, color="gray"
        )

        if not expand_data:
            grid = grid.drop(labels=["grid_row_id", "grid_col_id", "grid_name"], axis=1)

        # Standardize CRS projections
        grid.crs = first_layer_data.data.crs

        if second_layer_local_path:
            # Use the second layer
            logger.debug("Applying second layer...")
            second_layer = gpd.read_file(second_layer_local_path)
            second_layer = second_layer.to_crs(epsg=4269)
            grid.crs = second_layer.crs
            cols_names = grid.columns.values
            grid = grid.sjoin(second_layer, how="inner", rsuffix="census_right")
            grid = grid[cols_names]
            grid = grid.sort_values(by="grid_id", ascending=False)

            # Remove unused grids
            logger.debug("Applying first layer...")
            cols_names = grid.columns.values
            grid.crs = first_layer_data.data.crs
            grid = grid.sjoin(first_layer_data.data, how="inner", rsuffix="right")
            grid = grid[cols_names]

            # Add to plot
            second_layer.crs = first_layer_data.data.crs
            second_layer_trimmed = second_layer.sjoin(
                first_layer_data.data, how="inner", rsuffix="census_right"
            )
            gpd.GeoSeries(second_layer_trimmed["geometry"]).boundary.plot(
                ax=ax, color="red"
            )

        if save_as_file:
            gpd.GeoSeries(grid["geometry"]).boundary.plot(ax=ax)
            fig.savefig(save_as_file + ".pdf")
            grid.to_file(save_as_file)

        grid = grid.drop_duplicates()

        return GeoSpatialDataset(data=grid)
