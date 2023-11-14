from config.settings import ProjectSettings
from data.prep.crime import DatasetCrime
from data.prep.fire_incidents import DatasetFireIncidents
from data.prep.grid import DataGrid
from data.prep.property_assessment import DatasetPropertyAssessment
from data.prep.tax_rolls import DatasetTaxRoll
from utils.enums.databases import ExternalDatabases
from utils.enums.states import StateMachineStates

settings = ProjectSettings()


grid_dataset = DataGrid(
    grid_generic_filepath=settings.grid_shp_filepath,
    first_layer_db_settings=settings.databases[ExternalDatabases.LIM_ADMIN_MTL],
    second_layer_db_settings=settings.databases[ExternalDatabases.CENSUS_2021],
    grid_distance=settings.grid_distance,
    grid_units=settings.grid_units,
)

tax_roll_dataset = DatasetTaxRoll(
    dataset_settings=settings.databases[ExternalDatabases.TAX_ROLLS],
    grid_generic_filepath=settings.grid_shp_filepath,
    processed_root_dir=settings.processed_root_dir,
    processed_sub_dir=StateMachineStates.STATE_TRANSFORMATION.value,
    processed_file_path=settings.processed_file_path,
    processed_file_name=settings.databases[ExternalDatabases.PROPERTY_ASSESSMENT].name,
    grid_distance=settings.grid_distance,
    grid_units=settings.grid_units,
)

property_assessment_dataset = DatasetPropertyAssessment(
    dataset_settings=settings.databases[ExternalDatabases.PROPERTY_ASSESSMENT],
    grid_generic_filepath=settings.grid_shp_filepath,
    processed_root_dir=settings.processed_root_dir,
    processed_file_path=settings.processed_file_path,
    grid_distance=settings.grid_distance,
    grid_units=settings.grid_units,
)

crime_dataset = DatasetCrime(
    dataset_settings=settings.databases[ExternalDatabases.ACTES_CRIMINELS],
    grid_generic_filepath=settings.grid_shp_filepath,
    processed_root_dir=settings.processed_root_dir,
    processed_file_path=settings.processed_file_path,
    grid_distance=settings.grid_distance,
    grid_units=settings.grid_units,
)

fire_incidents_dataset = DatasetFireIncidents(
    dataset_settings=settings.databases[ExternalDatabases.INTERVENTIONS_SIM],
    grid_generic_filepath=settings.grid_shp_filepath,
    processed_root_dir=settings.processed_root_dir,
    processed_file_path=settings.processed_file_path,
    grid_distance=settings.grid_distance,
    grid_units=settings.grid_units,
)


def all_datasets() -> list:
    """
    The list of prep to process

    Returns:
        A list containing the prep
    """
    datasets_to_process = [
        grid_dataset,
        tax_roll_dataset,
        # property_assessment_dataset,
        # crime_dataset,
        # fire_incidents_dataset,
        # new prep goes here
    ]

    return datasets_to_process
