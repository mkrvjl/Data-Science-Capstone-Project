import config.logs as logs

from typing import List
from config.settings import ProjectSettings
from config.state_machine import process_datasets
from data.prep.abstract_processor import DataProcessor
from data.prep import all_datasets

settings = ProjectSettings()
logger = logs.get_logger(__name__)


def execute():
    logger.info("Starting execution...")

    # get processing prep
    dataset_to_process: List[DataProcessor] = all_datasets()

    # execute dataset pipeline
    process_datasets(datasets=dataset_to_process)

    print("Processing completed...")

    # # create_fire_stations_graph()
    # # create_choropleth_stations()
    # # create_fire_incidents_yearly()
    # # year = clean_fire_incidents(
    # #     remove_unrelevant=True,
    # #     add_time_categories=True
    # # )
    # #
    # # get_grid_with_dates(
    # #         distance=500,
    # #         units=Unit.METERS,
    # #         start_date=datetime.date(2018, 1, 1),
    # #         end_date=datetime.date.today(),
    # #         freq="MS",
    # # )
    # #
    # # clean_crime_data()
    # #
    # # data_exploration()
    # #
    # # aggregate_fire_data()
    # #
    # # examen()
    # #
    # # view_heat_map()

    # # Old Good
    # balance_data()
    # integrate_data()
    # # create_model()
    #
    # # integrate_data()
    #
    # # clean_eval_data()
    #
    # # clean_crime_data()
    #
    # get_grid_with_dates(
    #     distance=1000,
    #     units=Unit.METERS,
    #     start_date=datetime.date(2015, 1, 1),
    #     end_date=datetime.date.today(),
    #     freq='3MS',
    # )
    #
    # # clean_fire_incidents_v3(
    # #     remove_unrelevant=True,
    # #     add_time_categories=True,
    # #     aggregate_data=True,
    # #     grid_distance=500,
    # #     grid_units=Unit.METERS,
    # # )
    #
    # #
    # # fire_clean = f"{settings.out_dir}/data/fire-aggregated.csv"
    # #
    # # clean_fire_incidents_v2(
    # #     file_path=fire_clean
    # # )
    # #
    # # visualize_grid(500, Unit.METERS)
    #
    # # integrate_datasets_draft_02(
    # #
    # # )
