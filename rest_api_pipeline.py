import dlt
from rest_api import (
    RESTAPIConfig,
    rest_api_resources,
)
from logging import getLogger

logger = getLogger(__name__)


@dlt.source
def tfl_source(tfl_id: str = dlt.secrets.value, tfl_key: str = dlt.secrets.value):
    # Create a REST API configuration for the TFL API
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.tfl.gov.uk/",
            "headers": {
                "Content-Type": "application/json",
                "app_id": tfl_id,
                "app_key": tfl_key,
            },
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
            "endpoint": {
                "params": {
                    "per_page": 100,
                },
            },
        },
        "resources": [
            {
                "name": "tube_lines",
                "endpoint": {
                    "path": "Line/Mode/tube",
                },
            },
            {
                "name": "train_arrivals",
                "endpoint": {
                    "path": "Line/{line_id}/Arrivals",
                    "params": {
                        "line_id": {
                            "type": "resolve",
                            "resource": "tube_lines",
                            "field": "id",
                        },
                    },
                },
                "include_from_parent": ["id"],
            },
        ],
    }

    yield from rest_api_resources(config)


def load_tfl_data() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_tfl",
        destination="filesystem",
        dataset_name="tfl_data",
    )

    load_info = pipeline.run(tfl_source())
    logger.info(load_info)


if __name__ == "__main__":
    load_tfl_data()
