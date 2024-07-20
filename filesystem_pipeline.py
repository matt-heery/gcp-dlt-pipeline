import os

import dlt

try:
    from .filesystem import FileItemDict, filesystem, readers
except ImportError:
    from filesystem import (
        FileItemDict,
        filesystem,
        readers,
    )

BUCKET_URL = "gs://met-office-data-202307201043000"


def create_reader(table_name):
    return (
        readers(BUCKET_URL, file_glob=f"tfl_data/{table_name}/*.parquet")
        .read_parquet()
        .with_name(table_name)
    )


def read_parquet_and_jsonl_chunked(table_names) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="standard_filesystem",
        destination="bigquery",
        dataset_name="tfl_data",
    )
    # When using the readers resource, you can specify a filter to select only the files you
    # want to load including a glob pattern. If you use a recursive glob pattern, the filenames
    # will include the path to the file inside the bucket_url.

    # PARQUET reading
    print(table_names)
    readers = [create_reader(table_name) for table_name in table_names]
    # load both folders together to specified tables
    load_info = pipeline.run(readers)
    print(load_info)
    print(pipeline.last_trace.last_normalize_info)


def copy_files_resource(local_folder: str) -> None:
    """Demonstrates how to copy files locally by adding a step to filesystem resource and the to load the download listing to db"""
    pipeline = dlt.pipeline(
        pipeline_name="standard_filesystem_copy",
        destination="bigquery",
        dataset_name="standard_filesystem_data",
    )

    # a step that copies files into test storage
    def _copy(item: FileItemDict) -> FileItemDict:
        # instantiate fsspec and copy file
        dest_file = os.path.join(local_folder, item["relative_path"])
        # create dest folder
        os.makedirs(os.path.dirname(dest_file), exist_ok=True)
        # download file
        item.fsspec.download(item["file_url"], dest_file)
        # return file item unchanged
        return item

    # use recursive glob pattern and add file copy step
    downloader = filesystem(BUCKET_URL, file_glob="**").add_map(_copy)

    # NOTE: you do not need to load any data to execute extract, below we obtain
    # a list of files in a bucket and also copy them locally
    # listing = list(downloader)
    # print(listing)

    # download to table "listing"
    # downloader = filesystem(TESTS_BUCKET_URL, file_glob="**").add_map(_copy)
    load_info = pipeline.run(
        downloader.with_name("listing"), write_disposition="replace"
    )
    # pretty print the information on data that was loaded
    print(load_info)
    print(pipeline.last_trace.last_normalize_info)


def main_load(tables):
    copy_files_resource("_storage")
    read_parquet_and_jsonl_chunked(tables)
