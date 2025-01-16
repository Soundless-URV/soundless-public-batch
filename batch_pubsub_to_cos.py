#!/opt/venv/bin python
import os
import sys
import time

import re
import json
import yaml
from io import StringIO
from datetime import datetime

import logging
from google.cloud import storage, pubsub_v1
from google.cloud import monitoring_v3
from google.cloud.monitoring_v3 import query

import pandas as pd
import geopandas as gpd
from geopandas import GeoDataFrame


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# How many Pub/Sub entries we load into memory at once
CHUNK_SIZE = 1000


project_id = os.environ["PROJECT_ID"]

# Pub/Sub
topic_id = os.environ["TOPIC_ID"]
topic_name = f"projects/{project_id}/topics/{topic_id}"
subscription_id = os.environ["SUBSCRIPTION_ID"]
subscription_name = f"projects/{project_id}/subscriptions/{subscription_id}"

# Cloud Storage
bucket_name = os.environ["BUCKET_NAME"]

# Folder in disk to store provisional files before uploading
output_folder = os.environ["OUTPUT_FOLDER"]

# Other variables
town_borders_path = os.environ["TOWN_BORDERS_PATH"]
regions_yaml_path = os.environ["REGIONS_YAML_PATH"]


def load_town_borders() -> GeoDataFrame:
    town_borders = gpd.read_file(town_borders_path)
    town_borders = town_borders.to_crs(4326)  # WGS84 Latitude/Longitude (EPSG:4326)
    return town_borders


def load_regions() -> GeoDataFrame:
    """
    Compute a GeoDataframe for every allowed region, and return a concatenation of them.
    Columns: (region, geometry).
    
    Example:

    region              geometry
    Reus                MULTIPOLYGON(((...
    Baix Camp_county    MULTIPOLYGON(((...
    Tarragonès_county   MULTIPOLYGON(((...
    Tarragona_province  MULTIPOLYGON(((...
    """
    regions = read_yaml_regions()
    town_borders = load_town_borders()
    
    gdf_list = []

    town_names, town_codes = extract_region_tuples_from_yaml(regions, region_type="towns")
    if town_names is not None:
        towns = town_borders[town_borders["CODIMUNI"].isin(town_codes)]
        for i, code in enumerate(town_codes):
            row = towns.index[towns["CODIMUNI"] == code].tolist()[0]
            col = "region"
            towns.at[row, col] = town_names[i]
        gdf_list.append(towns)
        logger.info(towns)

    # Obtain big multipolygons that contain every province in our list (by joining towns)
    province_names, province_codes = extract_region_tuples_from_yaml(regions, region_type="provinces")
    if province_names is not None:
        provinces = town_borders[town_borders["CODIPROV"].isin(province_codes)].dissolve(by="CODIPROV", as_index=False)
        for i, code in enumerate(province_codes):
            row = provinces.index[provinces["CODIPROV"] == str(code)].tolist()[0]
            col = "region"
            provinces.at[row, col] = province_names[i] + "_province"
        gdf_list.append(provinces)
        logger.info(provinces)

    # Obtain big multipolygons that contain every county in our list (by joining towns)
    county_names, county_codes = extract_region_tuples_from_yaml(regions, region_type="counties")
    if county_names is not None:
        counties = town_borders[town_borders["CODICOMAR"].isin(county_codes)].dissolve(by="CODICOMAR", as_index=False)
        for i, code in enumerate(county_codes):
            row = counties.index[counties["CODICOMAR"] == str(code)].tolist()[0]
            col = "region"
            counties.at[row, col] = county_names[i] + "_county"
        gdf_list.append(counties)
        logger.info(counties)

    regions = gpd.GeoDataFrame(
        pd.concat(gdf_list, ignore_index=True), 
        crs=town_borders.crs
    )
    regions = drop_useless_columns(regions)

    return regions


def read_yaml_regions():
    with open(regions_yaml_path, 'r') as stream:
        regions = yaml.safe_load(stream)
    return regions


def extract_region_tuples_from_yaml(yaml: str, region_type: str) -> tuple:
    if not yaml[region_type]:
        return (None, None)

    keys = [str(list(region.keys())[0]) for region in yaml[region_type]]
    values = [str(list(region.values())[0]) for region in yaml[region_type]]
    return keys, values


def drop_useless_columns(region_gdf: GeoDataFrame) -> GeoDataFrame:
    return region_gdf.loc[:, ["region", "geometry"]]


def load_csv_into_geopandas(csv: StringIO, header=True) -> GeoDataFrame:
    """
    Loads values into a DataFrame and transforms it into a GeoDataFrame.

    @csv: Comma-separated (",") and with UNIX line breaks ("\\n")
    @header: If true, then the column names are included in the csv
    """
    csv.seek(0)  # Go back to the beginning of the csv 'file' to get all data
    df = pd.read_csv(csv)
    gdf = GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.lng, df.lat),
        crs="EPSG:4326"  # WGS84 Latitude/Longitude (EPSG:4326)
    )
    return gdf


def geopandas_to_files(gdf: GeoDataFrame, regions: GeoDataFrame, output_folder: str) -> None:
    """
    Appends the GeoDataFrame contents to their corresponding csv files.

    Notice each file should contain values for a certain REGION and DATE, and 
    should be named accordingly. Examples:
        - Tarragona_2022-05-13.csv
        - Tarragona_2022-05-14.csv
        - Tarragona_province_2022-05-15.csv
        - Reus_2022-05-12.csv
        - Reus_2022-05-14.csv

    In order to have that, we need to group values, and so: 
        - We find in what region every row is (via town borders)
        - We transform timestamps to formatted dates
        - We group rows by region and date (pandas groupby)

    And finally, for every group, we write values to the corresponding file.

    @gdf: Geometry should contain points in EPSG:4326 format.
    """
    # Find city for the row and add it as a column in gdf
    gdf = gdf.sjoin(regions, how="left", predicate="intersects")
    gdf.drop(columns=["index_right"], inplace=True)

    # Add column for formatted date
    gdf["date"] = gdf["timestamp"].apply(format_date)

    logger.info("gdf columns: ")
    logger.info(gdf.columns)

    # Group by both variables and write each group as a separate file
    for group, data in gdf.groupby(["region", "date"], dropna=False):
        # 'group' is a tuple
        region = group[0]
        date = group[1]  # e.g. 2022-05-13
        output_file_path = str(output_folder) + "/" + str(region) + "_" + str(date) + ".csv"
        
        logger.info("Will write in file: " + str(output_file_path))

        # We do not need to write the grouping columns, they will appear in the file name
        data.drop(columns=["region", "date"], inplace=True)

        # Write header, only if file does NOT exist in disk
        should_write_header = not os.path.exists(output_file_path)
        data.to_csv(output_file_path, index=False, mode='a', header=should_write_header)


def format_date(timestamp: int) -> str:
    """
    Returns a YYYY-MM-DD string for the input timestamp.

    @timestamp In milliseconds.
    """
    date = datetime.fromtimestamp(int(timestamp) / 1000)
    date_str = f"{date.year}-{date.month}-{date.day}"
    return date_str

        
def pull_into_csv(
    subscription: str, 
    client: pubsub_v1.SubscriberClient, 
    num_messages: int) -> StringIO:
    """
    Read @num_messages messages from the @pubsub_subscription and load them into
    a comma-separated StringIO object.

    Returns None if data could not be pulled or was corrupted, or a StringIO 
    object otherwise.

    Throws ValueError if num_messages is not a positive integer.
    """
    if num_messages <= 0:
        raise ValueError("You must pull at least 1 message.")

    separator = "\n"
    csv = StringIO()
    ids = []
    response = client.pull(
        subscription=subscription,
        return_immediately=False, 
        max_messages=num_messages,
        timeout=180.0
    )

    header_written = False
    if response.received_messages is None:
        logger.info("Received messages is None")
    else:
        logger.info("Received messages!")

    for received_message in response.received_messages:
        data_dict = json.loads(received_message.message.data.decode('UTF-8'))

        if not header_written:
            header = ",".join([str(key) for key in data_dict.keys()])
            csv.write(header + str(separator))
            header_written = True

        row = ",".join([str(val) for val in data_dict.values()])
        csv.write(row + str(separator))

        ids.append(received_message.ack_id)
    if len(ids) > 0:
        logger.info("Trying to pull " + str(num_messages) + " messages. Pulled: " + str(len(ids)))
        client.acknowledge(subscription=subscription, ack_ids=ids)
    
    if not header_written:
        # We presume data has not been properly read
        logger.info("NOT header_written")
        return None

    return csv


def files_to_cloud_storage(source_folder_path: str, bucket: str):
    """
    Stream upload csv files from provisional storage to the Cloud Storage bucket.

    This is a long-running operation.

    @folder Local folder
    @bucket GCS bucket name
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)
    blobs_in_bucket = [blob.name for blob in storage_client.list_blobs(bucket)]

    logger.info("List of available files to upload:")
    logger.info(os.listdir(source_folder_path))

    for source_file in os.listdir(source_folder_path):
        # If source = /home/user/data/soundless.csv, then this variable is soundless.csv.
        # As we can get data from users in days other than the current day, we have to
        # check that files in bucket are actually not created, otherwise we might be
        # overriding data files. We'll create secondary files and merge later
        destination_blob_name = os.path.basename(source_file)
        while destination_blob_name in blobs_in_bucket:
            destination_blob_name = alternative_name(destination_blob_name)

        blob = bucket.blob(destination_blob_name)
        with open(os.path.join(source_folder_path, source_file), 'rb') as f:
            f.seek(0)
            blob.upload_from_file(f)

        logger.info("Uploaded file to bucket: " + str(destination_blob_name))

        # Clean up files
        os.remove(os.path.join(source_folder_path, source_file))
            

def alternative_name(path: str):
    """
    Given a file name, this returns a similar name by appending an underscore and
    a correlative number to it. It will append the number before the first dot in
    the string to be able to work with extensions such as .tar.gz.
    
        something.csv -> something_1.csv
        /some.folder/something_3.csv -> /some.folder/something_4.csv
        something.tar.gz -> something_1.tar.gz
        .env -> _1.env
    """
    logging.info("Looking for an alternative name for file: " + str(path))
    folder, file_name = os.path.split(path)
    file_name_no_ext = file_name.split(".")[0]

    pattern = ".*(_\d+)\..*$"
    result = re.search(pattern, file_name)

    if result is not None:
        suffix = result.group(1)
        number = int(suffix.removeprefix("_"))
        number += 1
        new_suffix = "_" + str(number)
    else:
        suffix = ""
        new_suffix = "_1"

    new_file_name = file_name.replace(
        file_name_no_ext, 
        file_name_no_ext.removesuffix(suffix) + str(new_suffix), 
        1  # Number of times we replace
    )

    if folder != "":
        new_file_name = os.path.join(folder, new_file_name)

    return new_file_name


if __name__ == "__main__":
    
    # Read number of undelivered messages from GCP Monitoring
    client = monitoring_v3.MetricServiceClient()
    result = query.Query(
            client,
            project_id,
            'pubsub.googleapis.com/subscription/num_undelivered_messages',
            days=7
        ).as_dataframe()
    num_messages = result['pubsub_subscription'][project_id][subscription_id][-1]  # Latest entry

    if num_messages <= 0:
        sys.exit()

    # Sometimes Monitoring metrics can be faster than Pub/Sub messages
    # So, if we wait some seconds, we will get (at least) the number of messages
    # that were recognized by the Monitoring client
    time.sleep(10)

    # Partition into chunks, to batch load the undelivered messages
    # Example: if num_messages = 34,655, then chunks = [10000, 10000, 10000, 4655]
    num_full_chunks = num_messages // CHUNK_SIZE
    chunks = [CHUNK_SIZE for _ in range(0, num_full_chunks)]

    remaining = num_messages % CHUNK_SIZE
    if remaining: chunks.append(remaining)

    # Make sure output folder exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Batch load chunks from subscription
    with pubsub_v1.SubscriberClient() as client:
        subscription = client.subscription_path(
            project=project_id,
            subscription=subscription_id
        )
        
        num_processed_messages = 0
        regions = load_regions()

        for chunk in chunks:
            try:
                logging.info("pull_into_csv")
                csv = pull_into_csv(subscription, client, chunk)
                if csv is not None:
                    logging.info("load_csv_into_geopandas")
                    gdf = load_csv_into_geopandas(csv)
                    logging.info("num_processed_messages +=")
                    num_processed_messages += len(gdf.index)
                    logging.info("geopandas_to_files")
                    geopandas_to_files(gdf, regions, output_folder)
            except Exception as e:
                logger.error("Error when processing chunk!")
                logger.error(e)

        files_to_cloud_storage(output_folder, bucket_name)
        
        logger.info("Num of messages we should have processed: " + str(num_messages))
        logger.info("Num of messages we actually processed: " + str(num_processed_messages))
