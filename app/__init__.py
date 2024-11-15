import time
import os
import pandas as pd

from app.config import POLLING_INTERVAL, s3, S3_BUCKET_NAME
from app.database import (
    file_has_a_job_in_db,
    get_job_status,
    has_header_row,
    get_email_column,
    has_email_column,
)
from app.utilities import is_file_old_enough_to_delete, delete_file, download_file
from app.uptime import ping_uptime_monitor


# Returns the list of newly uploaded files
# Delete the old orphan files while looping
def list_files():
    # Check if there are any new files in the S3 bucket
    s3_response = s3.meta.client.list_objects_v2(
        Bucket=S3_BUCKET_NAME, Prefix="validation/uploaded/"
    )

    return s3_response.get("Contents", [])


# Filter the list for the ones that have a pending_start record
def list_new_files(files):
    new_files = []
    for file in files:
        if file_has_a_job_in_db(file):
            if get_job_status(file) == "pending_start":
                new_files.append(file)

    return new_files


def get_row_count(file):
    file_extension = file.split(".")[-1]
    if file_extension == "xlsx" or file_extension == "xls":
        local_temp_file = f"temp.{file_extension}"
        local_temp_file_path = os.path.join(os.path.dirname(__file__), local_temp_file)
        download_file(file, local_temp_file)
        df = pd.read_excel(local_temp_file_path)
        row_count = df.shape[0]
        os.remove(local_temp_file_path)
        return row_count


#
#
#


def only_keep_column(df, column_name):
    return pd.DataFrame(df[column_name])


# If it doesn't have a header row and we confirmed that there is one column
# Name that column Email
def name_column_email(df):
    # Get the current column name
    current_column_name = df.columns[0]

    # Rename the column to 'Email'
    df.rename(columns={current_column_name: "Email"}, inplace=True)

    return df


def read_file_into_df(item):
    # Download the file
    local_temp_file = f"temp.{file_extension}"
    local_temp_file_path = os.path.join(os.path.dirname(__file__), local_temp_file)
    download_file(item["Key"], local_temp_file)
    file_extension = item["Key"].split(".")[-1]

    # Process the file
    if file_extension == "xlsx" or file_extension == "xls":
        # Should the first row be the df headers?
        if has_header_row(item["Key"]):
            df = pd.read_excel(local_temp_file_path)
        else:
            df = pd.read_excel(local_temp_file_path, header=None)

    elif file_extension == "csv":
        # Should the first row be the df headers?
        if has_header_row(item["Key"]):
            df = pd.read_csv(local_temp_file_path)
        else:
            df = pd.read_csv(local_temp_file_path, header=None)
    else:
        print(f"Unexpected file extension: {file_extension}")
        continue

    # Delete the locally downloaded file after reading into df
    os.remove(local_temp_file_path)

    return df


def main():
    # Main loop
    while True:
        # Iterations start time
        start_time = time.time()

        # List all files in validation/uploaded/
        all_files = list_files()

        # Loop the files
        for item in all_files:
            # Don't grab the folder itself
            if item["Key"] != "validation/uploaded/":
                if is_file_old_enough_to_delete(item):
                    delete_file(item["Key"])
                else:
                    # If we find a matching db record
                    if file_has_a_job_in_db(item["Key"]):

                        # If db says the file is pending_start
                        if get_job_status(item["Key"]) == "pending_start":
                            df = read_file_into_df(item)

                            # If user declared that the file has multiple columns
                            if has_email_column(item["Key"]):
                                df = only_keep_column(df, get_email_column(item["Key"]))

                            # If user has not declared an email column but there are multiple columns
                            if df.shape[1] > 1:
                                # TODO: Job status error: detected multiple columns but user did not declare an email column
                                # ALSO put a giant try except around these file operations
                                pass

                            # After we guaranteed to have only one column, name that Email
                            df = name_column_email(df)

                            # Drop empty rows
                            df.dropna(inplace=True)

                            # Calculate remaining row count
                            row_count = (
                                df.shape[0] + 1
                            )  # +1 because shape starts counting at 0

        # Check if the user has enough credits

        # Send a heartbeat to the uptime monitor
        ping_uptime_monitor()

        # Iteration end time
        end_time = time.time()
        elapsed_time = end_time - start_time
        # If the elapsed time is not as long as the polling interval, sleep until it is
        sleep_time = POLLING_INTERVAL - elapsed_time
        if sleep_time > 0:
            time.sleep(sleep_time)
