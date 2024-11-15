import time
import os
import pandas as pd

from app.config import POLLING_INTERVAL, s3, S3_BUCKET_NAME
from app.database import file_has_a_job_in_db, get_job_status
from app.utilities import is_file_old_enough_to_delete, delete_files, download_file
from app.uptime import ping_uptime_monitor


# Returns the list of newly uploaded files
# Delete the old orphan files while looping
def list_files_and_delete_orphans():
    # Check if there are any new files in the S3 bucket
    s3_response = s3.meta.client.list_objects_v2(
        Bucket=S3_BUCKET_NAME, Prefix="validation/uploaded/"
    )

    # Make the list
    s3_files_list = []
    files_to_be_deleted = []
    for item in s3_response.get("Contents", []):
        if item["Key"] != "validation/uploaded/":
            if is_file_old_enough_to_delete(item):
                files_to_be_deleted.append(item["Key"])
            else:
                s3_files_list.append(item["Key"])
    delete_files(files_to_be_deleted)

    return s3_files_list


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


def record_file_sizes(file):
    row_count = get_row_count(file) + 1  # +1 because shape starts counting at 0


# I SHOULD PROBABLY LOOP THE FILES HERE ANYWAYS
def main():
    # Main loop
    while True:
        # Iterations start time
        start_time = time.time()

        # List all files in validation/uploaded/
        all_files = list_files_and_delete_orphans()

        # New files to be processed
        new_files = list_new_files(all_files)

        # Record file sizes
        record_file_sizes(new_files)

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
