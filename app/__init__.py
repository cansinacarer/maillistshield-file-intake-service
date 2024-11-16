import time
import os
from io import StringIO
import pandas as pd

from app.config import POLLING_INTERVAL, s3, S3_BUCKET_NAME
from app.database import (
    file_has_a_job_in_db,
    get_job_status,
    set_job_status,
    has_header_row,
    get_email_column,
    has_email_column,
    get_user_of_file,
    set_row_count,
)
from app.utilities import (
    list_files,
    is_file_old_enough_to_delete,
    delete_file,
    download_file,
    upload_csv_buffer,
)
from app.uptime import ping_uptime_monitor


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
    file_extension = item["Key"].split(".")[-1]

    # Download the file
    local_temp_file = f"temp.{file_extension}"
    local_temp_file_path = os.path.join(os.path.dirname(__file__), local_temp_file)
    download_file(item["Key"], local_temp_file)

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
        return None

    # Delete the locally downloaded file after reading into df
    os.remove(local_temp_file_path)

    return df


def upload_df_as_csv(df, item_key):
    file_name = item_key.split("validation/uploaded/")[-1]
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    upload_csv_buffer(csv_buffer, file_name)


def process_files(all_files):
    # Loop the files in the uploaded directory
    for item in all_files:
        # Don't grab the folder itself
        if item["Key"] == "validation/uploaded/":
            continue

        # If the file has been here for a long time with no matching db record
        # This should also clear the errored out files
        if is_file_old_enough_to_delete(item):
            print(f'Deleting {item["Key"]} because it is too old')
            delete_file(item["Key"])
            try:
                set_job_status(item["Key"], "error_too_old")
            except:
                pass  # There may not be a matching db record, it is fine
            continue

        # Skip file if we find a matching db record
        if not file_has_a_job_in_db(item["Key"]):
            print(f'We did not find a db record for {item["Key"]}')
            continue

        # Skip file if db says the file is not pending_start
        # It might be because we previously set an error status for it
        # It might be a file just created, but we are seeing it before the database record is created
        if get_job_status(item["Key"]) != "pending_start":
            print(f'{item["Key"]} has a db record but it is not pending_start')
            continue

        df = read_file_into_df(item)

        # If read_file_into_df returned None
        if df is None:
            print(f'We could not load {item["Key"]} into a df')
            set_job_status(item["Key"], "error_df")

        # If user declared that the file has multiple columns
        if has_email_column(item["Key"]):
            df = only_keep_column(df, get_email_column(item["Key"]))

        # Skip file if user has not declared an email column but there are multiple columns
        if df.shape[1] > 1:
            print(f'Column count doesn\'t match user declaration for {item["Key"]}')
            set_job_status(item["Key"], "error_column_count")
            continue

        # At this point, we are guaranteed to
        # - have only one column,
        # - have a header, even if user file didn't have it
        # During read_file_into_df, we also checked whether user declared a header row
        # and read it to df accordingly
        # So we name that single column with a guaranteed header "Email"
        df = name_column_email(df)

        # Drop empty rows
        df.dropna(inplace=True)

        # Calculate remaining row count
        row_count = df.shape[0]
        set_row_count(item["Key"], row_count)

        # If the user does not have enough credits, error out
        user = get_user_of_file(item["Key"])
        if user.credits < row_count:
            print(f'User does not have enough credits to validate {item["Key"]}')
            set_job_status(item["Key"], "error_insufficient_credits")
            continue

        # Deduct credits
        user.deduct_credits(row_count)

        # Save the processed df to 'in-progress' as the new file
        upload_df_as_csv(df, item["Key"])

        # Delete the original file
        delete_file(item["Key"])

        # Update job status
        print(f'File {item["Key"]} has been processed successfully.')
        set_job_status(item["Key"], "file_accepted")


def main():
    # Main loop
    while True:
        # Iterations start time
        start_time = time.time()

        # List all files in validation/uploaded/
        all_files = list_files()

        # Process the files
        process_files(all_files)

        # Send a heartbeat to the uptime monitor
        ping_uptime_monitor()

        # Iteration end time
        end_time = time.time()
        elapsed_time = end_time - start_time
        # If the elapsed time is not as long as the polling interval, sleep until it is
        sleep_time = POLLING_INTERVAL - elapsed_time
        if sleep_time > 0:
            time.sleep(sleep_time)
