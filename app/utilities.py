import datetime
import os
from app.config import (
    appTimezone,
    RETENTION_PERIOD_FOR_ORPHAN_FILES,
    S3_BUCKET_NAME,
    s3,
)
from app.logging import logger


# Returns the list of newly uploaded files
def list_files():
    # Check if there are any new files in the S3 bucket
    s3_response = s3.meta.client.list_objects_v2(
        Bucket=S3_BUCKET_NAME, Prefix="validation/uploaded/"
    )

    return s3_response.get("Contents", [])


def is_file_old_enough_to_delete(item):
    last_modified = item["LastModified"].astimezone(appTimezone)
    now = datetime.datetime.now().astimezone(appTimezone)
    age = now - last_modified
    age_in_seconds = age.total_seconds()
    return age_in_seconds > RETENTION_PERIOD_FOR_ORPHAN_FILES


def delete_file(key):
    objects = [{"Key": key}]
    try:
        s3.Bucket(S3_BUCKET_NAME).delete_objects(Delete={"Objects": objects})
    except Exception as e:
        logger.error("Error while deleting file: ", e)


def download_file(key_name, local_name):
    file_path = os.path.join(os.path.dirname(__file__), local_name)

    try:
        s3.Bucket(S3_BUCKET_NAME).download_file(key_name, file_path)
    except Exception as e:
        logger.error("Error while downloading file: ", e)


def upload_csv_buffer(csv_buffer, file_name):
    try:
        s3.meta.client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key="validation/in-progress/" + file_name,
            Body=csv_buffer.getvalue(),
        )
    except Exception as e:
        logger.error("Error while uploading file: ", e)
