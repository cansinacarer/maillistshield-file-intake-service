import datetime
import os
from app.config import (
    appTimezone,
    RETENTION_PERIOD_FOR_ORPHAN_FILES,
    S3_BUCKET_NAME,
    s3,
)


def is_file_old_enough_to_delete(item):
    last_modified = item["LastModified"].astimezone(appTimezone)
    now = datetime.datetime.now().astimezone(appTimezone)
    age = now - last_modified
    age_in_seconds = age.total_seconds()
    return age_in_seconds > RETENTION_PERIOD_FOR_ORPHAN_FILES


def delete_files(keys):
    objects = []
    for key in keys:
        objects.append({"Key": key})
    try:
        s3.Bucket(S3_BUCKET_NAME).delete_objects(Delete={"Objects": objects})
    except Exception as e:
        print("Error: ", e)


def download_file(key_name, local_name):
    file_path = os.path.join(os.path.dirname(__file__), local_name)

    try:
        s3.Bucket(S3_BUCKET_NAME).download_file(key_name, file_path)
    except Exception as e:
        print("Error: ", e)
