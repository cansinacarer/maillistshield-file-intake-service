from decouple import config
import pytz
import boto3

# S3 bucket name
S3_BUCKET_NAME = config("S3_BUCKET_NAME")

# Polling interval (in seconds) for checking the S3 bucket for new files, pinging uptime monitor, etc.
POLLING_INTERVAL = config("POLLING_INTERVAL", cast=int)

# Number of seconds a file without a matching database record will be retained
RETENTION_PERIOD_FOR_ORPHAN_FILES = config(
    "RETENTION_PERIOD_FOR_ORPHAN_FILES", cast=int
)

# Uptime monitor address
UPTIME_MONITOR = config("UPTIME_MONITOR")

# Database connection
DATABASE_CONNECTION_STRING = config("DATABASE_CONNECTION_STRING")


# Timezone used in this app
appTimezoneStr = config("TIMEZONE")
appTimezone = pytz.timezone(appTimezoneStr)

# S3 object
S3_ENDPOINT = config("S3_ENDPOINT")
S3_KEY = config("S3_KEY")
S3_SECRET = config("S3_SECRET")
s3 = boto3.resource(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_KEY,
    aws_secret_access_key=S3_SECRET,
)
