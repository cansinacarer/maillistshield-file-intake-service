# Mail List Shield - File Intake Service

This is the process that monitors the folder where files are first uploaded (`uploaded`), processes them, 
This microservice runs a monitoring loop to check the `/validation/uploaded` directory on the S3 bucket and the `Jobs` table in the database for matching job records. 

When a new file and a corresponding job is found, this service performs the following tasks:

- Remove all columns except email,
- Remove empty rows,
- Rename the email column as "Email",
- Count the rows and record it into the job record in the database,
- Deduct credits based on the record count in the cleaned up file,
- Create a standardized version of the file in `/validation/in-progress` in the S3 bucket.

Excel files are supported as input, but all outputs are converted to csv.

## The Loop

The files are processed in FIFO by the main loop, with the following actions performed in order:

- List files,
- Process files,
- List files,
- ...

This loop can be paused by setting an environment variable: `PAUSE=True`.

## Batch Job States

- Expected before:
  - `pending_start`
- Error states
  - `error_too_old` : file deleted because it has been here too long
  - `error_df` : file could not be read
  - `error_column_count` : user did not select a column name but we detect more than 1 column
  - `error_insufficient_credits` :  User didn't have enough credits to process the file.
- Success state:
  - `file_accepted`

## Clean up of orphan files

If a file is found but a corresponding job is not found, there is a retention period to allow for delays in database update. This retention period is declared in seconds with the environment variable `RETENTION_PERIOD_FOR_ORPHAN_FILES`. If there is no job record found in the database for a file found on the S3 bucket at the end of the retention period, the file is deleted.

---

See the [main repository](https://github.com/cansinacarer/maillistshield-com) for a complete list of other microservices.
