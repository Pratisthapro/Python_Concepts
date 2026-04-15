import boto3
import logging
import io
import csv
import re
import sys

# ================= CONFIGURATION =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True
)

aws_bucket = "orlando-data"

# ================= NEVER HARDCODE AWS KEYS =================
aws_access_key = "AKIADIGD6"
aws_secret_key = "RQBLwJ7agrxITVwgUZAg+3"

# ================= CLEAN COLUMN NAMES =================
def clean_column_name(name):
    name = name.strip()
    name = name.lower()
    name = re.sub(r"[ \-\.\t]+", "_", name)
    name = re.sub(r"[^a-z0-9_]", "", name)
    name = re.sub(r"_+", "_", name)
    name = name.strip("_")
    return name


# ================= DELETE ALL FILES FROM OUTPUT PREFIX =================
def clear_s3_prefix(s3_client, aws_bucket, prefix):
    response = s3_client.list_objects_v2(Bucket=aws_bucket, Prefix=prefix)

    if "Contents" not in response:
        logging.info(f"No existing files found under output prefix: {prefix}")
        return

    for obj in response["Contents"]:
        s3_client.delete_object(Bucket=aws_bucket, Key=obj["Key"])

    logging.info(f"Cleared all existing files from output prefix: {prefix}")


# ================= CONVERT TXT TO CSV IN S3 =================
def convert_txt_to_csv_s3(s3_client, aws_bucket, input_prefix, output_prefix):

    # 🔴 NEW STEP: Clear output folder before processing
    clear_s3_prefix(
        s3_client=s3_client,
        aws_bucket=aws_bucket,
        prefix=output_prefix
    )

    response = s3_client.list_objects_v2(Bucket=aws_bucket, Prefix=input_prefix)

    if "Contents" not in response:
        logging.info("No .txt files found in S3 input folder.")
        return

    for obj in response["Contents"]:
        key = obj["Key"]

        if not key.lower().endswith(".txt"):
            continue

        logging.info(f"Processing file: {key}")

        base_name = key.split("/")[-1].rsplit(".", 1)[0]
        output_key = f"{output_prefix}{base_name}.csv"

        try:
            # -------- Read input file --------
            s3_object = s3_client.get_object(Bucket=aws_bucket, Key=key)
            file_stream = io.TextIOWrapper(s3_object["Body"], encoding="utf-8")

            output_stream = io.StringIO()
            writer = csv.writer(output_stream)

            first_row = True
            row_count = 0

            for line in file_stream:
                line = line.strip()
                if not line:
                    continue

                row = line.split("\t")

                if first_row:
                    row = [clean_column_name(col) for col in row]
                    first_row = False

                writer.writerow(row)
                row_count += 1

            # -------- Upload new file --------
            s3_client.put_object(
                Bucket=aws_bucket,
                Key=output_key,
                Body=output_stream.getvalue().encode("utf-8")
            )

            logging.info(f"SUCCESS: {key} → {output_key} | Rows processed: {row_count}")

        except Exception as e:
            logging.error(f"FAILED converting {key}. ERROR: {e}")


# ================= RUNNER =================
class Runner:
    @staticmethod
    def runner(*args, **kwargs):
        logging.info("Creating S3 client")

        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )

        input_prefix = "aetna/aetnaaco_incremental_file_new_incremental_files/"
        output_prefix = "aetna/aetna_incremental_files/new_aco_parsed_csv_file/"

        convert_txt_to_csv_s3(
            s3_client=s3_client,
            aws_bucket=aws_bucket,
            input_prefix=input_prefix,
            output_prefix=output_prefix
        )

        # ExecPy requires iterable return
        yield "DONE"
