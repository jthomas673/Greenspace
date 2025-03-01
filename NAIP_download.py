import boto3
import pandas as pd
import re
import threading
from queue import Queue

# Set up AWS S3 client
s3 = boto3.client("s3")

# Define source and target S3 buckets
source_bucket = "naip-analytic"  # NAIP dataset bucket
target_bucket = "tgsp25"  # Your S3 bucket

# Path to the CSV containing APFONAME values
csv_path = "/tmp/apfoname_data.csv"

# Folder path in the target S3 bucket where TIFFs will be stored
target_folder = "CO_NAIP_Repository/tifs/relevant_tiles/"

# Load extracted APFONAME values from CSV
tile_ids = pd.read_csv(csv_path)

# Convert APFONAME values to a set for fast lookups
apfoname_set = set(tile_ids["APFONAME"].astype(str))

# Function to check if a file exists in the target S3 bucket
def file_exists_in_s3(s3_bucket, s3_key):
    try:
        s3.head_object(Bucket=s3_bucket, Key=s3_key)
        return True  # File exists
    except s3.exceptions.ClientError:
        return False  # File does not exist

# Function to find available files in the NAIP S3 bucket
def list_matching_files(apfoarea):
    """
    List available TIFF files in the NAIP S3 bucket and filter them
    based on the APFONAME values from the CSV.
    """
    prefix = f"co/2021/60cm/rgbir_cog/{apfoarea}/"
    response = s3.list_objects_v2(Bucket=source_bucket, Prefix=prefix, RequestPayer="requester")

    matching_files = []
    if "Contents" in response:
        for obj in response["Contents"]:
            # Extract APFONAME from filename using regex
            match = re.search(r"m_(\d+)_se_13_060_\d{8}\.tif$", obj["Key"])
            if match:
                apfoname = match.group(1)  # Extract APFONAME from filename
                if apfoname in apfoname_set:  # Only keep files that match CSV data
                    matching_files.append(obj["Key"])

    return matching_files

# Function to process a queue of files (multi-threading)
def process_files(queue):
    while not queue.empty():
        try:
            file_path, apfoname = queue.get()

            # Extract filename
            filename = file_path.split("/")[-1]

            # Construct the target S3 path
            s3_target_path = f"{target_folder}{filename}"

            # Check if file already exists in S3 before processing
            if file_exists_in_s3(target_bucket, s3_target_path):
                print(f"Skipping {filename} (Already Exists in S3)")
            else:
                # Print the APFONAME before copying
                print(f"Processing APFONAME: {apfoname} -> Copying {file_path} directly to {s3_target_path}...")

                # Stream file from NAIP S3 to Target S3
                s3.copy_object(
                    Bucket=target_bucket,
                    CopySource={"Bucket": source_bucket, "Key": file_path},
                    Key=s3_target_path,
                    MetadataDirective="COPY",
                    RequestPayer="requester"
                )

                print(f"Successfully copied {filename} to {s3_target_path}.")

        except Exception as e:
            print(f"Failed to process {apfoname}: {str(e)}")
        finally:
            # Only mark the task as done if it was retrieved from the queue
            queue.task_done()

# Create a queue for multi-threading
file_queue = Queue()

# Find matching TIFF files for each APFONAME in the CSV
for apfoname in apfoname_set:
    apfoarea = apfoname[:5]  # Extract first 5 digits for APFOAREA
    matching_files = list_matching_files(apfoarea)
    for file in matching_files:
        file_queue.put((file, apfoname))  # Store APFONAME with file path

# Start multi-threading with 10 concurrent workers
num_threads = 10
threads = []
for _ in range(num_threads):
    t = threading.Thread(target=process_files, args=(file_queue,))
    t.start()
    threads.append(t)

# Wait for all threads to finish
file_queue.join()

print("Filtered NAIP TIFF streaming completed.")
