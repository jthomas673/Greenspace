import boto3
import pandas as pd
import re
from collections import defaultdict

# Set up AWS S3 client
s3 = boto3.client("s3")

# Define source and target S3 buckets
source_bucket = "naip-analytic"
target_bucket = "tgsp25"

# Path to the CSV containing APFONAME values
csv_path = "/tmp/apfoname_data.csv"

# Folder path in the target S3 bucket
target_folder = "CO_NAIP_Repository/tifs/relevant_tiles/"

# Load APFONAME values
tile_ids = pd.read_csv(csv_path)

# Extract unique APFONAMEs from filenames
apfoname_counts = defaultdict(int)

# Function to list available files in NAIP
def list_matching_files(apfoarea):
    prefix = f"co/2021/60cm/rgbir_cog/{apfoarea}/"
    response = s3.list_objects_v2(Bucket=source_bucket, Prefix=prefix, RequestPayer="requester")
    
    matching_files = []
    if "Contents" in response:
        for obj in response["Contents"]:
            match = re.search(r"m_(\d+)_se_13_060_(\d{8})\.tif$", obj["Key"])
            if match:
                apfoname, date = match.groups()
                filename = f"m_{apfoname}_se_13_060_{date}.tif"
                matching_files.append((apfoname, filename, obj["Key"]))
    
    return matching_files

# Function to check if a file exists in S3
def file_exists_in_s3(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False

# Process each APFONAME and handle duplicates
for index, row in tile_ids.iterrows():
    apfoname = str(row["APFONAME"])
    apfoarea = apfoname[:5]  # Extract first 5 digits for APFOAREA
    matching_files = list_matching_files(apfoarea)

    for apfoname, filename, source_path in matching_files:
        apfoname_counts[filename] += 1  # Track occurrences
        base_name, ext = filename.rsplit(".", 1)

        # If it's the first occurrence, keep the original filename
        if apfoname_counts[filename] == 1:
            new_filename = filename
        else:
            new_filename = f"{base_name} ({apfoname_counts[filename]-1}).{ext}"  # Append copy number

        s3_target_path = f"{target_folder}{new_filename}"

        print(f"Processing {filename} -> Saving as {new_filename} in S3...")

        # Copy the file to S3 with the correct name
        s3.copy_object(
            Bucket=target_bucket,
            CopySource={"Bucket": source_bucket, "Key": source_path},
            Key=s3_target_path,
            MetadataDirective="COPY",
            RequestPayer="requester"
        )

        print(f"Successfully saved {new_filename} to {s3_target_path}.")

print("Duplicate detection and renaming completed.")