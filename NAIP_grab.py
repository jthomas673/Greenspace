import geopandas as gpd
import boto3
import pandas as pd
import re

# Set up AWS S3 client
s3 = boto3.client("s3")

# Define source and target S3 buckets
source_bucket = "naip-analytic"  # NAIP dataset bucket
target_bucket = "tgsp25"  # Your S3 bucket for storing downloaded TIFFs

# Define S3 path to the shapefile in your tgsp25 bucket
shapefile_prefix = "City_tile_shapes"
shapefile_name = "cities_feature_tiles.shp"

local_shapefile_path = f"/tmp/{shapefile_name}"
local_dbf_path = f"/tmp/cities_feature_tiles.dbf"
local_shx_path = f"/tmp/cities_feature_tiles.shx"

# Download shapefile components from S3
s3.download_file(target_bucket, f"{shapefile_prefix}/{shapefile_name}", local_shapefile_path)
s3.download_file(target_bucket, f"{shapefile_prefix}/cities_feature_tiles.dbf", local_dbf_path)
s3.download_file(target_bucket, f"{shapefile_prefix}/cities_feature_tiles.shx", local_shx_path)

# Load the shapefile into a GeoDataFrame
gdf = gpd.read_file(local_shapefile_path)

# Extract all unique APFONAME values
apfoname_data = gdf[["APFONAME"]]

# Save the APFONAME data to a CSV for processing
csv_path = "/tmp/apfoname_data.csv"
apfoname_data.to_csv(csv_path, index=False)

print(f"APFONAME data saved to {csv_path}! with length {len(apfoname_data)}")