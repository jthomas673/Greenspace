import boto3
import rasterio
import geopandas as gpd
from shapely.geometry import box
import os

# AWS S3 Client
s3 = boto3.client("s3")

# Define source bucket and folder
bucket_name = "tgsp25"
source_folder = "CO_NAIP_Repository/tifs/relevant_tiles/"
output_shp_dir = "/tmp/tif_boundaries/"
output_shp = os.path.join(output_shp_dir, "tif_bounding_boxes.shp")

# Create output directory
os.makedirs(output_shp_dir, exist_ok=True)

# List all TIFF files in S3
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=source_folder)
tif_files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".tif")]

# Store bounding boxes
geometries = []
tif_names = []

# Process each TIFF in Batches (10 at a time to reduce memory usage)
batch_size = 10
for i in range(0, len(tif_files), batch_size):
    batch = tif_files[i:i+batch_size]
    print(f"Processing batch {i // batch_size + 1}/{len(tif_files) // batch_size + 1}")

    for tif_file in batch:
        try:
            local_tif_path = f"/tmp/{os.path.basename(tif_file)}"

            # Download the file temporarily
            s3.download_file(bucket_name, tif_file, local_tif_path)

            # Open with Rasterio to extract bounding box
            with rasterio.open(local_tif_path) as dataset:
                bbox = dataset.bounds  # (min_x, min_y, max_x, max_y)
                geom = box(*bbox)  # Convert to Polygon

                geometries.append(geom)
                tif_names.append(os.path.basename(tif_file))

            print(f"✅ Extracted bounding box for: {tif_file}")

            # Delete the file to save space
            os.remove(local_tif_path)

        except Exception as e:
            print(f"⚠️ Failed to process {tif_file}: {str(e)}")

# Convert to GeoDataFrame
gdf = gpd.GeoDataFrame({"filename": tif_names}, geometry=geometries, crs="EPSG:4326")

# Save to Shapefile
gdf.to_file(output_shp, driver="ESRI Shapefile")

print(f"✅ TIFF bounding boxes saved as {output_shp}")
