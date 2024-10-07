import rasterio
from rasterio.mask import mask
from rasterio.warp import calculate_default_transform, reproject, Resampling
from shapely.geometry import box

sentinel_tile = "data/tiles/S2A_MSIL2A_20180612T095031_N0500_R079_T33SVB_20230716T080944.SAFE/GRANULE/L2A_T33SVB_A015520_20180612T095654/IMG_DATA/R10m/T33SVB_20180612T095031_B08_10m.jp2"
input_raster = "data/u2018_clc2018_v2020_20u1_raster100m/DATA/U2018_CLC2018_V2020_20u1.tif"
output_reprojected = "data/u2018_clc2018_v2020_20u1_raster100m/DATA/CLC2018_32633.tif"
output_clipped = "data/u2018_clc2018_v2020_20u1_raster100m/DATA/CLC2018_clipped.tif"

print("Getting bounding box")
with rasterio.open(sentinel_tile) as src:
    sentinel_bounds = src.bounds
    sentinel_crs = src.crs
    xmin, ymin, xmax, ymax = sentinel_bounds

print(f"Bounding Box del tile Sentinel-2: {xmin}, {ymin}, {xmax}, {ymax}")

print("Reproject raster")
with rasterio.open(input_raster) as src:
    dst_crs = sentinel_crs

    transform, width, height = calculate_default_transform(
        src.crs, dst_crs, src.width, src.height, *src.bounds)

    kwargs = src.meta.copy()
    kwargs.update({
        'crs': dst_crs,
        'transform': transform,
        'width': width,
        'height': height
    })

    with rasterio.open(output_reprojected, 'w', **kwargs) as dst:
        for i in range(1, src.count + 1):
            reproject(
                source=rasterio.band(src, i),
                destination=rasterio.band(dst, i),
                src_transform=src.transform,
                src_crs=src.crs,
                dst_transform=transform,
                dst_crs=dst_crs,
                resampling=Resampling.nearest
            )

print("Clip raster")
bbox = box(xmin, ymin, xmax, ymax)

with rasterio.open(output_reprojected) as src:
    bbox_transform = [bbox.bounds]

    out_image, out_transform = mask(src, [bbox], crop=True)

    out_meta = src.meta.copy()
    out_meta.update({
        "driver": "GTiff",
        "height": out_image.shape[1],
        "width": out_image.shape[2],
        "transform": out_transform
    })

    with rasterio.open(output_clipped, "w", **out_meta) as dest:
        dest.write(out_image)

print(f"New raster saved in: {output_clipped}")
