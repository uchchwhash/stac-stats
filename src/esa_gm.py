from datetime import datetime
import json
import multiprocessing
from pathlib import Path
import random
import typing
import sys

import boto3
import botocore
from dask import array as da
from pystac_client import Client
from pystac import ItemCollection
import numpy

from odc.algo import xr_geomedian, geomedian_with_mads
from odc.geo import BoundingBox
from odc.geo.xr import write_cog, assign_crs
from odc.stac import configure_rio, stac_load


s2_bands = [
    "red",
    "green",
    "blue",
    "visual",
    "nir",
    "swir22",
    "rededge2",
    "rededge3",
    "rededge1",
    "swir16",
    "wvp",
    "nir08",
    "scl",
    "aot",
    "coastal",
    "nir09",
    "cloud",
    "snow",
    "preview",
    "granule_metadata",
    "tileinfo_metadata",
    "product_metadata",
    "thumbnail",
]

query_crs = "EPSG:4326"
output_crs = "EPSG:32757"

measurements = ["blue", "green", "red", "nir"]
mad_bands = ["smad", "emad", "bcmad", "count"]
masking_band = "scl"
resolution = 10


product = "2026-Jan-Aug-10m"
s3_bucket = "dea-dme-dev"
s3_prefix = "products/solomons/imam/geomad"

chunks = {"x": 1000, "y": 1000}
threads_per_chunk = 4


class TaskMetaData(typing.NamedTuple):
    """
    Data storage for query parameters.

    Dates have the form 'YYYY-MM-DD'
    """

    start_date: str
    end_date: str


def log(*args, **kwargs):
    print(*args, **kwargs)
    sys.stdout.flush()


def read_tasks_list():
    with open("/src/gm_tasks.list") as fl:
        return [line.strip() for line in fl]


def write_tasks_list(tasks_list):
    with open("/src/gm_tasks.list", "w") as fl:
        for task in tasks_list:
            print(task, file=fl)


def extract_feature(region_code):
    with open("/src/mgrs.geojson") as fl:
        data = json.load(fl)

    features = data["features"]

    for feature in features:
        if feature["properties"]["region_code"] == region_code:
            return feature

    raise ValueError(f"region not found: {region_code}")


def bounds(feature):
    geom = feature["geometry"]
    assert geom["type"] == "Polygon"
    coords = geom["coordinates"]
    assert len(coords) == 1
    points = coords[0]
    lons = [p[0] for p in points]
    lats = [p[1] for p in points]
    left, right = min(lons), max(lons)
    top, bottom = max(lats), min(lats)

    return BoundingBox(left=left, top=top, right=right, bottom=bottom, crs=query_crs)


def search(bbox, meta: TaskMetaData):
    stac_client = Client.open("https://earth-search.aws.element84.com/v1/")
    l2col = "sentinel-2-c1-l2a"

    date_query = f"{meta.start_date}/{meta.end_date}"

    return stac_client.search(
        collections=[l2col],
        datetime=date_query,
        bbox=bbox.bbox,
    ).item_collection()


def load_mask(items, bbox):
    mask_ds = stac_load(
        items=items,
        bands=[masking_band],
        crs=output_crs,
        resolution=resolution,
        bbox=bbox,
        resampling="nearest",
        dtype="int16",
        chunks=chunks,
    )

    masking_data = mask_ds[masking_band]

    # 0: no data, 1: saturated, 2: cast shadow
    # 3: cloud shadow, 4: vegetation, 5: not-vegetated
    # 6: water, 7: unclassified, 8: cloud (medium)
    # 9: cloud (high), 10: cirrus, 11: snow
    return ~masking_data.isin([0, 1, 2, 3, 8, 9, 10])


def load_optical(items, bbox):
    optical_ds = stac_load(
        items=items,
        bands=measurements,
        crs=output_crs,
        resolution=resolution,
        bbox=bbox,
        resampling="average",
        dtype="float32",
        chunks=chunks,
    )

    nodata = 0
    scale = 0.0001
    offset = -0.1
    rescale = 10000.0

    for band in measurements:
        optical_ds[band] = (
            optical_ds[band].dims,
            da.where(optical_ds[band].data != nodata, optical_ds[band].data, numpy.nan),
        )
        optical_ds[band] = (optical_ds[band] * scale + offset) * rescale
        optical_ds[band] = optical_ds[band].clip(0, rescale)
    return optical_ds


def load(items, bbox):
    log("loading mask", datetime.now())
    mask = load_mask(items, bbox)
    log("loading bands", datetime.now())
    optical_ds = load_optical(items, bbox)

    log("masking", datetime.now())
    for band in measurements:
        optical_ds[band] = (
            optical_ds[band].dims,
            da.where(mask, optical_ds[band], numpy.nan),
        )

    return optical_ds


def write_input_data(ds):
    for i, time in enumerate(numpy.datetime_as_string(ds["time"].data)):
        for band in measurements:
            write_cog(
                ds[band].isel(time=i).compute(),
                f"/output/{band}_{time}_{i}.tif",
                overwrite=True,
            )


def write_geomedian(gm, region_code, upload=False):
    root = Path("/output")
    folder = f"esa_s2_gm/{region_code}"
    (root / folder).mkdir(parents=True, exist_ok=True)

    for band in (measurements + mad_bands):
        filename = f"{folder}/gm_{product}_{region_code}_{band}.tif"
        write_cog(
            gm[band],
            str(root / filename),
            overwrite=True,
            compress="zstd",
            zstd_level=16,
            predictor=3 if band != "count" else 2,
        )

    filename = f"{folder}/gm_{product}_{region_code}.completed"
    with open(root / filename, "w") as fl:
        print("done!", file=fl)

    if not upload:
        return

    s3_client = boto3.client("s3")
    for band in (measurements + mad_bands):
        filename = f"{folder}/gm_{product}_{region_code}_{band}.tif"
        s3_client.upload_file(
            str(root / filename), s3_bucket, f"{s3_prefix}/{filename}"
        )

    filename = f"{folder}/gm_{product}_{region_code}.completed"
    s3_client.upload_file(str(root / filename), s3_bucket, f"{s3_prefix}/{filename}")


def check_exists(region_code):
    s3_client = boto3.client("s3")
    folder = f"esa_s2_gm/{region_code}"
    filename = f"{folder}/gm_{product}_{region_code}.completed"
    try:
        s3_client.head_object(Bucket=s3_bucket, Key=f"{s3_prefix}/{filename}")
        return True
    except botocore.exceptions.ClientError:
        return False


def execute_task(region_code, meta: TaskMetaData):
    ncpus = multiprocessing.cpu_count()
    num_workers = int(ncpus / threads_per_chunk)
    configure_rio(cloud_defaults=True)

    bbox = bounds(extract_feature(region_code))
    log("searching", bbox.bbox, region_code, datetime.now())
    items = search(bbox, meta)
    log("loading", datetime.now())
    ds = load(items, bbox)
    # log('writing input', datetime.now())
    # write_input_data(ds)
    log("geomedian", datetime.now())
    gm = geomedian_with_mads(
        ds,
        reshape_strategy="yxbt",
        work_chunks=(chunks["y"], chunks["x"]),
        num_threads=threads_per_chunk,
    )
    log("compute with", ncpus, "cpus", num_workers, "workers", datetime.now())
    computed = gm.load(scheduler="threads", num_workers=num_workers)
    log("writing", datetime.now())
    write_geomedian(assign_crs(computed, crs=output_crs), region_code)

    log("done", datetime.now())


def main():
    # TODO: gather date strings & job specific params here as needed
    meta = TaskMetaData(start_date="2026-01-01", end_date="2026-08-31")

    tasks_list = read_tasks_list()

    while tasks_list != []:
        region_code = random.choice(tasks_list)

        if not check_exists(region_code):
            execute_task(region_code, meta)
        else:
            log(region_code, "already exists!")

        tasks_list.remove(region_code)
        write_tasks_list(tasks_list)


if __name__ == "__main__":
    main()
