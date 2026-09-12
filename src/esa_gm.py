from datetime import datetime
import json
import multiprocessing
from pathlib import Path
import random
import typing
import sys
from random import randint

import boto3
import botocore
from dask import array as da
import dask.distributed
from pystac_client import Client
from pystac import ItemCollection
import numpy
import xarray

from odc.algo import xr_geomedian, geomedian_with_mads
from odc.geo import BoundingBox
from odc.geo.xr import write_cog, assign_crs
from odc.stac import configure_rio, stac_load

query_crs = "EPSG:4326"
output_crs = "EPSG:32757"

measurements_10m = ["blue", "green", "red", "nir"]
measurements_20m = ["swir22", "rededge2", "rededge3", "rededge1", "swir16", "nir08"]
mad_bands = ["smad", "emad", "bcmad", "count"]
masking_band = "scl"
resolution = 20
measurements = measurements_20m

product = f"2026-Jan-Aug-s2-{resolution}m-MAD"
s3_bucket = "dea-dme-dev"
s3_prefix = f"products/solomons/imam/geomad/{product}"
workspace = f"products/solomons/imam/geomad/tmp-workspace"

chunks = {"x": 1000, "y": 1000}
threads_per_chunk = 8


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

    return mask_ds[masking_band]


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

    return optical_ds


def mask_invalid(optical, mask):
    # 0: no data, 1: saturated, 2: cast shadow
    # 3: cloud shadow, 4: vegetation, 5: not-vegetated
    # 6: water, 7: unclassified, 8: cloud (medium)
    # 9: cloud (high), 10: cirrus, 11: snow
    mask = ~mask.isin([0, 1, 2, 3, 8, 9, 10])

    nodata = 0
    scale = 0.0001
    offset = -0.1
    rescale = 10000.0

    optical = optical.where(optical != nodata)
    optical = (optical * scale + offset) * rescale
    optical = optical.clip(0, rescale)
    optical = optical.where(mask)
    return optical


def load(items, bbox):
    log("loading mask", datetime.now())
    mask_da = load_mask(items, bbox).persist()
    log("loading bands", datetime.now())
    optical_ds = load_optical(items, bbox)

    log("masking", datetime.now())
    for band in measurements:
        optical_ds[band] = xarray.map_blocks(
            mask_invalid,
            optical_ds[band],
            (mask_da,),
            template=optical_ds[band],
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


def write_geomedian(gm, region_code, upload=True):
    root = Path("/output")
    folder = f"esa_s2_gm/{region_code}"
    (root / folder).mkdir(parents=True, exist_ok=True)

    for band in measurements + mad_bands:
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
    for band in measurements + mad_bands:
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


def setup_dask_with_rio(num_workers, threads_per_worker):
    cluster = dask.distributed.LocalCluster(
        processes=False,
        n_workers=num_workers,
        threads_per_worker=threads_per_worker,
        local_directory="/dask-workspace",
    )
    dask_client = dask.distributed.Client(cluster)
    configure_rio(cloud_defaults=True, client=dask_client)
    return dask_client


def write_zarr(ds):
    prefix = "obs"
    filename = f"{prefix}-{randint(0, 0xFFFFFFFF):08x}.zarr"
    store = f"{workspace}/{filename}"
    log("writing to zarr", datetime.now())
    ds.to_zarr(store, mode="w", consolidated=True, storage_options={"anon": False})
    log("done writing to zarr", datetime.now())
    return store


def execute_task(region_code, meta: TaskMetaData):
    ncpus = multiprocessing.cpu_count()
    num_workers = int(ncpus / threads_per_chunk)
    dask_client = setup_dask_with_rio(num_workers, threads_per_chunk)

    bbox = bounds(extract_feature(region_code))
    log("searching", bbox.bbox, region_code, datetime.now())
    items = search(bbox, meta)
    log("loading", datetime.now())
    ds = load(items, bbox)
    # log('writing input', datetime.now())
    # write_input_data(ds)

    ds = xarray.open_zarr(
        write_zarr(ds),
        chunks={"time": 1, "x": chunks["x"], "y": chunks["y"]},
        consolidated=True,
    )

    log("geomedian", datetime.now())
    gm = geomedian_with_mads(
        ds,
        reshape_strategy="yxbt",
        work_chunks=(chunks["y"], chunks["x"]),
        num_threads=threads_per_chunk,
    )
    log("compute with", ncpus, "cpus", num_workers, "workers", datetime.now())
    computed = gm.load()
    log("writing", datetime.now())
    write_geomedian(assign_crs(computed, crs=output_crs), region_code)

    log("done", datetime.now())
    dask_client.shutdown()


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
