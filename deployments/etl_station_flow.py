#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Define utilities to retrieve station status data."""

# pylint: disable=invalid-name,dangerous-default-value
# pylint: disable=too-many-locals,unused-argument,unnecessary-lambda
# pylint: disable=too-many-arguments

import io
import os
from calendar import month_name
from datetime import date, datetime
from typing import List

import boto3
import polars as pl
import pytz
import requests
from contexttimer import Timer
from dotenv import find_dotenv, load_dotenv
from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(
    name="extract-urls",
    description="Extract station status and info for specified systems",
    tags=["extract-urls"],
    log_prints=True,
)
def get_api_urls(system_auto_disc_url: str) -> List[str]:
    """."""
    feeds_response = requests.get(system_auto_disc_url)
    assert feeds_response.status_code == 200
    feeds = feeds_response.json()["data"]["en"]["feeds"]
    status_url = [
        feed["url"] for feed in feeds if feed["name"] == "station_status"
    ][0]
    info_url = [
        feed["url"] for feed in feeds if feed["name"] == "station_information"
    ][0]
    return [info_url, status_url]


def extract_station_info_data(url: str) -> pl.DataFrame:
    """."""
    with Timer() as t:
        info_response = requests.get(url)
        assert info_response.status_code == 200
    print(f"Got response from info endpoint in {t.elapsed:,.3f} seconds")
    info_records = info_response.json()["data"]["stations"]
    df = pl.DataFrame(info_records)
    return df


def extract_station_status_data(url: str) -> pl.DataFrame:
    """."""
    with Timer() as t:
        status_response = requests.get(url)
        assert status_response.status_code == 200
    print(f"Got response from status endpoint in {t.elapsed:,.3f} seconds")
    status_records = status_response.json()["data"]["stations"]
    df = pl.DataFrame(status_records)
    return df


@task(
    name="extract-status-info",
    retries=2,
    retry_delay_seconds=5,
    description="Extract station status and info for specified systems",
    tags=["extract-status-info"],
    log_prints=True,
)
def extract(url_info: str, url_status: str) -> List[pl.DataFrame]:
    """."""
    df_info = extract_station_info_data(url_info)
    df_status = extract_station_status_data(url_status)

    nrows, ncols = df_status.shape
    msg = (
        f"Extracted {nrows:,} rows and {ncols:,} columns of raw data "
        f"from {url_status}"
    )
    print(msg)
    return [df_info, df_status]


def transform_info_data(df: pl.DataFrame) -> pl.DataFrame:
    """."""
    dtypes = {
        "station_id": pl.Int16,
        "name": pl.Utf8,
        "capacity": pl.Int8,
        "lat": pl.Float32,
        "lon": pl.Float32,
    }

    df = df.select(list(dtypes)).with_columns(
        [pl.col(col).cast(col_dtype) for col, col_dtype in dtypes.items()]
    )
    return df


def transform_station_status_data(
    df: pl.DataFrame,
    auto_disc_url: str,
    status_url: str,
    tz: str = "America/Toronto",
    country_code: str = "Canada",
    system_name: str = "Bike Share Toronto",
    location: str = "Toronto",
) -> pl.DataFrame:
    """."""
    dtypes = dict(
        station_id=pl.Int16,
        num_bikes_available=pl.Int8,
        num_bikes_disabled=pl.Int8,
        num_docks_available=pl.Int8,
        num_docks_disabled=pl.Int8,
        last_reported=pl.Int64,
        status=pl.Categorical,
        is_installed=pl.Boolean,
        is_renting=pl.Boolean,
        is_returning=pl.Boolean,
        traffic=pl.Boolean,
    )
    df = (
        df.select(list(dtypes))
        .filter((pl.col("status") == "IN_SERVICE") & (pl.col("is_installed")))
        .with_columns(
            [pl.col(col).cast(col_dtype) for col, col_dtype in dtypes.items()]
        )
        .with_columns(
            [
                pl.col("last_reported"),
                (pl.col("last_reported") * 1000)
                .cast(pl.Datetime)
                .dt.with_time_unit("ms")
                .dt.replace_time_zone("UTC")
                .dt.convert_time_zone(time_zone=tz)
                .alias("last_reported_timestamp"),
            ]
        )
        .with_columns(
            [
                pl.lit(tz).cast(pl.Utf8).alias("tz"),
                pl.col("last_reported_timestamp")
                .dt.year()
                .cast(pl.Int16)
                .alias("year"),
                pl.col("last_reported_timestamp")
                .dt.month()
                .cast(pl.Int8)
                .alias("month"),
                pl.col("last_reported_timestamp")
                .dt.week()
                .cast(pl.Int8)
                .alias("week"),
                pl.col("last_reported_timestamp")
                .dt.weekday()
                .cast(pl.Int8)
                .alias("weekday"),
                pl.col("last_reported_timestamp")
                .dt.hour()
                .cast(pl.Int8)
                .alias("hour"),
                pl.lit(country_code).cast(pl.Utf8).alias("country_code"),
                pl.lit(system_name).cast(pl.Utf8).alias("system_name"),
                pl.lit(location).cast(pl.Utf8).alias("location"),
                pl.lit(auto_disc_url)
                .cast(pl.Utf8)
                .alias("auto_discoverable_url"),
                pl.lit(status_url).cast(pl.Utf8).alias("station_status_url"),
            ]
        )
    )
    return df


@task(
    name="transform",
    description="Transform and Merge station status and info data",
    tags=["transform"],
    log_prints=True,
)
def transform(
    df_info: pl.DataFrame,
    df_status: pl.DataFrame,
    auto_disc_url: str,
    status_url: str,
    tz: str = "America/Toronto",
    country_code: str = "Canada",
    system_name: str = "Bike Share Toronto",
    location: str = "Toronto",
) -> pl.DataFrame:
    """."""
    df_info = df_info.pipe(transform_info_data)
    df_status = df_status.pipe(
        transform_station_status_data,
        auto_disc_url,
        status_url,
        tz,
        country_code,
        system_name,
        location,
    )
    df = df_status.join(df_info, on=["station_id"], how="left")

    nrows, ncols = df.shape
    msg = (
        f"Transformed raw data for {system_name} ({country_code} - "
        f"{location}) giving {nrows:,} rows and {ncols:,} columns"
    )
    print(msg)
    return df


@task(
    name="load",
    description="Load processed data into specified sub-folder of S3 bucket.",
    tags=["load"],
    log_prints=True,
)
def load(
    df: pl.DataFrame, system_name: str, s3_bucket_name: str, tz: str
) -> None:
    """."""
    aws_s3_credentials = dict(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
        profile_name="nonelevenx",
    )
    curr_timestamp = (
        datetime.now(pytz.timezone(tz))
        .strftime("%Y-%m-%d_%H:%M:%S")
        .replace("-", "")
        .replace(":", "")
    )
    month_str = str(date.today().month).zfill(2)
    month_name_str = month_name[1:][int(month_str) - 1]
    proc_data_fpath = os.path.join(
        "status",
        system_name.replace(" ", "_").lower(),
        f"{date.today().year}",
        f"{month_str}-{month_name_str}",
        "raw",
        f"proc_data_status_{curr_timestamp}.parquet.gzip",
    )

    out_buffer = io.BytesIO()
    df.write_parquet(out_buffer, compression="gzip", use_pyarrow=True)

    session = boto3.Session(**aws_s3_credentials)
    s3_client = session.client("s3")
    response = s3_client.put_object(
        Bucket=s3_bucket_name,
        Key=proc_data_fpath,
        Body=out_buffer.getvalue(),
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    print(
        f"Uploaded {len(df):,} rows of station status data to "
        f"{s3_bucket_name} bucket at {proc_data_fpath}"
    )


@flow(
    name="status-flow",
    description="Query GBFS API endpoints for bikeshare station status",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def run_station_status_etl_workflow(
    system_auto_disc_url: str,
    tz: str = "America/Toronto",
    country_code: str = "Canada",
    system_name: str = "Bike Share Toronto",
    location: str = "Toronto",
    s3_bucket_name: str = "bucket",
) -> None:
    """Run ETL workflow to retrieve station status data."""
    station_info_url, status_url = get_api_urls(system_auto_disc_url)
    df_info, df_status = extract(station_info_url, status_url)
    df = transform(
        df_info,
        df_status,
        system_auto_disc_url,
        status_url,
        tz,
        country_code,
        system_name,
        location,
    )
    load(df, system_name, s3_bucket_name, tz)


if __name__ == "__main__":
    bikeshare_auto_disc_url = (
        "https://tor.publicbikesystem.net/customer/gbfs/v2/gbfs.json"
    )
    aws_s3_bucket_name = "bikeshare-toronto-dash"

    load_dotenv(find_dotenv())

    run_station_status_etl_workflow(
        bikeshare_auto_disc_url,
        tz="America/Toronto",
        country_code="Canada",
        system_name="Bike Share Toronto",
        location="Toronto",
        s3_bucket_name=aws_s3_bucket_name,
    )
