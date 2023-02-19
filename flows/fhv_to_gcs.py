#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from pathlib import Path
from prefect_gcp.cloud_storage import GcsBucket
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(log_prints=True, tags=["extract"], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract(url) -> pd.DataFrame:
    """Read data from web to Dataframe"""
    data=pd.read_csv(url)

    #changed all column headers to lowercase
    data.columns=[i.lower() for i in data.columns]

    #fixed some dtype issues
    data['pickup_datetime']=pd.to_datetime(data['pickup_datetime'])
    data['dropoff_datetime']=pd.to_datetime(data['dropoff_datetime'])
    data['pulocationid']=pd.to_numeric(data['pulocationid'], downcast='float')
    data['dolocationid']=pd.to_numeric(data['dolocationid'], downcast='float')

    return data


@task()
def write_local(data: pd.DataFrame,  dataset_file: str) -> Path:
    """Write dataframe to local"""
    path=f"fhv-data-project/fhv_datasets/{dataset_file}.parquet"
    data.to_parquet(Path(path), compression="gzip")
    return path


@task()
def write_gcs(path) -> None:
    """"Uploading local file to GCS"""
    gcp_block = GcsBucket.load("de-zoom")
    gcp_block.upload_from_path(
        from_path=Path(path), to_path=path)
    return


@flow()
def etl_to_gcs(year:int, month: int) -> None:
    data_set=f"fhv_tripdata_{year}-{month:02}"
    url=f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{data_set}.csv.gz"
    data= extract(url)
    path=write_local(data, data_set)
    write_gcs(path)

@flow()
def main_flow(years: list, months: list):
    for year in years:
        for month in months:
            etl_to_gcs(year,month)
    
if __name__=='__main__':
    years= [2019]
    months= list(range(1,13))
    main_flow(years, months)