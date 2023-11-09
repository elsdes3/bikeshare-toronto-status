#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Define Prefect Deployment for Flow."""

# pylint: disable=invalid-name

from etl_station_flow import run_station_status_etl_workflow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule

if __name__ == "__main__":
    # flow
    system_auto_disc_url = (
        "https://tor.publicbikesystem.net/customer/gbfs/v2/gbfs.json"
    )
    s3_bucket_name = "bikeshare-toronto-dash"

    # deployment
    deployment_name = "get-status-deployment"
    deployment_description = (
        "Get station status and metadata at scheduled intervals"
    )
    deployment_tags = ["station-status", "bikeshare-toronto"]
    schedule_interval = 1_200
    schedule_tz = "America/Toronto"
    queue_name = "get-status"
    deployment_vesion = 1

    infra_overrides_dict = dict(env=dict(DEMO_ENV_VAR="demo_value"))

    deployment_schedule = IntervalSchedule(
        interval=schedule_interval, timezone=schedule_tz
    )

    flow_params_dict = {
        "system_auto_disc_url": system_auto_disc_url,
        "tz": "America/Toronto",
        "country_code": "Canada",
        "system_name": "Bike Share Toronto",
        "location": "Toronto",
        "s3_bucket_name": s3_bucket_name,
    }

    deployment = Deployment.build_from_flow(
        flow=run_station_status_etl_workflow,
        name=deployment_name,
        description=deployment_description,
        parameters=flow_params_dict,
        schedule=deployment_schedule,
        work_queue_name=queue_name,
        tags=deployment_tags,
        infra_overrides=infra_overrides_dict,
        skip_upload=False,
        ignore_file=".prefectignore",
        version=deployment_vesion,
        enforce_parameter_schema=True,
    )
    deployment.apply()
