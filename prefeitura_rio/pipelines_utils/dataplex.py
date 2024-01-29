# -*- coding: utf-8 -*-

import json
import os
import re
import base64
from pathlib import Path
from typing import Union, List
from time import sleep
import yaml

import google.api_core.exceptions

try:
    from google.cloud.dataplex_v1 import (
        DataScanJob,
        DataScan,
        DataProfileSpec,
        DataQualitySpec,
        DataQualityRule,
        CreateDataScanRequest,
        GetDataScanJobRequest,
        UpdateDataScanRequest,
        DataScanServiceClient,
        GetDataScanRequest,
        RunDataScanRequest,
    )
    from google.oauth2 import service_account

except ImportError:
    from prefeitura_rio.utils import base_assert_dependencies
    
    base_assert_dependencies(['google.cloud.dataplex_v1'], extras=['pipelines'])

class Dataplex:
    """
    Base class for interacting with Google Dataplex API
    """

    def __init__(
        self,
        data_scan_id: str,
        credentials_path: str = None,
        project_id: str = None,
        location: str = "us-central1",
    ):
        self.credentials_path = credentials_path
        self.credentials = self._load_credentials()
        self.location = location
        self.project_id = project_id or self.credentials._project_id
        self.id = data_scan_id
        self.client = DataScanServiceClient(credentials=self.credentials)
        self.scan = self._get_scan()
        if self.scan:
            self.spec = self.scan.data_quality_spec
        else:
            self.spec = None

    @staticmethod
    def to_snake_case(name: str):
        """
        Converts camelCase to snake_case
        """
        name = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
        return name

    @staticmethod
    def snake_case_rule(rule):
        """
        Converts all keys in a dictionary from camelCase to snake_case
        """
        sc_rule = {Dataplex.to_snake_case(key): rule[key] for key in rule}
        return sc_rule

    @staticmethod
    def _decode_env(env: str) -> str:
        """
        Decode environment variable
        Adapted from basedosdados.upload.base.Base
        """
        return base64.b64decode(os.getenv(env).encode("utf-8")).decode("utf-8")

    def _load_credentials(self):
        """
        Load credentials from env or filepath

        Returns:
            _type_: _description_
        """
        json_acct_info = None
        if self.credentials_path:
            with open(self.credentials_path) as fp:
                json_acct_info = json.load(fp, strict=False)
        elif os.getenv("BASEDOSDADOS_CREDENTIALS_PROD"):
            stream = self._decode_env("BASEDOSDADOS_CREDENTIALS_PROD")
            json_acct_info = json.loads(stream, strict=False)

        credentials = service_account.Credentials.from_service_account_info(
            json_acct_info
        )
        return credentials

    def _get_scan(self):
        """
        Fetches DataScan definitions

        Returns:
            google.cloud.dataplex_v1.DataScan
        """
        try:
            # Initialize request argument(s)
            request = GetDataScanRequest(
                name=f"projects/{self.project_id}/locations/{self.location}/dataScans/{self.id}",
                view=GetDataScanRequest.DataScanView.FULL.value,
            )
            # Make the request
            response = self.client.get_data_scan(request=request)
            return response
        except google.api_core.exceptions.NotFound as e:
            print(e)
            return None

    def _wait_for_job_completion(self, job_name: str):
        """
        _summary_

        Args:
            job_name (str): _description_

        Returns:
            _type_: _description_
        """
        # Initialize request argument(s)
        request = GetDataScanJobRequest(
            name=job_name, view=GetDataScanJobRequest.DataScanJobView.FULL.value
        )
        # Make the request
        job = self.client.get_data_scan_job(request=request)
        unfinished_states = [
            DataScanJob.State.PENDING.value,
            DataScanJob.State.RUNNING.value,
            DataScanJob.State.CANCELING.value,
            DataScanJob.State.STATE_UNSPECIFIED.value,
        ]
        while job.state.value in unfinished_states:
            sleep(1)
            job = self.client.get_data_scan_job(request=request)
        return job

    def run(self, wait_run_completion:bool=False):
        """
        _summary_

        Returns:
            _type_: _description_
        """
        # Initialize request argument(s)
        request = RunDataScanRequest(
            name=f"projects/{self.project_id}/locations/{self.location}/dataScans/{self.id}",
        )
        # Make the request
        response = self.client.run_data_scan(request=request)
        if wait_run_completion:
            job = self._wait_for_job_completion(job_name=response.job.name)
            return job
        return response


class DataQuality(Dataplex):
    """
    Class representing a Data Quality Scan resource
    """

    def __init__(
        self,
        data_scan_id: str,
        credentials_path: str = None,
        project_id: str = None,
        location: str = "us-central1",
    ):
        super().__init__(
            data_scan_id=data_scan_id,
            credentials_path=credentials_path,
            project_id=project_id,
            location=location,
        )

    def _create_default(self):
        # create default data quality scan based on profile
        pass

    def generate_rule_yaml(self, to_file: str = None, to_dir: str = None):
        if not to_file and not to_dir:
            raise ValueError("You must set one of `to_file` or `to_dir`")
        if not to_file and not Path(to_dir).is_dir():
            raise TypeError("`to_dir` must be a directory")
        rules = [
            DataQualityRule.to_dict(rule) for rule in self.scan.data_quality_spec.rules
        ]
        dump_data = {"rules": rules}
        if not to_file and to_dir:
            to_file = f"{to_dir}/{self.id}.yaml"
        with open(to_file, "w") as file:
            yaml.safe_dump(dump_data, file)
        return rules

    def _patch(self, row_filter: str):
        """
        Updates a data quality scan row_filter

        Args:
            row_filter (str): a valid SQL expression for a
            `WHERE` clause

        Returns:
            _type_: _description_
        """
        self.scan.data_quality_spec.row_filter = row_filter
        # self.scan.data_profile_spec.sampling_percent = sampling_percent
        update_mask = [
            "dataQualitySpec.rowFilter",
            # "dataQualitySpec.samplingPercent", #TODO: patching sampling percent goes to 0.0
        ]
        request = UpdateDataScanRequest(
            data_scan=self.scan, update_mask=",".join(update_mask)
        )
        # Make the request
        operation = self.client.update_data_scan(request=request)

        response = operation.result()
        # remove row_filter after running
        return response

    def create(
        self,
        dataset_id: str,
        table_id: str,
        rules: List[dict],
        export_table_id: str = None,
        export_dataset_id: str = "bq_logs",
        row_filter: str = None,
        incremental_field: str = None,
    ):
        """
        Create a Data Quality Scan Resource.
        Args:
            dataset_id (str): dataset_id for the table being scanned
            table_id (str): table_id for the table being scanned
            rules (List[dict]): set of rules to check against table data.
                Should be a list of dicts with the rules formatted as  per
                `https://cloud.google.com/dataplex/docs/use-auto-data-quality#create-scan-using-gcloud`
            export_table_id (str, optional): table_id which to export scan results to.
                Defaults to None.
            export_dataset_id (str, optional): dataset_id which to export scan results to.
                Defaults to "bq_logs".
            row_filter (str, optional): run scan only on filtered data.
                Should be a valid SQL expression for a `WHERE` clause.
                Defaults to None.
            incremental_field (str, optional): if set, will define the data scan scope as
                incremental. Should be a monotonically increasing field on the source table.
                Defaults to None.

        Returns:
            _type_: _description_
        """
        base_url = f"//bigquery.googleapis.com/projects/{self.project_id}/datasets"
        data_quality_scan = DataScan()
        data_quality_scan.data_quality_spec = DataQualitySpec()
        data_quality_scan.data.resource = f"{base_url}/{dataset_id}/tables/{table_id}"
        # TODO: check rules syntax
        # TODO: add loading rules from file (yaml/json)
        # May pass rules as dict or filepath
        data_quality_scan.data_quality_spec.rules = self._get_rules(rules)

        if export_table_id and export_dataset_id:
            table_str = f"{base_url}/{export_dataset_id}/tables/{export_table_id}"
            data_quality_scan.data_quality_spec.post_scan_actions.bigquery_export.results_table = (
                table_str
            )

        if row_filter:
            data_quality_scan.data_quality_spec.row_filter = row_filter

        if incremental_field:
            data_quality_scan.execution_spec.field = incremental_field

        request = CreateDataScanRequest(
            parent=f"projects/{self.project_id}/locations/{self.location}",
            data_scan=data_quality_scan,
            data_scan_id=self.id,
        )

        response = self.client.create_data_scan(request=request)

        return response

    def run_parameterized(
        self, row_filters: Union[list, str], wait_run_completion: bool = False
    ):
        """
        Run a data quality scan with row filters instead of the entire table data

        Args:
            row_filters (Union[list, str]): _description_
            wait_run_completion (bool, optional): _description_. Defaults to False.

        Returns:
            _type_: _description_
        """
        if isinstance(row_filters, str):
            row_filter = row_filters
        else:
            row_filter = " AND ".join(row_filters)

        self._patch(row_filter=row_filter)
        response = self.run()
        if wait_run_completion:
            job = self._wait_for_job_completion(job_name=response.job.name)
            return job
        self._patch(row_filter=None)
        return response

    def _normalize_rules(self, rules):
        snake_case_rules = [self.snake_case_rule(rule) for rule in rules]
        rules = [DataQualityRule(**rule) for rule in snake_case_rules]
        return rules

    def _get_rules(self, rules: Union[list, str] = None):
        if rules is None:
            return
        if isinstance(rules, list):
            read_rules = rules
        elif isinstance(rules, str):
            with open(rules, "r") as fp:
                read_rules = yaml.safe_load(fp)
                if isinstance(read_rules, str):
                    read_rules = json.load(fp, strict=False)
                read_rules = read_rules["rules"]

        return self._normalize_rules(read_rules)


class DataProfile(Dataplex):
    """
    Class representing a Data Profiling resource
    """

    def __init__(
        self,
        data_scan_id: str,
        credentials_path: str = None,
        project_id: str = None,
        location: str = "us-central1",
    ):
        super().__init__(
            data_scan_id=data_scan_id,
            credentials_path=credentials_path,
            project_id=project_id,
            location=location,
        )

    def create(
        self,
        dataset_id: str,
        table_id: str,
        export_table_id: str = None,
        export_dataset_id: str = "bq_logs",
        exclude_columns: list = None,
        row_filter: str = None,
        incremental_field: str = None,
    ):
        """
        Create a Data Quality Scan Resource.



        Args:
            dataset_id (str): dataset_id for the table being scanned
            table_id (str): table_id for the table being scanned
            rules (List[dict]): set of rules to check against table data.
                Should be a list of dicts with the rules formatted as  per
                `https://cloud.google.com/dataplex/docs/use-auto-data-quality#create-scan-using-gcloud`
            export_table_id (str, optional): table_id which to export scan results to.
                Defaults to None.
            export_dataset_id (str, optional): dataset_id which to export scan results to.
                Defaults to "bq_logs".
            row_filter (str, optional): run scan only on filtered data.
                Should be a valid SQL expression for a `WHERE` clause.
                Defaults to None.
            incremental_field (str, optional): if set, will define the data scan scope as
                incremental. Should be a monotonically increasing field on the source table.
                Defaults to None.

        Returns:
            _type_: _description_
        """
        base_url = f"//bigquery.googleapis.com/projects/{self.project_id}/datasets"
        data_profile = DataScan()
        data_profile.data_profile_spec = DataProfileSpec()
        data_profile.data.resource = f"{base_url}/{dataset_id}/tables/{table_id}"

        if exclude_columns:
            data_profile.data_profile_spec.exclude_fields = (
                DataProfileSpec.SelectedFields(field_names=exclude_columns)
            )

        if export_table_id and export_dataset_id:
            table_str = f"{base_url}/{export_dataset_id}/tables/{export_table_id}"
            data_profile.data_profile_spec.post_scan_actions.bigquery_export.results_table = (
                table_str
            )
        if row_filter:
            data_profile.data_profile_spec.row_filter = row_filter

        if incremental_field:
            data_profile.execution_spec.field = incremental_field

        request = CreateDataScanRequest(
            parent=f"projects/{self.project_id}/locations/{self.location}",
            data_scan=data_profile,
            data_scan_id=self.id,
        )

        response = self.client.create_data_scan(request=request)

        return response
