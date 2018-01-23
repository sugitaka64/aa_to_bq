#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""Export data from Adobe Analytics to Google BigQuery."""

from google.cloud import bigquery
import pandas as pd
import re

class ExportFromAdobeAnalyticsToBigQuery(object):
    """Export data from Adobe Analytics to Google BigQuery."""

    def __init__(
        self,
        project_id: str,
        dataset_id: str,
    ):
        """init."""
        self.bigquery_client = bigquery.Client(project=project_id)
        self.dataset = self.bigquery_client.dataset(dataset_id)

    def check_table(
        self,
        table_id: str,
    ) -> bool:
        """check table."""
        table_list = self.bigquery_client.list_dataset_tables(self.dataset)
        for existed_table in table_list:
            if existed_table.table_id == table_id:
                # table exists
                return True

        return False

    def make_table(
        self,
        table_id: str,
        aa_datafeed_header_file_path: str,
        aa_datafeed_body_file_path: str,
    ) -> bool:
        schema = []
        table = self.dataset.table(table_id)

        header_df = pd.read_csv(
            aa_datafeed_header_file_path,
            delimiter='\t',
            header=None
        )
        column_names = header_df.iloc[0]

        body_df = pd.read_csv(
            aa_datafeed_body_file_path,
            delimiter='\t',
            header=None
        )


        """
        for column_name in column_names:
            if re.search('\(deprecated\)$', column_name):
                continue

            schema.append(bigquery.SchemaField(column_name, 'STRING', mode='nullable'))

        table_ref = bigquery.Table(table, schema=schema)
        self.bigquery_client.create_table(table_ref)
        """

        return True

    def add_schema_field(self):
        pass

    def combine_column_heasers_and_hit_data(self):
        pass
