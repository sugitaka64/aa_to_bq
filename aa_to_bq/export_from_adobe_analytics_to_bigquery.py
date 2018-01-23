#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""Export data from Adobe Analytics to Google BigQuery."""

from google.cloud import bigquery
import pandas as pd
import re
from datetime import datetime
import os
from pyspark.sql.functions import udf
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
import sys

class ExportFromAdobeAnalyticsToBigQuery(object):
    """Export data from Adobe Analytics to Google BigQuery."""

    def __init__(
        self,
        bq_project_id: str,
        bq_dataset_id: str,
    ):
        """init."""
        self.bigquery_client = bigquery.Client(project=bq_project_id)
        self.bq_dataset = self.bigquery_client.dataset(bq_dataset_id)
        spark = SparkSession\
            .builder\
            .appName('export_from_adobe_analytics_to_bigquery')\
            .config('spark.debug.maxToStringFields', 2000)\
            .getOrCreate()
        self.sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)
        self.pyspark_schema = []
        self.bq_schema = []

    def __make_schema(
        self,
        aa_datafeed_header_file_path: str,
    ) -> bool:
        """make schema."""
        header_df = pd.read_csv(
            aa_datafeed_header_file_path,
            delimiter='\t',
            header=None
        )
        column_names = header_df.iloc[0]

        for column_name in column_names:
            column_name = column_name.replace(' (deprecated)', '_deprecated')
            self.pyspark_schema.append(StructField(column_name, StringType(), True))
            self.bq_schema.append(bigquery.SchemaField(column_name, 'STRING', mode='nullable'))

        return True

    def check_bq_table(
        self,
        bq_table_id: str,
    ) -> bool:
        """check table."""
        table_list = self.bigquery_client.list_dataset_tables(self.bq_dataset)
        for existed_table in table_list:
            if existed_table.table_id == bq_table_id:
                # table exists
                return True

        return False

    def make_datafeed_csv(
        self,
        aa_datafeed_header_file_path: str,
        aa_datafeed_body_file_path: str,
        aa_datafeed_output_file_path: str,
    ) -> bool:
        """make datafeed csv."""
        if len(self.pyspark_schema) == 0:
            self.__make_schema(aa_datafeed_header_file_path)

        pyspark_schema = StructType(self.pyspark_schema)

        body_df = self.sqlContext\
            .read.format('csv')\
            .options(header='false')\
            .option('delimiter', '\t')\
            .load(aa_datafeed_body_file_path, schema=pyspark_schema)

        body_df.write\
            .format('csv')\
            .mode('overwrite')\
            .options(header='true')\
            .save(aa_datafeed_output_file_path)

        return True

    def make_bq_empty_table(
        self,
        aa_datafeed_header_file_path: str,
        bq_table_id: str,
    ) -> bool:
        """make bq empty table."""
        if len(self.bq_schema) == 0:
            self.__make_schema(aa_datafeed_header_file_path)

        bq_table = self.bq_dataset.table(bq_table_id)
        bq_table_ref = bigquery.Table(bq_table, schema=self.bq_schema)
        self.bigquery_client.create_table(bq_table_ref)

        return True

    def load_data_into_bq(
        self,
        aa_datafeed_header_file_path: str,
        bq_table_id: str,
    )-> bool:
        """load data into bq."""
        csv_files = []
        for target in os.listdir(aa_datafeed_header_file_path):
            if re.search('^part-\d+.+\.csv$', target):
                target = os.path.join(
                    aa_datafeed_header_file_path,
                    target,
                )
                csv_files.append(target)

        # setting
        bq_table = self.bq_dataset.table(bq_table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = 'CSV'
        job_config.skip_leading_rows = 1
        job_config.allow_quoted_newlines = True

        for csv_file in csv_files:
            with open(csv_file, 'rb') as readable:
                self.bigquery_client.load_table_from_file(
                    readable,
                    bq_table,
                    job_config=job_config
                )

        return True
