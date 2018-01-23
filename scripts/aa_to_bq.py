#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""Export data from Adobe Analytics to Google BigQuery.


"""

from datetime import datetime
from docopt import docopt
import os
import sys

try:
    import export_from_adobe_analytics_to_bigquery
except ModuleNotFoundError:
    sys.path.append(os.path.abspath(os.path.dirname(__file__)) + '/../aa_to_bq')
    import export_from_adobe_analytics_to_bigquery

if __name__ == '__main__':
    print('%s %s start.' % (datetime.today(), __file__))

    bq_project_id = 'excellent-guard-91807'
    bq_dataset_id = 'aa_datafeed'
    bq_table_id = 'test_table'
    aa_datafeed_dir_path = '/Users/hj1840/Desktop/kddimstall_2017-12-11/'
    aa_datafeed_header_file_name = 'column_headers.tsv'
    aa_datafeed_body_file_name = 'hit_data_100.tsv'
    aa_datafeed_output_file_path = '/Users/hj1840/program_files/python/aa_to_bq/outputs/tmp'

    # create model
    efaatbq = export_from_adobe_analytics_to_bigquery.ExportFromAdobeAnalyticsToBigQuery(
        bq_project_id,
        bq_dataset_id,
    )

    # check bq table
    if efaatbq.check_bq_table(bq_table_id) is False:
        # set datafeed files
        aa_datafeed_header_file_path = os.path.join(
            aa_datafeed_dir_path,
            aa_datafeed_header_file_name,
        )
        aa_datafeed_body_file_path = os.path.join(
            aa_datafeed_dir_path,
            aa_datafeed_body_file_name,
        )

        # make datafeed csv
        efaatbq.make_datafeed_csv(
            aa_datafeed_header_file_path,
            aa_datafeed_body_file_path,
            aa_datafeed_output_file_path,
        )

        # make bq empty table
        efaatbq.make_bq_empty_table(
            aa_datafeed_header_file_path,
            bq_table_id,
        )

        # load data into bq
        efaatbq.load_data_into_bq(
            aa_datafeed_output_file_path,
            bq_table_id,
        )


    print('%s %s end.' % (datetime.today(), __file__))
    sys.exit(0)



