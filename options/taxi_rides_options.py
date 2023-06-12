#  Copyright 2023 Google LLC
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from apache_beam.options.pipeline_options import PipelineOptions


class TaxiRidesOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--taxirides_subscription',
            help='Input Pub/Sub subscription to read taxirides events')
        parser.add_argument(
            '--bigquery_stat_events_table',
            help='Output BQ table to write taxirides stat events specified as:'
                 'PROJECT:DATASET.TABLE')
        parser.add_argument(
            "--raw_events_flag",
            default=False,
            type=bool,
            help="Raw Events Flag: true or false; default is False")
        parser.add_argument(
            '--bigquery_raw_events_table',
            help='Output BQ table to write taxirides raw events specified as:'
                 'PROJECT:DATASET.TABLE')
