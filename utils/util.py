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

from datetime import datetime


class Util:

    @staticmethod
    def convert_object_to_bq_row(event) -> dict:
        return event._asdict()

    @staticmethod
    def parse_datetime(input_timestamp: str) -> datetime:

        formats = ["%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"]
        for fmt in formats:
            try:
                dt = datetime.strptime(input_timestamp, fmt)
                return dt
            except ValueError:
                pass
        raise ValueError("Invalid datetime format!")

    bq_raw_events_table_schema = {
        'fields': [
            {'name': 'ride_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'point_idx', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'latitude', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'longitude', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'meter_reading', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'meter_increment', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'ride_status', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'passenger_count', 'type': 'INTEGER', 'mode': 'NULLABLE'}
        ]
    }

    raw_events_bq_parameters = {
        'timePartitioning': {'type': 'DAY', 'field': 'timestamp'},
    }

    bq_stat_events_table_schema = {
        'fields': [
            {'name': 'ride_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'session_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'total_meter_reading', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'passenger_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'journey_length_seconds', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'number_of_ride_events', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'session_reason', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'session_start_time', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'session_end_time', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
        ]
    }

    stat_events_bq_parameters = {
        'timePartitioning': {'type': 'DAY', 'field': 'session_end_time'},
    }
