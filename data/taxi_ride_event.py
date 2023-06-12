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

import typing
import datetime
from utils.util import Util
import apache_beam as beam


class TaxiRideEvent(typing.NamedTuple):
    ride_id: str
    point_idx: int
    latitude: float
    longitude: float
    timestamp: datetime.datetime
    meter_reading: float
    meter_increment: float
    ride_status: str
    passenger_count: int

    @staticmethod
    def convert_json_to_taxi_ride_event_object(input_json):
        dt = Util.parse_datetime(input_json['timestamp']).astimezone(datetime.timezone.utc)
        event = TaxiRideEvent(
            ride_id=input_json['ride_id'],
            point_idx=input_json['point_idx'],
            latitude=input_json['latitude'],
            longitude=input_json['longitude'],
            timestamp=dt,
            meter_reading=input_json['meter_reading'],
            meter_increment=input_json['meter_increment'],
            ride_status=input_json['ride_status'],
            passenger_count=input_json['passenger_count'])
        return beam.window.TimestampedValue(event, dt.timestamp())

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, TaxiRideEvent):
            return NotImplemented

        return self.timestamp < other.timestamp
