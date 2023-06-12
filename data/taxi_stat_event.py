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
from enum import Enum


class TaxiStatEvent(typing.NamedTuple):
    ride_id: str
    session_id: str
    total_meter_reading: float
    passenger_count: int
    journey_length_seconds: float
    number_of_ride_events: int
    session_reason: str
    session_start_time: datetime.datetime
    session_end_time: datetime.datetime

    class Reason(str, Enum):
        GARBAGE_COLLECTION = 'GARBAGE_COLLECTION'  # No drop-off message was seen, or new messages arrived after the
        # session was emitted
        DROPOFF = 'DROPOFF'  # Generated when a drop-off message is seen, without waiting
