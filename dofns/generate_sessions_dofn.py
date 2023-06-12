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

import apache_beam as beam
import uuid

from apache_beam.coders import IterableCoder, FloatCoder

from typing import Tuple, Iterable
from apache_beam.metrics import Metrics
from apache_beam.metrics.metricbase import Distribution
from apache_beam.transforms.userstate import (CombiningValueStateSpec, BagStateSpec,
                                              TimerSpec, on_timer)
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam import coders
from apache_beam.utils.timestamp import Timestamp, Duration
from data.taxi_ride_event import TaxiRideEvent
from data.taxi_stat_event import TaxiStatEvent


class GenerateSessionsDoFn(beam.DoFn):
    _distribution: Distribution
    # Metric to count the number of ride events received
    ride_events_received = Metrics.counter('GenerateSessionsDoFn', 'ride_events_received')

    # Metric to count the number of ride events processed 
    ride_events_processed = Metrics.counter('GenerateSessionsDoFn', 'ride_events_processed')

    # Metric to count the number of sessions processed
    sessions_processed = Metrics.counter('GenerateSessionsDoFn', 'sessions_processed')

    # Elements bag of taxi ride events
    TAXI_RIDE_EVENTS_BAG = BagStateSpec('taxi_ride_events_bag', coders.registry.get_coder(TaxiRideEvent))

    # Event time timer for Garbage Collection
    GC_TIMER = TimerSpec('gc_timer', TimeDomain.WATERMARK)

    # The maximum element timestamp seen so far.
    MAX_TIMESTAMP = CombiningValueStateSpec('max_timestamp_seen',
                                            IterableCoder(FloatCoder()),
                                            lambda elements: max(elements, default=0))

    def __init__(self):
        self._distribution = Metrics.distribution('My sessions DoFn', 'vehicle_speed')

    def calculate_session(self, taxi_ride_events_bag,
                          session_reason: TaxiStatEvent.Reason) -> TaxiStatEvent:
        first_event = None
        last_event = None
        events_count = 0

        for taxi_ride_event in taxi_ride_events_bag.read():

            if first_event is None:
                first_event = taxi_ride_event[1]
            elif first_event.timestamp > taxi_ride_event[1].timestamp:
                first_event = taxi_ride_event[1]

            if last_event is None:
                last_event = taxi_ride_event[1]
            elif last_event.timestamp < taxi_ride_event[1].timestamp:
                last_event = taxi_ride_event[1]

            events_count += 1
            GenerateSessionsDoFn.ride_events_processed.inc()

        session_id = uuid.uuid4()

        start_time = first_event.timestamp
        end_time = last_event.timestamp

        journey_length_seconds = (end_time - start_time).total_seconds()
        distance = last_event.meter_reading  # Just an example, this could be actual distance
        session_speed = distance / journey_length_seconds
        self._distribution.update(session_speed)

        taxi_stat_event = TaxiStatEvent(
            ride_id=first_event.ride_id,
            session_id=str(session_id),
            total_meter_reading=last_event.meter_reading,
            passenger_count=last_event.passenger_count,
            journey_length_seconds=journey_length_seconds,
            number_of_ride_events=events_count,
            session_reason=session_reason,
            session_start_time=start_time,
            session_end_time=end_time
        )

        return taxi_stat_event

    def process(self,
                element: Tuple[str, TaxiRideEvent],
                element_timestamp: Timestamp = beam.DoFn.TimestampParam,
                taxi_ride_events_bag=beam.DoFn.StateParam(TAXI_RIDE_EVENTS_BAG),
                max_timestamp_seen=beam.DoFn.StateParam(MAX_TIMESTAMP),
                gc_timer=beam.DoFn.TimerParam(GC_TIMER)) -> Iterable[TaxiStatEvent]:

        taxi_ride_events_bag.add(element)
        max_timestamp_seen.add(element_timestamp.seconds())
        GenerateSessionsDoFn.ride_events_received.inc()

        if element[1].ride_status == "dropoff":
            taxi_stat_event = GenerateSessionsDoFn.calculate_session(taxi_ride_events_bag,
                                                                     TaxiStatEvent.Reason.DROPOFF)

            # Generate session and output TaxiStatEvent
            GenerateSessionsDoFn.sessions_processed.inc()
            yield beam.window.TimestampedValue(taxi_stat_event, Timestamp(max_timestamp_seen.read()))

            # Clear state for the key
            taxi_ride_events_bag.clear()
            max_timestamp_seen.clear()
            gc_timer.clear()
        else:
            # Set the timer to be 5 minutes to keep track of inactive keys
            expiration_time = Timestamp(max_timestamp_seen.read()) + Duration(seconds=5 * 60)
            gc_timer.set(expiration_time)

    @on_timer(GC_TIMER)
    def expiry_callback(self,
                        taxi_ride_events_bag=beam.DoFn.StateParam(TAXI_RIDE_EVENTS_BAG),
                        max_timestamp_seen=beam.DoFn.StateParam(MAX_TIMESTAMP)) -> Iterable[TaxiStatEvent]:

        # We have not seen the drop-off message 5 minutes after the max timestamp, so let's emit this session now
        taxi_stat_event = GenerateSessionsDoFn.calculate_session(taxi_ride_events_bag,
                                                                 TaxiStatEvent.Reason.GARBAGE_COLLECTION)

        # Generate session and output TaxiStatEvent
        GenerateSessionsDoFn.sessions_processed.inc()
        yield beam.window.TimestampedValue(taxi_stat_event, Timestamp(max_timestamp_seen.read()))

        # Clear state for the key
        taxi_ride_events_bag.clear()
        max_timestamp_seen.clear()
