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

from typing import Tuple
import apache_beam as beam
import logging
import json
from apache_beam.options.pipeline_options import StandardOptions
from data.taxi_stat_event import TaxiStatEvent
from dofns.generate_sessions_dofn import GenerateSessionsDoFn
from options.taxi_rides_options import TaxiRidesOptions
from data.taxi_ride_event import TaxiRideEvent
from utils.util import Util


def main():

    options = TaxiRidesOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:

        taxi_rides = (
            p   | "Read TaxiRide events" >> beam.io.ReadFromPubSub(
                    subscription=options.taxirides_subscription)
                | "Read JSON" >> beam.Map(json.loads)
                | "TaxiRideEvent with EventTimestamp" >> beam.Map(TaxiRideEvent.convert_json_to_taxi_ride_event_object)
                                            .with_output_types(TaxiRideEvent)
        )

        # Create tuples of TaxiRide objects with ride_id as key
        taxi_rides_tuples =(
            taxi_rides | "Create Keys with ride_id" >> beam.WithKeys(lambda event: event.ride_id)
                                                        .with_output_types(Tuple[str, TaxiRideEvent])
        )

        # Generate Sessions with stateful DoFn
        taxi_stats = (
            taxi_rides_tuples | "Generate Sessions" >> beam.ParDo(GenerateSessionsDoFn())
                                                        .with_output_types(TaxiStatEvent)
                                                        .with_input_types(Tuple[str, TaxiRideEvent])
        )

        # Write Stat Events to BigQuery
        _ = (
            taxi_stats  | "Parse Stats to Row" >> beam.Map(Util.convert_object_to_bq_row)
                        | "Write StatEvents to BQ" >> beam.io.WriteToBigQuery(
                                table=options.bigquery_stat_events_table,
                                schema=Util.bq_stat_events_table_schema,
                                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                additional_bq_parameters=Util.stat_events_bq_parameters)
        )

        # Write Raw Events to BigQuery
        if options.raw_events_flag:
            (
                taxi_rides  | "Parse Raw to Row" >> beam.Map(Util.convert_object_to_bq_row)
                            | "Write RawEvents to BQ" >> beam.io.WriteToBigQuery(
                                table=options.bigquery_raw_events_table,
                                schema=Util.bq_raw_events_table_schema,
                                ignore_insert_ids=True,
                                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                additional_bq_parameters=Util.raw_events_bq_parameters)
            )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
