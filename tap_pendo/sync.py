import json

import humps
import singer
import singer.metrics as metrics
from singer import Transformer, metadata
from singer.transform import strptime_to_utc
from singer.utils import strftime

LOGGER = singer.get_logger()


def sync_stream(state, start_date, instance):
    stream = instance.stream

    bookmark_date = instance.get_bookmark(state, instance.name, start_date,
                                          instance.replication_key)
    bookmark_dttm = strptime_to_utc(bookmark_date)
    new_bookmark = bookmark_dttm

    with metrics.record_counter(stream.tap_stream_id) as counter, Transformer(
            integer_datetime_fmt="unix-milliseconds-integer-datetime-parsing"
    ) as transformer:
        (stream, records) = instance.sync(state)
        for record in records:
            schema_dict = stream.schema.to_dict()
            stream_metadata = metadata.to_map(stream.metadata)

            transformed_record = instance.transform(record)

            try:
                transformed_record = transformer.transform(
                    transformed_record, schema_dict, stream_metadata)
            except Exception as err:
                LOGGER.error('Error: %s', err)
                LOGGER.error(' for schema: %s',
                             json.dumps(schema_dict, sort_keys=True, indent=2))
                LOGGER.error('Transform failed for %s', record)
                raise err

            record_timestamp = strptime_to_utc(
                transformed_record.get(
                    humps.decamelize(instance.replication_key)))
            new_bookmark = max(new_bookmark, record_timestamp)

            if record_timestamp > bookmark_dttm:
                singer.write_record(stream.tap_stream_id, transformed_record)
                counter.increment()
            else:
                singer.write_record(stream.tap_stream_id, transformed_record)
                counter.increment()

        instance.update_bookmark(state, instance.name, strftime(new_bookmark),
                                 instance.replication_key)
        singer.write_state(state)

        return counter.value


def sync_full_table(state, instance):
    stream = instance.stream

    with metrics.record_counter(stream.tap_stream_id) as counter, Transformer(
            integer_datetime_fmt="unix-milliseconds-integer-datetime-parsing"
    ) as transformer:
        (stream, records) = instance.sync(state)
        for record in records:
            schema_dict = stream.schema.to_dict()
            stream_metadata = metadata.to_map(stream.metadata)

            transformed_record = instance.transform(record)

            try:
                transformed_record = transformer.transform(
                    transformed_record, schema_dict, stream_metadata)
            except Exception as err:
                LOGGER.error('Error: %s', err)
                LOGGER.error(' for schema: %s',
                             json.dumps(schema_dict, sort_keys=True, indent=2))
                LOGGER.error('Transform failed for %s', record)
                raise err

            singer.write_record(stream.tap_stream_id, transformed_record)
            counter.increment()
    return counter.value
