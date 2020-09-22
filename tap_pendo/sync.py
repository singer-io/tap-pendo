import json

import humps
import singer
import singer.metrics as metrics
from singer import Transformer, metadata

LOGGER = singer.get_logger()


def sync_stream(state, start_date, instance):
    stream = instance.stream

    # If we have a bookmark, use it; otherwise use start_date
    if (instance.replication_method == 'INCREMENTAL'
            and not state.get('bookmarks', {}).get(
                stream.tap_stream_id, {}).get(instance.replication_key)):
        singer.write_bookmark(state, stream.tap_stream_id,
                              instance.replication_key, start_date)

    parent_stream = stream
    with metrics.record_counter(
            stream.tap_stream_id) as counter, Transformer() as transformer:
        for (stream, record) in instance.sync(state):
            # NB: Only count parent records in the case of sub-streams
            if stream.tap_stream_id == parent_stream.tap_stream_id:
                counter.increment()

            schema_dict = stream.schema.to_dict()
            stream_metadata = metadata.to_map(stream.metadata)

            transformed_record = humps.decamelize(record)
            try:
                transformed_record = transformer.transform(
                    transformed_record, schema_dict, stream_metadata)
            except Exception as err:
                LOGGER.error('Error: %s', err)
                LOGGER.error(' for schema: %s',
                             json.dumps(schema_dict, sort_keys=True, indent=2))
                raise err

            singer.write_record(stream.tap_stream_id, transformed_record)
            # NB: We will only write state at the end of a stream's sync:
            #  We may find out that there exists a sync that takes too long and can never emit a bookmark
            #  but we don't know if we can guarentee the order of emitted records.

        if instance.replication_method == "INCREMENTAL":
            singer.write_state(state)

        return counter.value
