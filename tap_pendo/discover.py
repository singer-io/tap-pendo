import os

from tap_pendo.streams import STREAMS


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def discover_streams(config):
    streams = []

    for s in STREAMS.values():
        s = s(config)
        streams.append({
            'stream': s.name,
            'tap_stream_id': s.name,
            'schema': s.load_schema(),
            'metadata': s.load_metadata()
        })
    return streams
