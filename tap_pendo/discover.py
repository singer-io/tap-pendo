import os

import singer
from singer import metadata

from tap_pendo.streams import STREAMS

LOGGER = singer.get_logger()

metadata_fields = {
    "type": {
        "type": ["null", "string"]
    },
    "display_name": {
        "type": ["null", "string"]
    },
    "element_type": {
        "type": ["null", "string"]
    },
    "element_format": {
        "type": ["null", "string"]
    },
    "dirty": {
        "type": ["null", "boolean"]
    },
    "is_hidden": {
        "type": ["null", "boolean"]
    },
    "is_deleted": {
        "type": ["null", "boolean"]
    },
    "is_calculated": {
        "type": ["null", "boolean"]
    },
    "is_per_app": {
        "type": ["null", "boolean"]
    },
    "never_index": {
        "type": ["null", "boolean"]
    }
}


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_custom_fields(instance):
    return instance.get_fields().get('custom')


def get_schema_propery_type(schema_type):
    if schema_type == 'string':
        return {"type": ["null", "string"]}
    else:
        return None


def discover_streams(config):
    streams = []

    LOGGER.info("Discovering custom fields for Accounts")
    custom_account_fields = STREAMS['metadata_accounts'](
        config).get_fields().get('custom')

    LOGGER.info("Discovering custom fields for Visitors")
    custom_visitor_fields = STREAMS['metadata_visitors'](
        config).get_fields().get('custom')

    for s in STREAMS.values():

        LOGGER.info("Disco vering stream %s", s.name)
        s = s(config)

        schema = s.load_schema()
        mdata = metadata.to_map(s.load_metadata())

        if s.name == 'accounts':
            LOGGER.info("Discovering custom fields for Accounts")
            custom_account_fields = {}
            instance = STREAMS['metadata_accounts'](config)

            custom_account_fields = get_custom_fields(instance)
            for key, value in custom_account_fields.items():
                if 'metadata_custom' not in schema['properties']:
                    schema['properties']['metadata_custom'] = {}
                schema['properties']['metadata_custom']['properties'] = {
                    key: get_schema_propery_type(value.get('type'))
                }
                mdata = metadata.write(mdata,
                                       ("properties", 'metadata_custom'),
                                       'inclusion', 'available')

        if s.name == 'visitors':
            for key, value in custom_visitor_fields.items():
                if 'metadata_custom' not in schema['properties']:
                    schema['properties']['metadata_custom'] = {}
                schema['properties']['metadata_custom'] = {
                    key: get_schema_propery_type(value.get('type'))
                }
                mdata = metadata.write(mdata,
                                       ("properties", 'metadata_custom'),
                                       'inclusion', 'available')

        if s.name == 'metadata_accounts':
            for key, value in custom_account_fields.items():
                if 'custom' not in schema['properties']:
                    schema['properties']['custom'] = {}
                schema['properties']['custom']['properties'] = {}
                schema['properties']['custom']['properties'][key] = {}
                schema['properties']['custom']['properties'][key]['properties'] = {}
                schema['properties']['custom']['properties'][key]['type'] = ["null", "object"]
                schema['properties']['custom']['properties'][key]['additional_properties'] = "false"
                schema['properties']['custom']['properties'][key]['properties'] = metadata_fields
                mdata = metadata.write(mdata, ("properties", "custom"),
                                       'inclusion', 'available')
        if s.name == 'metadata_visitors':
            for key, value in custom_visitor_fields.items():
                if 'custom' not in schema['properties']:
                    schema['properties']['custom'] = {}
                schema['properties']['custom']['properties'] = {}
                schema['properties']['custom']['properties'][key] = {}
                schema['properties']['custom']['properties'][key]['properties'] = {}
                schema['properties']['custom']['properties'][key]['type'] = ["null", "object"]
                schema['properties']['custom']['properties'][key]['additional_properties'] = "false"
                schema['properties']['custom']['properties'][key]['properties'] = metadata_fields
                mdata = metadata.write(mdata, ("properties", 'custom'),
                                       'inclusion', 'available')

        stream = {
            'stream': s.name,
            'tap_stream_id': s.name,
            'schema': schema,
            'metadata': metadata.to_list(mdata)
        }

        streams.append(stream)

    return streams
