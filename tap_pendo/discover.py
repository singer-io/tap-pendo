import os

import singer
from singer import metadata

from tap_pendo.streams import STREAMS, SUB_STREAMS, PendoForbiddenError

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
    # Return custom fields to add into a catalog for provided instance(stream)
    return instance.get_fields().get('custom')


def get_schema_property_type(schema_type):
    # Return dictionary of types to add into catalog based on provided data type
    if schema_type == 'string':
        return {"type": ["null", "string"]}
    elif schema_type == 'time':
        return {"type": ["null", "string"], "format": "date-time"}
    elif schema_type == 'boolean':
        return {"type": ["null", "boolean"]}
    elif schema_type == 'integer':
        return {"type": ["null", "integer", "number", "string"]}
    elif schema_type == 'float':
        return {"type": ["null", "number"]}
    elif schema_type == '':
        return {}

    raise Exception(f"No case matching JSON schema for property type: {schema_type}")


def build_metadata_metadata(mdata, schema, custom_fields):
    # Build metadata with custom fields for streams 'metadata_accounts' and 'metadata_visitors'
    if 'custom' not in schema['properties']:
        schema['properties']['custom'] = {}
        schema['properties']['custom']['type'] = ["null", "object"]
        schema['properties']['custom']['additional_properties'] = "false"
        schema['properties']['custom']['properties'] = {}
    for key, _ in custom_fields.items():
        schema['properties']['custom']['properties'][key] = {}
        schema['properties']['custom']['properties'][key]['type'] = [
            "null", "object"
        ]
        schema['properties']['custom']['properties'][key][
            'additional_properties'] = "false"
        schema['properties']['custom']['properties'][key][
            'properties'] = metadata_fields
        mdata = metadata.write(mdata, ("properties", 'custom'), 'inclusion',
                               'available')


def build_account_visitor_metadata(mdata, schema, custom_fields):
    # Build metadata with custom fields for streams 'accounts' and 'visitors'
    if 'metadata_custom' not in schema['properties']:
        schema['properties']['metadata_custom'] = {}
        schema['properties']['metadata_custom']['type'] = ["null", "object"]
        schema['properties']['metadata_custom'][
            'additional_properties'] = "false"
    for key, value in custom_fields.items():
        schema['properties']['metadata_custom']['type'] = ["null", "object"]
        schema['properties']['metadata_custom']['properties'] = {
            **schema['properties']['metadata_custom'].get('properties', {}),
            key: get_schema_property_type(value.get('type'))
        }
        mdata = metadata.write(mdata, ("properties", 'metadata_custom'),
                               'inclusion', 'available')


def _apply_access_checks(config):
    """
    Probe each parent stream for read access and return sets of accessible
    parent and child streams.
    Note: Child streams inherit parent accessibility. If a parent is not accessible,
    its child streams are also excluded.
    Raises PendoForbiddenError if no parent streams are accessible.
    """
    accessible_parents = set()
    inaccessible_parents = []

    child_streams = set(SUB_STREAMS.values())

    # Only check parent streams — child accessibility is derived from their parent
    for stream_name, stream_cls in STREAMS.items():
        if stream_name in child_streams:
            continue
        stream_obj = stream_cls(config)
        try:
            if stream_obj.check_access():
                accessible_parents.add(stream_name)
            else:
                inaccessible_parents.append(stream_name)
        except PendoForbiddenError as e:
            LOGGER.warning("Access check failed for stream '%s': %s", stream_name, e)
            inaccessible_parents.append(stream_name)

    if not accessible_parents:
        raise PendoForbiddenError(
            "HTTP-error-code: 403, Error: The credentials do not have 'read' access to any supported streams."
        )

    if inaccessible_parents:
        LOGGER.warning(
            "No 'read' access to stream(s): %s. Excluded from catalog.",
            ", ".join(inaccessible_parents),
        )

    # Build set of accessible child streams (children of accessible parents)
    accessible_children = set()
    for parent_stream, child_stream in SUB_STREAMS.items():
        if parent_stream in accessible_parents:
            accessible_children.add(child_stream)
        else:
            LOGGER.warning(
                "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                child_stream, parent_stream,
            )

    return accessible_parents, accessible_children


def discover_streams(config):
    # Discover schemas, build metadata for all the streams and return catalog
    # First, check access to all parent streams and get accessible streams
    accessible_parents, accessible_children = _apply_access_checks(config)

    streams = []

    LOGGER.info("Discovering custom fields for Accounts")
    custom_account_fields = STREAMS['metadata_accounts'](
        config).get_fields().get('custom') or {}

    LOGGER.info("Discovering custom fields for Visitors")
    custom_visitor_fields = STREAMS['metadata_visitors'](
        config).get_fields().get('custom') or {}

    for stream_name, stream_cls in STREAMS.items():
        is_accessible = (stream_name in accessible_parents) or (stream_name in accessible_children)

        if not is_accessible:
            LOGGER.info("Skipping stream '%s': not accessible", stream_name)
            continue

        s = stream_cls(config)

        schema = s.load_schema() # load schema for the stream
        mdata = metadata.to_map(s.load_metadata())

        # additionally, build metadata for custom fields for below streams
        if s.name == 'accounts':
            build_account_visitor_metadata(mdata, schema,
                                           custom_account_fields)

        if s.name == 'visitors':
            build_account_visitor_metadata(mdata, schema,
                                           custom_visitor_fields)

        if s.name == 'metadata_accounts':
            build_metadata_metadata(mdata, schema, custom_account_fields)

        if s.name == 'metadata_visitors':
            build_metadata_metadata(mdata, schema, custom_visitor_fields)

        stream_name = s.name
        parent = None
        if stream_name in SUB_STREAMS.values():
            for name, child in SUB_STREAMS.items():
                if child == stream_name:
                    parent = name
        if parent:
            mdata[()]['parent-tap-stream-id'] = parent

        stream = {
            'stream': s.name,
            'tap_stream_id': s.name,
            'schema': schema,
            'metadata': metadata.to_list(mdata)
        }

        streams.append(stream)

    return streams
