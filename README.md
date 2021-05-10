# tap-pendo

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Python 3.7](https://upload.wikimedia.org/wikipedia/commons/f/fc/Blue_Python_3.7_Shield_Badge.svg)](https://www.python.org/downloads/release/python-370/)


# tap-pendo

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [Pendo API](https://developers.pendo.io/docs/?bash#overview).
- Extracts the following resources:
  - Accounts
  - Features
  - Guides
  - Pages
  - Visitors
  - Visitor History
    - Syncs for this endpoint may be very long running if extracting anonymous visitors, see Visitors config `include_anonymous_visitors`.
  - Track Types
  - Feature Events
  - Events
  - Page Events
  - Guide Events
  - Poll Events
  - Track Events
  - Metadata Accounts
  - Metadata Visitors
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Streams

**[accounts](https://developers.pendo.io/docs/?bash#entities)**

- Endpoint: [https://api/v1/aggregation](https://app.pendo.io/api/v1/aggregation)
- Primary key fields: `account_id`
- Replication strategy: INCREMENTAL (query filtered)
  - Bookmark: `lastupdated`
- Transformations
  - Camel to snake case.
  - `metadata.auto.lastupdated` denested to root as `lastupdated`
  - `metadata` objects denested(`metadata_agent`, `metadata_audo`, `metadata_custom`, etc)

**[features](https://developers.pendo.io/docs/?bash#entities)**

- Endpoint: [https://api/v1/aggregation](https://app.pendo.io/api/v1/feature)
- Primary key fields: `id`
- Replication strategy: INCREMENTAL (query filtered)
  - Bookmark: `last_updated_at`
- Transformations: Camel to snake case.

**[guides](https://developers.pendo.io/docs/?bash#entities)**

- Endpoint: [https://api/v1/aggregation](https://app.pendo.io/api/v1/aggregation)
- Primary key fields: `id`
- Replication strategy: INCREMENTAL (query filtered)
  - Bookmark: `last_pdated_at`
- Transformations: Camel to snake case.

**[track types](https://developers.pendo.io/docs/?bash#entities)**

- Endpoint: [https://api/v1/aggregation](https://app.pendo.io/api/v1/aggregation)
- Primary key fields: `id`
- Replication strategy: INCREMENTAL (query filtered)
  - Bookmark: `last_pdated_at`
- Transformations: Camel to snake case.

**[visitors](https://developers.pendo.io/docs/?bash#entities)**

- Endpoint: [https://api/v1/aggregation](https://app.pendo.io/api/v1/aggregation)
- Primary key fields: `visitor_id`
- Replication strategy: INCREMENTAL (query filtered)
  - Bookmark: `lastupdated`
- Config option: `include_anonymous_visitors`: `true`/`false`
  - Default: `false` to eliminate anonymous visitors
  - https://developers.pendo.io/docs/?bash#source-specification
- Transformations
  - Camel to snake case.
  - `metadata.auto.lastupdated` denested to root as `lastupdated`
  - `metadata` objects denested(`metadata_agent`, `metadata_audo`, `metadata_custom`, etc)

**[visitor_history](https://developers.pendo.io/docs/?bash#get-an-account-by-id)**

- Endpoint: [https://api/v1/aggregation](https://app.pendo.io/api/v1/aggregation)
- Primary key fields: `visitor_id`
- Replication strategy: INCREMENTAL (query filtered)
  - Bookmark: `modified_ts` (Max from `ts` or `lastTs`)
- Transformations: Camel to snake case.

**[feature_events](https://developers.pendo.io/docs/?bash#get-an-account-by-id)**

- Endpoint: [https://api/v1/aggregation](https://app.pendo.io/api/v1/aggregation)
- Primary key fields: `visitor_id`, `account_id`, `server`, `remote_ip`
- Replication strategy: INCREMENTAL (query filtered)
  - Bookmark: `day` or `hour`
- Transformations: Camel to snake case.

**[events](https://developers.pendo.io/docs/?bash#get-an-account-by-id)**

- Endpoint: [https://api/v1/aggregation](https://app.pendo.io/api/v1/aggregation)
- Primary key fields: `visitor_id`, `account_id`, `server`, `remote_ip`
- Replication strategy: INCREMENTAL (query filtered)
  - Bookmark: `day` or `hour`
- Transformations: Camel to snake case.

**[page_events](https://developers.pendo.io/docs/?bash#get-an-account-by-id)**

- Endpoint: [https://api/v1/aggregation](https://app.pendo.io/api/v1/aggregation)
- Primary key fields: `visitor_id`, `account_id`, `server`, `remote_ip`
- Replication strategy: INCREMENTAL (query filtered)
  - Bookmark: `day` or `hour`
- Transformations: Camel to snake case.

**[guide_events](https://developers.pendo.io/docs/?bash#get-an-account-by-id)**

- Endpoint: [https://api/v1/aggregation](https://app.pendo.io/api/v1/aggregation)
- Primary key fields: `visitor_id`, `account_id`, `server`, `remote_ip`
- Replication strategy: INCREMENTAL (query filtered)
  - Bookmark: `browserTime`
- Transformations: Camel to snake case.

**[poll_events](https://developers.pendo.io/docs/?bash#get-an-account-by-id)**

- Endpoint: [https://api/v1/aggregation](https://app.pendo.io/api/v1/aggregation)
- Primary key fields: `visitor_id`, `account_id`, `server`, `remote_ip`
- Replication strategy: INCREMENTAL (query filtered)
  - Bookmark: `browserTime`
- Transformations: Camel to snake case.

**[track_events](https://developers.pendo.io/docs/?bash#get-an-account-by-id)**

- Endpoint: [https://api/v1/aggregation](https://app.pendo.io/api/v1/aggregation)
- Primary key fields: `visitor_id`, `account_id`, `server`, `remote_ip`
- Replication strategy: INCREMENTAL (query filtered)
  - Bookmark: `day` or `hour`
- Transformations: Camel to snake case.

**[guides](https://developers.pendo.io/docs/?bash#entities)**

- Endpoint: [https://api/v1/aggregation](https://app.pendo.io/api/v1/aggregation)
- Primary key fields: `id`
- Replication strategy: INCREMENTAL (query filtered)
  - Bookmark: `last_pdated_at`
- Transformations: Camel to snake case.

**[metadata accounts](https://developers.pendo.io/docs/?bash#automatically-generated-metadata)**

- Endpoint: [https://api/v1/metadata/schema/account](https://app.pendo.io/api/v1/metadata/schema/account)
- Replication strategy: FULL_TABLE
- Transformations: Camel to snake case.

**[metadata visitors](https://developers.pendo.io/docs/?bash#automatically-generated-metadata)**

- Endpoint: [https://api/v1/metadata/schema/account](https://app.pendo.io/api/v1/metadata/schema/visitor)
- Replication strategy: FULL_TABLE
- Transformations: Camel to snake case.

## Authentication

Authentication is managed by integration keys. An integration key may be created in the Pendo website: Settings -> Integrations -> Integration Keys.

## State

```json
{
  "currently_syncing": null,
  "bookmarks": {
    "pages": { "lastUpdatedAt": "2020-09-26T00:00:00.000000Z" },
    "page_events": { "day": "2020-09-27T04:00:00.000000Z" },
    "accounts": { "lastupdated": "2020-09-28T01:30:29.237000Z" },
    "feature_events": { "day": "2020-09-27T04:00:00.000000Z" },
    "guides": { "lastUpdatedAt": "2020-09-26T00:00:00.000000Z" },
    "poll_events": { "day": "2020-09-26T00:00:00.000000Z" },
    "events": { "day": "2020-09-27T04:00:00.000000Z" },
    "visitors": { "lastupdated": "2020-09-28T01:30:29.199000Z" },
    "features": { "lastUpdatedAt": "2020-09-26T00:00:00.000000Z" },
    "guide_events": { "day": "2020-09-26T00:00:00.000000Z" },
    "track_types": { "lastUpdatedAt": "2020-09-26T00:00:00.000000Z" }
  }
}
```

Interrupted syncs for Event type stream are resumed via a bookmark placed during processing, `last_processed`. The value of the parent GUID will be

```json
{
  "bookmarks": {
    "guides": { "lastUpdatedAt": "2020-09-22T20:23:44.514000Z" },
    "poll_events": { "day": "2020-09-20T00:00:00.000000Z" },
    "feature_events": { "day": "2020-09-27T04:00:00.000000Z" },
    "visitors": { "lastupdated": "2020-09-27T15:40:02.729000Z" },
    "pages": { "lastUpdatedAt": "2020-09-20T00:00:00.000000Z" },
    "track_types": { "lastUpdatedAt": "2020-09-20T00:00:00.000000Z" },
    "features": { "lastUpdatedAt": "2020-09-20T00:00:00.000000Z" },
    "accounts": { "lastupdated": "2020-09-27T15:39:50.585000Z" },
    "guide_events": { "day": "2020-09-20T00:00:00.000000Z" },
    "page_events": {
      "day": "2020-09-27T04:00:00.000000Z",
      "last_processed": "_E9IwR8tFCTQryv_hCzGVZvsgcg"
    },
    "events": { "day": "2020-09-27T04:00:00.000000Z" }
  },
  "currently_syncing": "track_events"
}
```

## Quick Start

1. Install

   Clone this repository, and then install using setup.py. We recommend using a virtualenv:

   ```bash
   > virtualenv -p python3 venv
   > source venv/bin/activate
   > python setup.py install
   OR
   > cd .../tap-pendo
   > pip install .
   ```

2. Dependent libraries. The following dependent libraries were installed.

   ```bash
   > pip install singer-python
   > pip install jsonlines
   > pip install singer-tools
   > pip install target-stitch
   > pip install target-json

   ```

   - [singer-tools](https://github.com/singer-io/singer-tools)
   - [target-stitch](https://github.com/singer-io/target-stitch)
   - [jsonlines](https://jsonlines.readthedocs.io/en/latest/)

3. Create your tap's `config.json` file. The tap config file for this tap should include these entries:

   - `start_date` - the default value to use if no bookmark exists for an endpoint (rfc3339 date string)
   - `x_pendo_integration_key` (string, `ABCdef123`): an integration key from Pendo.
   - `period` (string, `ABCdef123`): `dayRange` or `hourRange`
   - `lookback_window` (integer): 10 (For event objects. Default: 0)

   ```json
   {
     "x_pendo_integration_key": "YOUR_INTEGRATION_KEY",
     "start_date": "2020-09-18T00:00:00Z",
     "period": "dayRange",
     "lookback_window": 10,
     "include_anonymous_visitors: "true"
   }
   ```

4. Run the Tap in Discovery Mode
   This creates a catalog.json for selecting objects/fields to integrate:

   ```bash
   tap-pendo --config config.json --discover > catalog.json
   ```

   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

   For Sync mode:

   ```bash
   > tap-pendo --config tap_config.json --catalog catalog.json > state.json
   > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
   ```

   To load to json files to verify outputs:

   ```bash
   > tap-pendo --config tap_config.json --catalog catalog.json | target-json > state.json
   > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
   ```

   To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:

   ```bash
   > tap-pendo --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
   > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
   ```

6. Test the Tap

   While developing the pendo tap, the following utilities were run in accordance with Singer.io best practices:
   Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality):

   ```bash
   > pylint tap_pendo -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
   ```

   Pylint test resulted in the following score:

   ```bash
   Your code has been rated at 9.67/10
   ```

   To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:

   ```bash
   > tap-pendo --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
   > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
   ```

   Check tap resulted in the following:

   ```bash
   Checking stdin for valid Singer-formatted data
   The output is valid.
   It contained 3734 messages for 11 streams.

       13 schema messages
     3702 record messages
       19 state messages

   Details by stream:
   +----------------+---------+---------+
   | stream         | records | schemas |
   +----------------+---------+---------+
   | accounts       | 1       | 1       |
   | features       | 29      | 1       |
   | feature_events | 158     | 2       |
   | guides         | 0       | 1       |
   | pages          | 34      | 1       |
   | page_events    | 830     | 2       |
   | reports        | 2       | 1       |
   | visitors       | 1902    | 1       |
   | events         | 746     | 1       |
   | guide_events   | 0       | 1       |
   | poll_events    | 0       | 1       |
   +----------------+---------+---------+
   ```

   #### Unit Tests

   Unit tests may be run with the following.

   ```
   python -m pytest --verbose
   ```

   Note, you may need to install test dependencies.

   ```
   pip install -e .'[dev]'
   ```

---

Copyright &copy; 2020 Stitch
