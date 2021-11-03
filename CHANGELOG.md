# Changelog

## 0.3.1
  * Fix Out of Memory Issue[#74](https://github.com/singer-io/tap-pendo/pull/74)

## 0.3.0
  * For FULL_TABLE stream, the record count shown by the logger message is incorrect[#59](https://github.com/singer-io/tap-pendo/pull/59)
  * Added code comments [#58](https://github.com/singer-io/tap-pendo/pull/58)
  * Data loss of child streams [#57](https://github.com/singer-io/tap-pendo/pull/57)
  * Removed endpoints dict [#54](https://github.com/singer-io/tap-pendo/pull/54)
  * Add retry logic to requests and timeouts and retries to requests [#46](https://github.com/singer-io/tap-pendo/pull/46)
  * Fix incremental streams behaves as a full table and Make replication key with automatic inclusion [#45](https://github.com/singer-io/tap-pendo/pull/45)


## 0.2.0
  * Added support for custom field of type "float" [#39](https://github.com/singer-io/tap-pendo/pull/39)

## 0.1.0
  * Added support for custom field of type "int" and of type empty string [#37](https://github.com/singer-io/tap-pendo/pull/37)

## 0.0.15
  * Add optional sync for anonymous Visitors

## 0.0.14
  * Fixed query params for `track_events` [#30](https://github.com/singer-io/tap-pendo/pull/30)

## 0.0.13
  * Fix issue with substreams of visitors due to new pattern using a generator rather than a list [#29](https://github.com/singer-io/tap-pendo/pull/28)

## 0.0.12
  * Stream `visitors` and `events` aggregation responses via ijson [#28](https://github.com/singer-io/tap-pendo/pull/28)

## 0.0.11
  * Fix for discovering all custom fields in `visitors` and `accounts` [#27](https://github.com/singer-io/tap-pendo/pull/27)
  * Add boolean type handling to custom field discovery [#27](https://github.com/singer-io/tap-pendo/pull/27)
  * When matching custom field types, any unknown type will raise an exception where the error occurred, rather than generating an invalid schema [#27](https://github.com/singer-io/tap-pendo/pull/27)

## 0.0.10
  * Allow for Discovery to succeed when no custom metadata is found [#22](https://github.com/singer-io/tap-pendo/pull/22)

## 0.0.9
  * Change Guide and Poll Events replication key

## 0.0.8
  * Add modified timestamp as max of ts and lastTs

## 0.0.7
  * Fixed an issue parsing optional parameter from config

## 0.0.6
  * Temporarily comment out visitor history stream until bug SRCE-4103 is addressed

## 0.0.5
  * Update schema items

## 0.0.4
  * Enforce numerical lookback_window in streams

## 0.0.3
  * Fix for custom account and visitor schema fields
