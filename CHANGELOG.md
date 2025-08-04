# Changelog

## 1.2.0
  * Use replication_key in last_processed instead of primary_key [#133](https://github.com/singer-io/tap-pendo/pull/133)

## 1.1.1
  * Bump dependency versions for twistlock compliance
  * Update tests to fix failing circle build
  * [#131](https://github.com/singer-io/tap-pendo/pull/131)

## 1.1.0
  * Add the the custom pagination support for the Visitors stream [#126](https://github.com/singer-io/tap-pendo/pull/129)

## 1.0.1
  * Fix infinite loop issue [#126](https://github.com/singer-io/tap-pendo/pull/126)

## 1.0.0
  * Update key properties for streams, fixes issue [#34](https://github.com/singer-io/tap-pendo/issues/34) with missing records [#88](https://github.com/singer-io/tap-pendo/pull/88)
  * Add support for app_id selection [#94](https://github.com/singer-io/tap-pendo/pull/94)

## 0.6.2
  * Update the lookback window validation to use typecasting using `int()` function instead of `isdigit()` [#93](https://github.com/singer-io/tap-pendo/pull/93)

## 0.6.1
  * Fix the type error in the accounts stream records that resulted from comparing a none value. [#122](https://github.com/singer-io/tap-pendo/pull/122)

## 0.6.0
  * Fix connection reset, request timeout and memory overflow issues [#116](https://github.com/singer-io/tap-pendo/pull/116)
  * Refactor child stream methods
  * Implement logic to increase the request timeout duration during retry attempt [#118](https://github.com/singer-io/tap-pendo/pull/118)
  * Reduce backoff time

## 0.5.2
  * Fixes visitor_history duplicate record sync issue [#119](https://github.com/singer-io/tap-pendo/pull/119)

## 0.5.1
  * Skips visitor_history records for visitors with Do Not Process flag [#115](https://github.com/singer-io/tap-pendo/pull/115)

## 0.5.0
  * Fixed connection reset and read timeout errors [#102](https://github.com/singer-io/tap-pendo/pull/102)
  * Implemented new bookmark strategy for child streams to boost the extracation performance
  * Added custom pagination mechanism to fix out-of-memory issues
  * Fixed interrupted sync

## 0.4.3
  * Handle empty `include_anonymous_visitors` properties from config [#111](https://github.com/singer-io/tap-pendo/pull/111)

## 0.4.2
  * Retry when the connection snaps [#98](https://github.com/singer-io/tap-pendo/pull/98)

## 0.4.1
  * Allow custom metadata types to be more permissive [#97](https://github.com/singer-io/tap-pendo/pull/97)

## 0.4.0
  * Add support for EU endpoints [#95](https://github.com/singer-io/tap-pendo/pull/95)

## 0.3.2
  * Added data windowing for the events stream with configurable window size [#89](https://github.com/singer-io/tap-pendo/pull/89)

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
  * Add boolean type handling to custom field discovery
  * When matching custom field types, any unknown type will raise an exception where the error occurred, rather than generating an invalid schema

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
