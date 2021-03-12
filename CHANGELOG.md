# Changelog

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
