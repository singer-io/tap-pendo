def config():
    return {
        "test_name": "test_sync",
        "tap_name": "tap-pendo",
        "type": "platform.slack",
        "properties": {
            "start_date": "TAP_PENDO_START_DATE",
            "date_window_size": "TAP_PENDO_DATE_WINDOW_SIZE",
            "lookback_window": "TAP_PENDO_LOOKBACK_WINDOW",
            "period": "TAP_PENDO_PERIOD",
        },
        "credentials": {
            "x_pendo_integration_key": "TAP_PENDO_INTEGRATION_KEY"
        },
        "bookmark": {
            "bookmark_key": "accounts",
            "bookmark_timestamp": "2020-10-13T01:48:18.865000Z"
        },
        "streams": {
            "accounts": {"account_id"},
            "features": {"id"},
            "guides": {"id"},
            "pages": {"id"},
            "visitor_history": {"visitor_id"},
            "visitors": {"key_properties"},
            "track": {"id"},
            "feature_events": {"visitor_id", "account_id", "server", "remote_ip"},
            "events": {"visitor_id", "account_id", "server", "remote_ip"},
            "page_events": {"visitor_id", "account_id", "server", "remote_ip"},
            "guide_events": {"visitor_id", "account_id", "server", "remote_ip"},
            "poll_events": {"visitor_id", "account_id", "server", "remote_ip"},
            "track_events": {"visitor_id", "account_id", "server", "remote_ip"}
        },
        "exclude_streams": []
    }