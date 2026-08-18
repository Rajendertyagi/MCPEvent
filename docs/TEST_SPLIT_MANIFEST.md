# Test Split Manifest

## Original Files

| File | Lines | Test Functions |
|------|-------|----------------|
| `test/test_phase8.py` | 1917 | 45 (P8U1-4, D1-7, R1-5, SEC1-3, P8T1-10, N1-4, S1-15) |
| `test/integrate_test.py` | 1309 | 45 (T1-14, CP1-6, P7T1-22, PERF1-3) |

**Total original:** ~3226 lines, ~90 test functions

## Final Files

| New File | Lines | Tests | Source |
|----------|-------|-------|--------|
| `test/helpers/__init__.py` | 1 | - | New |
| `test/helpers/lifecycle.py` | 296 | - | New (shared) |
| `test/helpers/mcp.py` | 116 | - | New (shared) |
| `test/helpers/mock_http.py` | 39 | - | New (shared) |
| `test/helpers/runner.py` | 91 | - | New (shared) |
| `test/test_unit_sources.py` | 492 | 13 | P8U1-4, R1-5, SEC1-3, D6, D7 |
| `test/test_source_dedup.py` | 524 | 3 | D1D2D3, D4D5, S6 |
| `test/test_source_lifecycle.py` | 547 | 14 | S1-S15, R4 |
| `test/test_events.py` | 466 | 23 | T1-T14, P8T1-P8T9 |
| `test/test_consumers.py` | 224 | 6 | T5-T7, T11, P7T2, P7T9 |
| `test/test_acknowledgement.py` | 245 | 9 | CP1-6, T5, T12, P7T22 |
| `test/test_reconnect.py` | 198 | 3 | T10, P7T13, P8T10 |
| `test/test_errors.py` | 212 | 4 | P7T6, P7T14-P7T17 |
| `test/test_timeouts.py` | 161 | 3 | P7T5, P7T16, S10 |
| `test/test_background_tasks.py` | 161 | 5 | P7T10-P7T12, P7T20-P7T21 |
| `test/test_subscriptions.py` | 214 | 5 | N1-N4, S4 |
| `test/test_performance.py` | 120 | 3 | PERF1-PERF3 |
| `test/test_lifespan.py` | 77 | 1 | P7T19 |
| `test/test_sdk_alignment.py` | 190 | 8 | T1-T4, P7T1, P7T3, P7T4, P8T8 |
| `test/test_multi_client.py` | 163 | 3 | P7T8 |
| `test/run_all.py` | 95 | - | New (runner) |

## Old Files Disposition

| File | Status |
|------|--------|
| `test/test_phase8.py` | Kept as reference (not run by run_all.py) |
| `test/integrate_test.py` | **Converted to compatibility runner** — delegates to feature files |
| `test/mcp_result.py` | Kept (shared utility) |

## Migration Map

### Unit Tests (no server)
| Legacy | New File | New Name |
|--------|----------|----------|
| P8-U1 | test_unit_sources.py | test_source_state_crud |
| P8-U2 | test_unit_sources.py | test_http_json_poller_unit |
| P8-U3 | test_unit_sources.py | test_source_manager_registration |
| P8-U4 | test_unit_sources.py | test_create_publisher |
| R1 | test_unit_sources.py | test_registry_known_type |
| R2 | test_unit_sources.py | test_registry_two_instances |
| R3 | test_unit_sources.py | test_registry_unknown_type |
| R5 | test_unit_sources.py | test_server_no_source_imports |
| SEC1 | test_unit_sources.py | test_url_sanitized |
| SEC2 | test_unit_sources.py | test_error_message_sanitized |
| SEC3 | test_unit_sources.py | test_env_secret_not_in_status |
| D6 | test_unit_sources.py | test_dedup_isolation_across_sources |
| D7 | test_unit_sources.py | test_dedup_pruning |

### Source Dedup
| Legacy | New File | New Name |
|--------|----------|----------|
| D1/D2/D3 | test_source_dedup.py | test_durable_dedup_cross_restart |
| D4/D5 | test_source_dedup.py | test_dedup_on_publish_failure |
| S6 | test_source_dedup.py | test_dedup_within_poller |

### Source Lifecycle
| Legacy | New File | New Name |
|--------|----------|----------|
| S1 | test_source_lifecycle.py | test_source_running |
| S2 | test_source_lifecycle.py | test_source_disabled |
| S3 | test_source_lifecycle.py | test_exactly_one |
| S5 | test_source_lifecycle.py | test_persistent |
| S7 | test_source_lifecycle.py | test_restart_dedup |
| S8 | test_source_lifecycle.py | test_external_failure |
| S9 | test_source_lifecycle.py | test_recovery |
| S10 | test_source_lifecycle.py | test_timeout |
| S11 | test_source_lifecycle.py | test_cancellation |
| S12 | test_source_lifecycle.py | test_pub_failure |
| S13 | test_source_lifecycle.py | test_malformed |
| S14 | test_source_lifecycle.py | test_concurrent |
| S15 | test_source_lifecycle.py | test_regression |
| R4 | test_source_lifecycle.py | test_registry_disabled |

### Events
| Legacy | New File | New Name |
|--------|----------|----------|
| T1 | test_events.py | test_list_tools |
| T2 | test_events.py | test_ping |
| T3 | test_events.py | test_generate_event |
| T4 | test_events.py | test_tool_schemas |
| T7 | test_events.py | test_topic_filter |
| T8 | test_events.py | test_transient |
| T9 | test_events.py | test_replay_order |
| T11 | test_events.py | test_topic_targeted |
| T12 | test_events.py | test_ack_clears |
| T13 | test_events.py | test_resource_event_latest |
| T14 | test_events.py | test_resource_server_info |
| P7T7 | test_events.py | test_progress |
| P7T18 | test_events.py | test_info_fields |
| P8T1 | test_events.py | test_sources_status |
| P8T2 | test_events.py | test_info_features |
| P8T3 | test_events.py | test_schema_v7 |
| P8T4 | test_events.py | test_test_source_events |
| P8T5 | test_events.py | test_max_events |
| P8T6 | test_events.py | test_failure_resilience |
| P8T7 | test_events.py | test_http_poller |
| P8T9 | test_events.py | test_graceful_shutdown |

### Consumers
| Legacy | New File | New Name |
|--------|----------|----------|
| T5 | test_consumers.py | test_out_of_order_ack |
| T6 | test_consumers.py | test_broadcast |
| T7 | test_consumers.py | test_topic_filter |
| T11 | test_consumers.py | test_topic_targeted |
| P7T2 | test_consumers.py | test_broadcast_semantics |
| P7T9 | test_consumers.py | test_subscription_isolation |

### Acknowledgement
| Legacy | New File | New Name |
|--------|----------|----------|
| CP1 | test_acknowledgement.py | test_checkpoint_init |
| CP2 | test_acknowledgement.py | test_ack_advances |
| CP3 | test_acknowledgement.py | test_gap_blocks |
| CP4 | test_acknowledgement.py | test_fill_gap |
| CP5 | test_acknowledgement.py | test_all_acked |
| CP6 | test_acknowledgement.py | test_monotonic |
| T5 | test_acknowledgement.py | test_out_of_order_ack |
| T12 | test_acknowledgement.py | test_ack_clears_pending |
| P7T22 | test_acknowledgement.py | test_checkpoint_suite |

### Reconnect
| Legacy | New File | New Name |
|--------|----------|----------|
| T10 | test_reconnect.py | test_persistence |
| P7T13 | test_reconnect.py | test_graceful_restart |
| P8T10 | test_reconnect.py | test_state_survives |

### Errors
| Legacy | New File | New Name |
|--------|----------|----------|
| P7T6 | test_errors.py | test_cancel |
| P7T14 | test_errors.py | test_shutdown_active |
| P7T15 | test_errors.py | test_shutdown_bg |
| P7T17 | test_errors.py | test_structured_errors |

### Timeouts
| Legacy | New File | New Name |
|--------|----------|----------|
| P7T5 | test_timeouts.py | test_long_running |
| P7T16 | test_timeouts.py | test_db_after_timeout |
| S10 | test_timeouts.py | test_timeout |

### Background Tasks
| Legacy | New File | New Name |
|--------|----------|----------|
| P7T10 | test_background_tasks.py | test_bg_lifecycle |
| P7T11 | test_background_tasks.py | test_bg_persist |
| P7T12 | test_background_tasks.py | test_bg_fail |
| P7T20 | test_background_tasks.py | test_extension_seam |
| P7T21 | test_background_tasks.py | test_bg_status |

### Subscriptions
| Legacy | New File | New Name |
|--------|----------|----------|
| N1 | test_subscriptions.py | test_direct_notify |
| N2 | test_subscriptions.py | test_direct_notify_dedup |
| N3 | test_subscriptions.py | test_transient_no_replay |
| N4 | test_subscriptions.py | test_persistent_replay |
| S4 | test_subscriptions.py | test_live_notify |

### Performance
| Legacy | New File | New Name |
|--------|----------|----------|
| PERF1 | test_performance.py | test_publish_storm |
| PERF2 | test_performance.py | test_resource_during_load |
| PERF3 | test_performance.py | test_concurrent_calls |

### SDK Alignment
| Legacy | New File | New Name |
|--------|----------|----------|
| T1 | test_sdk_alignment.py | test_list_tools_returns_enough |
| T2 | test_sdk_alignment.py | test_ping_returns_ok |
| T3 | test_sdk_alignment.py | test_generate_event_has_id |
| T4 | test_sdk_alignment.py | test_tool_schemas_are_valid |
| P7T1 | test_sdk_alignment.py | test_schema_v5_checkpoints |
| P7T3 | test_sdk_alignment.py | test_sync_tool_works |
| P7T4 | test_sdk_alignment.py | test_async_tools_work |
| P8T8 | test_sdk_alignment.py | test_extensibility_proof |

### Multi-Client
| Legacy | New File | New Name |
|--------|----------|----------|
| P7T8 | test_multi_client.py | test_concurrent_clients |

### Lifespan
| Legacy | New File | New Name |
|--------|----------|----------|
| P7T19 | test_lifespan.py | test_lifespan |
