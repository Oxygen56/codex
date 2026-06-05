use codex_protocol::items::AgentMessageItem;
use codex_protocol::items::TurnItem;
use codex_protocol::models::ContentItem;
use codex_protocol::models::FunctionCallOutputPayload;
use codex_protocol::models::ResponseItem;
use pretty_assertions::assert_eq;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use super::TurnTimingState;
use super::response_item_records_turn_ttft;
use crate::ResponseEvent;

#[tokio::test]
async fn turn_timing_state_records_ttft_only_once_per_turn() {
    let state = TurnTimingState::default();
    assert_eq!(
        state
            .record_ttft_for_response_event(&ResponseEvent::OutputTextDelta("hi".to_string()))
            .await,
        None
    );

    state.mark_turn_started(Instant::now()).await;
    assert_eq!(
        state
            .record_ttft_for_response_event(&ResponseEvent::Created)
            .await,
        None
    );
    assert!(
        state
            .record_ttft_for_response_event(&ResponseEvent::OutputTextDelta("hi".to_string()))
            .await
            .is_some()
    );
    assert_eq!(
        state
            .record_ttft_for_response_event(&ResponseEvent::OutputTextDelta("again".to_string()))
            .await,
        None
    );
}

#[tokio::test]
async fn turn_timing_state_records_ttfm_independently_of_ttft() {
    let state = TurnTimingState::default();
    state.mark_turn_started(Instant::now()).await;

    assert!(
        state
            .record_ttft_for_response_event(&ResponseEvent::OutputTextDelta("hi".to_string()))
            .await
            .is_some()
    );
    assert!(
        state
            .record_ttfm_for_turn_item(&TurnItem::AgentMessage(AgentMessageItem {
                id: "msg-1".to_string(),
                content: Vec::new(),
                phase: None,
                memory_citation: None,
            }))
            .await
            .is_some()
    );
    assert_eq!(
        state
            .record_ttfm_for_turn_item(&TurnItem::AgentMessage(AgentMessageItem {
                id: "msg-2".to_string(),
                content: Vec::new(),
                phase: None,
                memory_citation: None,
            }))
            .await,
        None
    );
}

#[tokio::test]
async fn turn_timing_state_records_turn_started_epoch_millis() {
    let state = TurnTimingState::default();
    let before = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_millis();

    let started_at_unix_ms = state.mark_turn_started(Instant::now()).await;

    let after = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_millis();
    assert!(u128::try_from(started_at_unix_ms).is_ok_and(|ms| before <= ms && ms <= after));
    assert_eq!(
        state.started_at_unix_secs().await,
        Some(started_at_unix_ms / 1000)
    );
}

#[tokio::test]
async fn turn_timing_state_partitions_sampling_phases() {
    let state = TurnTimingState::default();
    let started_at = Instant::now();
    state.mark_turn_started(started_at).await;

    state
        .mark_sampling_started_at(started_at + Duration::from_millis(100))
        .await;
    state
        .mark_sampling_completed_at(started_at + Duration::from_millis(400))
        .await;
    state
        .mark_sampling_started_at(started_at + Duration::from_millis(700))
        .await;
    state
        .mark_sampling_completed_at(started_at + Duration::from_millis(1000))
        .await;
    state.record_sampling_retry(Duration::from_millis(50)).await;
    state
        .record_request_user_input_wait(Duration::from_millis(250))
        .await;

    assert_eq!(
        state
            .sampling_phase_timings_at(started_at + Duration::from_millis(1200))
            .await,
        Some(super::TurnSamplingPhaseTimings {
            sampling_request_count: 2,
            sampling_request_duration_ms: 600,
            sampling_retry_count: 1,
            sampling_retry_delay_duration_ms: 50,
            pre_sampling_duration_ms: 100,
            inter_sampling_duration_ms: 300,
            post_sampling_duration_ms: 200,
            request_user_input_count: 1,
            request_user_input_wait_duration_ms: 250,
        })
    );
}

#[tokio::test]
async fn turn_timing_state_counts_active_sampling_on_abort() {
    let state = TurnTimingState::default();
    let started_at = Instant::now();
    state.mark_turn_started(started_at).await;
    state
        .mark_sampling_started_at(started_at + Duration::from_millis(100))
        .await;

    assert_eq!(
        state
            .sampling_phase_timings_at(started_at + Duration::from_millis(500))
            .await,
        Some(super::TurnSamplingPhaseTimings {
            sampling_request_count: 1,
            sampling_request_duration_ms: 400,
            sampling_retry_count: 0,
            sampling_retry_delay_duration_ms: 0,
            pre_sampling_duration_ms: 100,
            inter_sampling_duration_ms: 0,
            post_sampling_duration_ms: 0,
            request_user_input_count: 0,
            request_user_input_wait_duration_ms: 0,
        })
    );
}

#[test]
fn response_item_records_turn_ttft_for_first_output_signals() {
    assert!(response_item_records_turn_ttft(
        &ResponseItem::FunctionCall {
            id: None,
            name: "shell".to_string(),
            namespace: None,
            arguments: "{}".to_string(),
            call_id: "call-1".to_string(),
        }
    ));
    assert!(response_item_records_turn_ttft(
        &ResponseItem::CustomToolCall {
            id: None,
            status: None,
            call_id: "call-2".to_string(),
            name: "custom".to_string(),
            input: "echo hi".to_string(),
        }
    ));
    assert!(response_item_records_turn_ttft(&ResponseItem::Message {
        id: None,
        role: "assistant".to_string(),
        content: vec![ContentItem::OutputText {
            text: "hello".to_string(),
        }],
        phase: None,
    }));
}

#[test]
fn response_item_records_turn_ttft_ignores_empty_non_output_items() {
    assert!(!response_item_records_turn_ttft(&ResponseItem::Message {
        id: None,
        role: "assistant".to_string(),
        content: vec![ContentItem::OutputText {
            text: String::new(),
        }],
        phase: None,
    }));
    assert!(!response_item_records_turn_ttft(
        &ResponseItem::FunctionCallOutput {
            call_id: "call-1".to_string(),
            output: FunctionCallOutputPayload::from_text("ok".to_string()),
        }
    ));
}
