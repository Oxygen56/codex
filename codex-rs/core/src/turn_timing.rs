use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use codex_otel::TURN_TTFM_DURATION_METRIC;
use codex_protocol::items::TurnItem;
use codex_protocol::models::ResponseItem;
use tokio::sync::Mutex;

use crate::ResponseEvent;
use crate::session::turn_context::TurnContext;
use crate::stream_events_utils::raw_assistant_output_text_from_item;

pub(crate) async fn record_turn_ttft_metric(turn_context: &TurnContext, event: &ResponseEvent) {
    let Some(duration) = turn_context
        .turn_timing_state
        .record_ttft_for_response_event(event)
        .await
    else {
        return;
    };
    turn_context.session_telemetry.record_turn_ttft(duration);
}

pub(crate) async fn record_turn_ttfm_metric(turn_context: &TurnContext, item: &TurnItem) {
    let Some(duration) = turn_context
        .turn_timing_state
        .record_ttfm_for_turn_item(item)
        .await
    else {
        return;
    };
    turn_context
        .session_telemetry
        .record_duration(TURN_TTFM_DURATION_METRIC, duration, &[]);
}

#[derive(Debug, Default)]
pub(crate) struct TurnTimingState {
    state: Mutex<TurnTimingStateInner>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TurnSamplingPhaseTimings {
    pub(crate) sampling_request_count: u64,
    pub(crate) sampling_request_duration_ms: u64,
    pub(crate) sampling_retry_count: u64,
    pub(crate) sampling_retry_delay_duration_ms: u64,
    pub(crate) pre_sampling_duration_ms: u64,
    pub(crate) inter_sampling_duration_ms: u64,
    pub(crate) post_sampling_duration_ms: u64,
    pub(crate) request_user_input_count: u64,
    pub(crate) request_user_input_wait_duration_ms: u64,
}

#[derive(Debug, Default)]
struct TurnTimingStateInner {
    started_at: Option<Instant>,
    started_at_unix_secs: Option<i64>,
    first_token_at: Option<Instant>,
    first_message_at: Option<Instant>,
    first_sampling_started_at: Option<Instant>,
    active_sampling_started_at: Option<Instant>,
    last_sampling_completed_at: Option<Instant>,
    sampling_request_count: u64,
    sampling_request_duration: Duration,
    sampling_retry_count: u64,
    sampling_retry_delay_duration: Duration,
    inter_sampling_duration: Duration,
    request_user_input_count: u64,
    request_user_input_wait_duration: Duration,
}

impl TurnTimingState {
    pub(crate) async fn mark_turn_started(&self, started_at: Instant) -> i64 {
        let started_at_unix_ms = now_unix_timestamp_ms();
        let mut state = self.state.lock().await;
        state.started_at = Some(started_at);
        state.started_at_unix_secs = Some(started_at_unix_ms / 1000);
        state.first_token_at = None;
        state.first_message_at = None;
        state.first_sampling_started_at = None;
        state.active_sampling_started_at = None;
        state.last_sampling_completed_at = None;
        state.sampling_request_count = 0;
        state.sampling_request_duration = Duration::ZERO;
        state.sampling_retry_count = 0;
        state.sampling_retry_delay_duration = Duration::ZERO;
        state.inter_sampling_duration = Duration::ZERO;
        state.request_user_input_count = 0;
        state.request_user_input_wait_duration = Duration::ZERO;
        started_at_unix_ms
    }

    pub(crate) async fn started_at_unix_secs(&self) -> Option<i64> {
        self.state.lock().await.started_at_unix_secs
    }

    pub(crate) async fn completed_at_duration_and_sampling_phase(
        &self,
    ) -> (Option<i64>, Option<i64>, Option<TurnSamplingPhaseTimings>) {
        let completed_at_instant = Instant::now();
        let state = self.state.lock().await;
        let completed_at = Some(now_unix_timestamp_secs());
        let duration_ms = state.started_at.map(|started_at| {
            duration_millis_i64(completed_at_instant.saturating_duration_since(started_at))
        });
        let sampling_phase = state.sampling_phase_timings(completed_at_instant);
        (completed_at, duration_ms, sampling_phase)
    }

    pub(crate) async fn mark_sampling_started(&self) {
        self.mark_sampling_started_at(Instant::now()).await;
    }

    pub(crate) async fn mark_sampling_completed(&self) {
        self.mark_sampling_completed_at(Instant::now()).await;
    }

    pub(crate) async fn record_sampling_retry(&self, delay: Duration) {
        let mut state = self.state.lock().await;
        state.sampling_retry_count = state.sampling_retry_count.saturating_add(1);
        state.sampling_retry_delay_duration =
            state.sampling_retry_delay_duration.saturating_add(delay);
    }

    pub(crate) async fn record_request_user_input_wait(&self, duration: Duration) {
        let mut state = self.state.lock().await;
        state.request_user_input_count = state.request_user_input_count.saturating_add(1);
        state.request_user_input_wait_duration = state
            .request_user_input_wait_duration
            .saturating_add(duration);
    }

    pub(crate) async fn time_to_first_token_ms(&self) -> Option<i64> {
        let state = self.state.lock().await;
        state
            .time_to_first_token()
            .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
    }

    pub(crate) async fn record_ttft_for_response_event(
        &self,
        event: &ResponseEvent,
    ) -> Option<Duration> {
        if !response_event_records_turn_ttft(event) {
            return None;
        }
        let mut state = self.state.lock().await;
        state.record_turn_ttft()
    }

    pub(crate) async fn record_ttfm_for_turn_item(&self, item: &TurnItem) -> Option<Duration> {
        if !matches!(item, TurnItem::AgentMessage(_)) {
            return None;
        }
        let mut state = self.state.lock().await;
        state.record_turn_ttfm()
    }

    async fn mark_sampling_started_at(&self, started_at: Instant) {
        let mut state = self.state.lock().await;
        state.mark_sampling_started(started_at);
    }

    async fn mark_sampling_completed_at(&self, completed_at: Instant) {
        let mut state = self.state.lock().await;
        state.mark_sampling_completed(completed_at);
    }

    #[cfg(test)]
    async fn sampling_phase_timings_at(
        &self,
        completed_at: Instant,
    ) -> Option<TurnSamplingPhaseTimings> {
        self.state.lock().await.sampling_phase_timings(completed_at)
    }
}

fn now_unix_timestamp_secs() -> i64 {
    now_unix_timestamp_ms() / 1000
}

pub(crate) fn now_unix_timestamp_ms() -> i64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

impl TurnTimingStateInner {
    fn mark_sampling_started(&mut self, started_at: Instant) {
        if self.started_at.is_none() || self.active_sampling_started_at.is_some() {
            return;
        }
        if self.first_sampling_started_at.is_none() {
            self.first_sampling_started_at = Some(started_at);
        } else if let Some(last_sampling_completed_at) = self.last_sampling_completed_at {
            self.inter_sampling_duration = self
                .inter_sampling_duration
                .saturating_add(started_at.saturating_duration_since(last_sampling_completed_at));
        }
        self.active_sampling_started_at = Some(started_at);
        self.sampling_request_count = self.sampling_request_count.saturating_add(1);
    }

    fn mark_sampling_completed(&mut self, completed_at: Instant) {
        let Some(started_at) = self.active_sampling_started_at.take() else {
            return;
        };
        self.sampling_request_duration = self
            .sampling_request_duration
            .saturating_add(completed_at.saturating_duration_since(started_at));
        self.last_sampling_completed_at = Some(completed_at);
    }

    fn sampling_phase_timings(&self, completed_at: Instant) -> Option<TurnSamplingPhaseTimings> {
        let turn_started_at = self.started_at?;
        let first_sampling_started_at = self.first_sampling_started_at?;
        let active_sampling_duration = self
            .active_sampling_started_at
            .map(|started_at| completed_at.saturating_duration_since(started_at))
            .unwrap_or_default();
        let sampling_request_duration = self
            .sampling_request_duration
            .saturating_add(active_sampling_duration);
        let post_sampling_duration = if self.active_sampling_started_at.is_some() {
            Duration::ZERO
        } else {
            self.last_sampling_completed_at
                .map(|last_completed_at| completed_at.saturating_duration_since(last_completed_at))
                .unwrap_or_default()
        };
        Some(TurnSamplingPhaseTimings {
            sampling_request_count: self.sampling_request_count,
            sampling_request_duration_ms: duration_millis_u64(sampling_request_duration),
            sampling_retry_count: self.sampling_retry_count,
            sampling_retry_delay_duration_ms: duration_millis_u64(
                self.sampling_retry_delay_duration,
            ),
            pre_sampling_duration_ms: duration_millis_u64(
                first_sampling_started_at.saturating_duration_since(turn_started_at),
            ),
            inter_sampling_duration_ms: duration_millis_u64(self.inter_sampling_duration),
            post_sampling_duration_ms: duration_millis_u64(post_sampling_duration),
            request_user_input_count: self.request_user_input_count,
            request_user_input_wait_duration_ms: duration_millis_u64(
                self.request_user_input_wait_duration,
            ),
        })
    }

    fn time_to_first_token(&self) -> Option<Duration> {
        Some(self.first_token_at?.duration_since(self.started_at?))
    }

    fn record_turn_ttft(&mut self) -> Option<Duration> {
        if self.first_token_at.is_some() {
            return None;
        }
        self.started_at?;
        self.first_token_at = Some(Instant::now());
        self.time_to_first_token()
    }

    fn record_turn_ttfm(&mut self) -> Option<Duration> {
        if self.first_message_at.is_some() {
            return None;
        }
        let started_at = self.started_at?;
        let first_message_at = Instant::now();
        self.first_message_at = Some(first_message_at);
        Some(first_message_at.duration_since(started_at))
    }
}

fn duration_millis_i64(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn response_event_records_turn_ttft(event: &ResponseEvent) -> bool {
    match event {
        ResponseEvent::OutputItemDone(item) | ResponseEvent::OutputItemAdded(item) => {
            response_item_records_turn_ttft(item)
        }
        ResponseEvent::OutputTextDelta(_)
        | ResponseEvent::ReasoningSummaryDelta { .. }
        | ResponseEvent::ReasoningContentDelta { .. } => true,
        ResponseEvent::Created
        | ResponseEvent::ServerModel(_)
        | ResponseEvent::ModelVerifications(_)
        | ResponseEvent::TurnModerationMetadata(_)
        | ResponseEvent::ServerReasoningIncluded(_)
        | ResponseEvent::ToolCallInputDelta { .. }
        | ResponseEvent::Completed { .. }
        | ResponseEvent::ReasoningSummaryPartAdded { .. }
        | ResponseEvent::RateLimits(_)
        | ResponseEvent::ModelsEtag(_) => false,
    }
}

fn response_item_records_turn_ttft(item: &ResponseItem) -> bool {
    match item {
        ResponseItem::Message { .. } => {
            raw_assistant_output_text_from_item(item).is_some_and(|text| !text.is_empty())
        }
        ResponseItem::Reasoning {
            summary, content, ..
        } => {
            summary.iter().any(|entry| match entry {
                codex_protocol::models::ReasoningItemReasoningSummary::SummaryText { text } => {
                    !text.is_empty()
                }
            }) || content.as_ref().is_some_and(|entries| {
                entries.iter().any(|entry| match entry {
                    codex_protocol::models::ReasoningItemContent::ReasoningText { text }
                    | codex_protocol::models::ReasoningItemContent::Text { text } => {
                        !text.is_empty()
                    }
                })
            })
        }
        ResponseItem::AgentMessage { .. } => false,
        ResponseItem::LocalShellCall { .. }
        | ResponseItem::FunctionCall { .. }
        | ResponseItem::CustomToolCall { .. }
        | ResponseItem::ToolSearchCall { .. }
        | ResponseItem::WebSearchCall { .. }
        | ResponseItem::ImageGenerationCall { .. }
        | ResponseItem::Compaction { .. }
        | ResponseItem::ContextCompaction { .. } => true,
        ResponseItem::CompactionTrigger => false,
        ResponseItem::FunctionCallOutput { .. }
        | ResponseItem::CustomToolCallOutput { .. }
        | ResponseItem::ToolSearchOutput { .. }
        | ResponseItem::Other => false,
    }
}

#[cfg(test)]
#[path = "turn_timing_tests.rs"]
mod tests;
