//! [Prometheus][prometheus] metrics.
//!
//! [prometheus]: https://prometheus.io/
use std::collections::HashMap;
use prometrics::metrics::{Counter, MetricBuilder};

use ProcedureId;

/// Client side metrics.
#[derive(Debug, Clone)]
pub struct ClientMetrics {
    pub(crate) notifications: Counter,
    pub(crate) requests: Counter,
    pub(crate) ok_responses: Counter,
    pub(crate) error_responses: Counter,
    pub(crate) discarded_outgoing_messages: Counter,
    pub(crate) channel: ChannelMetrics,
}
impl ClientMetrics {
    /// Metric: `fibers_rpc_client_notifications_total <COUNTER>`.
    pub fn notifications(&self) -> u64 {
        self.notifications.value() as u64
    }

    /// Metric: `fibers_rpc_client_requests_total <COUNTER>`.
    pub fn requests(&self) -> u64 {
        self.requests.value() as u64
    }

    /// Metric: `fibers_rpc_client_responses_total { result="ok" } <COUNTER>`.
    pub fn ok_responses(&self) -> u64 {
        self.ok_responses.value() as u64
    }

    /// Metric: `fibers_rpc_client_responses_total { result="error" } <COUNTER>`.
    pub fn error_responses(&self) -> u64 {
        self.error_responses.value() as u64
    }

    /// Metric: `fibers_rpc_client_discarded_outgoing_messages_total <COUNTER>`.
    pub fn discarded_outgoing_messages(&self) -> u64 {
        self.discarded_outgoing_messages.value() as u64
    }

    /// Returns the metrics of the channels associated with the client service.
    pub fn channel(&self) -> &ChannelMetrics {
        &self.channel
    }

    pub(crate) fn new(mut builder: MetricBuilder) -> Self {
        builder.namespace("fibers_rpc").subsystem("client");
        ClientMetrics {
            notifications: builder
                .counter("notifications_total")
                .help("Number of notification messages that started sending")
                .finish()
                .expect("Never fails"),
            requests: builder
                .counter("requests_total")
                .help("Number of request messages that started sending")
                .finish()
                .expect("Never fails"),
            ok_responses: builder
                .counter("responses_total")
                .help("Number of received response messages")
                .label("result", "ok")
                .finish()
                .expect("Never fails"),
            error_responses: builder
                .counter("responses_total")
                .help("Number of received response messages")
                .label("result", "error")
                .finish()
                .expect("Never fails"),
            discarded_outgoing_messages: builder
                .counter("discarded_outgoing_messages_total")
                .help("Number of discarded messages before sending")
                .finish()
                .expect("Never fails"),
            channel: ChannelMetrics::new(builder.subsystem("channel").label("role", "client")),
        }
    }
}

/// Server side metrics.
#[derive(Debug, Clone)]
pub struct ServerMetrics {
    pub(crate) channel: ChannelMetrics,
    handlers: HashMap<ProcedureId, HandlerMetrics>,
}
impl ServerMetrics {
    /// Returns the metrics of the channels associated with the server.
    pub fn channel(&self) -> &ChannelMetrics {
        &self.channel
    }

    /// Returns the metrics of the handlers registered on the server.
    pub fn handlers(&self) -> &HashMap<ProcedureId, HandlerMetrics> {
        &self.handlers
    }

    pub(crate) fn new(
        mut builder: MetricBuilder,
        handlers: HashMap<ProcedureId, HandlerMetrics>,
    ) -> Self {
        builder.namespace("fibers_rpc").subsystem("server");
        ServerMetrics {
            channel: ChannelMetrics::new(builder.subsystem("channel").label("role", "server")),
            handlers,
        }
    }
}

/// RPC handler metrics.
#[derive(Debug, Clone)]
pub struct HandlerMetrics {
    pub(crate) rpc_count: Counter,
}
impl HandlerMetrics {
    /// Metric: `fibers_rpc_handler_rpc_tocal { type="call|cast", procedure="${ID}@${NAME}" } <COUNTER>`.
    pub fn rpc_count(&self) -> u64 {
        self.rpc_count.value() as u64
    }

    pub(crate) fn new(
        mut builder: MetricBuilder,
        id: ProcedureId,
        name: &str,
        rpc_type: &str,
    ) -> Self {
        builder.namespace("fibers_rpc").subsystem("handler");

        let procedure = format!("{}@{}", id.0, name);
        HandlerMetrics {
            rpc_count: builder
                .counter("rpc_total")
                .help("Number of RPCs invoked by clients")
                .label("procedure", &procedure)
                .label("type", rpc_type)
                .finish()
                .expect("Never fails"),
        }
    }
}

/// RPC channel metrics.
#[derive(Debug, Clone)]
pub struct ChannelMetrics {
    pub(crate) created_channels: Counter,
    pub(crate) removed_channels: Counter,
    pub(crate) fiber_yielded: Counter,
    pub(crate) encode_frame_failures: Counter,
    pub(crate) decode_frame_failures: Counter,
    pub(crate) enqueued_outgoing_messages: Counter,
    pub(crate) dequeued_outgoing_messages: Counter,
}
impl ChannelMetrics {
    /// Metric: `fibers_rpc_channel_created_channels_total { role="server|client" } <COUNTER>`.
    pub fn created_channels(&self) -> u64 {
        self.created_channels.value() as u64
    }

    /// Metric: `fibers_rpc_channel_removed_channels_total { role="server|client" } <COUNTER>`.
    pub fn removed_channels(&self) -> u64 {
        self.removed_channels.value() as u64
    }

    /// Metric: `fibers_rpc_channel_fiber_yielded_total { role="server|client" } <COUNTER>`.
    pub fn fiber_yielded(&self) -> u64 {
        self.fiber_yielded.value() as u64
    }

    /// Metric: `fibers_rpc_channel_encode_frame_failures { role="server|client" } <COUNTER>`.
    pub fn encode_frame_failures(&self) -> u64 {
        self.encode_frame_failures.value() as u64
    }

    /// Metric: `fibers_rpc_channel_decode_frame_failures { role="server|client" } <COUNTER>`.
    pub fn decode_frame_failures(&self) -> u64 {
        self.decode_frame_failures.value() as u64
    }

    /// Metric: `fibers_rpc_channel_enqueued_outgoing_messages { role="server|client" } <COUNTER>`.
    pub fn enqueued_outgoing_messages(&self) -> u64 {
        self.enqueued_outgoing_messages.value() as u64
    }

    /// Metric: `fibers_rpc_channel_dequeued_outgoing_messages { role="server|client" } <COUNTER>`.
    pub fn dequeued_outgoing_messages(&self) -> u64 {
        self.dequeued_outgoing_messages.value() as u64
    }

    fn new(builder: &mut MetricBuilder) -> Self {
        ChannelMetrics {
            created_channels: builder
                .counter("created_channels_total")
                .help("Number of created RPC channels")
                .finish()
                .expect("Never fails"),
            removed_channels: builder
                .counter("removed_channels_total")
                .help("Number of removed RPC channels")
                .finish()
                .expect("Never fails"),
            fiber_yielded: builder
                .counter("fiber_yielded_total")
                .help("Number of `fibers::fiber::yield_poll()` function calls")
                .finish()
                .expect("Never fails"),
            encode_frame_failures: builder
                .counter("encode_frame_failures_total")
                .help("Number of frame encoding failures")
                .finish()
                .expect("Never fails"),
            decode_frame_failures: builder
                .counter("decode_frame_failures_total")
                .help("Number of frame decoding failures")
                .finish()
                .expect("Never fails"),
            enqueued_outgoing_messages: builder
                .counter("enqueued_outgoing_messages_total")
                .help("Number of enqueued outgoing messages")
                .finish()
                .expect("Never fails"),
            dequeued_outgoing_messages: builder
                .counter("dequeued_outgoing_messages_total")
                .help("Number of dequeued outgoing messages")
                .finish()
                .expect("Never fails"),
        }
    }
}
