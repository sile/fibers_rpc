//! [Prometheus][prometheus] metrics.
//!
//! [prometheus]: https://prometheus.io/
use atomic_immut::AtomicImmut;
use prometrics::metrics::{Counter, MetricBuilder};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use ProcedureId;

/// Client side metrics.
#[derive(Debug, Clone)]
pub struct ClientMetrics {
    pub(crate) notifications: Counter,
    pub(crate) requests: Counter,
    pub(crate) ok_responses: Counter,
    pub(crate) error_responses: Counter,
    pub(crate) discarded_outgoing_messages: Counter,
    channels: ChannelsMetrics,
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
    pub fn channels(&self) -> &ChannelsMetrics {
        &self.channels
    }

    pub(crate) fn new(mut builder: MetricBuilder) -> Self {
        builder.namespace("fibers_rpc").subsystem("client");
        let mut channel_metrics_builder = builder.clone();
        channel_metrics_builder.subsystem("channel");
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
            channels: ChannelsMetrics::new(&builder, "client"),
        }
    }
}

/// Server side metrics.
#[derive(Debug, Clone)]
pub struct ServerMetrics {
    channels: ChannelsMetrics,
    handlers: HashMap<ProcedureId, HandlerMetrics>,
}
impl ServerMetrics {
    /// Returns the metrics of the channels associated with the server.
    pub fn channels(&self) -> &ChannelsMetrics {
        &self.channels
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
            handlers,
            channels: ChannelsMetrics::new(&builder, "server"),
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

        let procedure = format!("{:08x}@{}", id.0, name);
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

/// RPC channels metrics.
#[derive(Debug, Clone)]
pub struct ChannelsMetrics {
    channels: Arc<AtomicImmut<HashMap<SocketAddr, ChannelMetrics>>>,
    builder: Arc<Mutex<MetricBuilder>>,
    created_channels: Counter,
    removed_channels: Counter,
    correction: Arc<ChannelMetrics>,
}
impl ChannelsMetrics {
    /// Metric: `fibers_rpc_channel_created_channels_total { role="client" } <COUNTER>`.
    pub fn created_channels(&self) -> u64 {
        self.created_channels.value() as u64
    }

    /// Metric: `fibers_rpc_channel_removed_channels_total { role="client" } <COUNTER>`.
    pub fn removed_channels(&self) -> u64 {
        self.removed_channels.value() as u64
    }

    /// Returns a reference to the internal address-to-metrics map.
    pub fn as_map(&self) -> &Arc<AtomicImmut<HashMap<SocketAddr, ChannelMetrics>>> {
        &self.channels
    }

    pub(crate) fn create_channel_metrics(&self, server: SocketAddr) -> ChannelMetrics {
        if let Some(metrics) = self.channels.load().get(&server).cloned() {
            return metrics;
        }

        self.created_channels.increment();
        let correction = Arc::clone(&self.correction);
        let metrics = if let Ok(builder) = self.builder.lock() {
            ChannelMetrics::new(&builder, Some(correction))
        } else {
            ChannelMetrics::new(&MetricBuilder::without_registry(), Some(correction))
        };
        self.channels.update(|channels| {
            let mut channels = channels.clone();
            channels.insert(server, metrics.clone());
            channels
        });
        metrics
    }

    pub(crate) fn remove_channel_metrics(&self, server: SocketAddr) {
        if !self.channels.load().contains_key(&server) {
            return;
        }
        self.channels.update(|channels| {
            let mut channels = channels.clone();
            channels.remove(&server);
            channels
        });
    }

    fn new(parent_builder: &MetricBuilder, role: &str) -> Self {
        let mut builder = parent_builder.clone();
        builder.subsystem("channel").label("role", role);
        let correction = Arc::new(ChannelMetrics::new(&builder, None));
        ChannelsMetrics {
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
            channels: Arc::new(AtomicImmut::new(HashMap::new())),
            builder: Arc::new(Mutex::new(builder)),
            correction,
        }
    }
}

/// RPC channel metrics.
#[derive(Debug, Clone)]
pub struct ChannelMetrics {
    pub(crate) fiber_yielded: Counter,
    pub(crate) async_outgoing_messages: Counter,
    pub(crate) async_incoming_messages: Counter,
    pub(crate) enqueued_outgoing_messages: Counter,
    pub(crate) dequeued_outgoing_messages: Counter,
    correction: Option<Arc<ChannelMetrics>>,
}
impl ChannelMetrics {
    /// Metric: `fibers_rpc_channel_fiber_yielded_total { role="server|client" } <COUNTER>`.
    pub fn fiber_yielded(&self) -> u64 {
        self.fiber_yielded.value() as u64
    }

    /// Metric: `fibers_rpc_async_outgoing_messages_total { role="server|client" } <COUNTER>`.
    pub fn async_outgoing_messages(&self) -> u64 {
        self.async_outgoing_messages.value() as u64
    }

    /// Metric: `fibers_rpc_async_incoming_messages_total { role="server|client" } <COUNTER>`.
    pub fn async_incoming_messages(&self) -> u64 {
        self.async_incoming_messages.value() as u64
    }

    /// Metric: `fibers_rpc_channel_enqueued_outgoing_messages_total { role="server|client" } <COUNTER>`.
    pub fn enqueued_outgoing_messages(&self) -> u64 {
        self.enqueued_outgoing_messages.value() as u64
    }

    /// Metric: `fibers_rpc_channel_dequeued_outgoing_messages_total { role="server|client" } <COUNTER>`.
    pub fn dequeued_outgoing_messages(&self) -> u64 {
        self.dequeued_outgoing_messages.value() as u64
    }

    /// Returns the number of messages in the transmit queue of the channel.
    ///
    /// PromQL: `fibers_rpc_channel_enqueued_outgoing_messages_total - fibers_rpc_channel_dequeued_outgoing_messages_total`
    pub fn queue_len(&self) -> u64 {
        let dequeued_messages = self.dequeued_outgoing_messages();
        let enqueued_messages = self.enqueued_outgoing_messages();
        enqueued_messages.saturating_sub(dequeued_messages)
    }

    fn new(builder: &MetricBuilder, correction: Option<Arc<Self>>) -> Self {
        ChannelMetrics {
            fiber_yielded: builder
                .counter("fiber_yielded_total")
                .help("Number of `fibers::fiber::yield_poll()` function calls")
                .finish()
                .expect("Never fails"),
            async_outgoing_messages: builder
                .counter("async_outgoing_messages_total")
                .help("Number of asynchronous outgoing messages")
                .finish()
                .expect("Never fails"),
            async_incoming_messages: builder
                .counter("async_incoming_messages_total")
                .help("Number of asynchronous incoming messages")
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
            correction,
        }
    }
}
impl Drop for ChannelMetrics {
    fn drop(&mut self) {
        if let Some(ref c) = self.correction {
            c.fiber_yielded.add_u64(self.fiber_yielded());
            c.async_outgoing_messages
                .add_u64(self.async_outgoing_messages());
            c.async_incoming_messages
                .add_u64(self.async_incoming_messages());
            c.enqueued_outgoing_messages
                .add_u64(self.enqueued_outgoing_messages());
            c.dequeued_outgoing_messages
                .add_u64(self.enqueued_outgoing_messages()); // Considers all messages dequeued
        }
    }
}
