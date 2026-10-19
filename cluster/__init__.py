"""SysLume-Cluster central aggregation package (DGX01).

First stage: a read-only central aggregator that fetches the three node
telemetry endpoints and the authoritative ``cluster-model`` controller
status, then combines them into one stable, JSON-serializable cluster
snapshot.

No storage, HTTP serving, dashboards, or services are introduced here;
those are deferred to later modules (see the Phase 18-19 design document).
"""

from cluster.aggregator import (
    ClusterAggregator,
    ClusterModelController,
    ControllerError,
    NodeSource,
    load_aggregator_config,
    read_node_token,
    AGGREGATOR_SCHEMA_VERSION,
    NODE_SCHEMAS,
)
from cluster.service_metrics import (
    collect_service_metrics,
    read_gateway_metrics,
    SERVICE_SCHEMA_VERSION,
    HEAVY_EXPECTED_MODELS,
)

__all__ = [
    "ClusterAggregator",
    "ClusterModelController",
    "ControllerError",
    "NodeSource",
    "load_aggregator_config",
    "read_node_token",
    "AGGREGATOR_SCHEMA_VERSION",
    "NODE_SCHEMAS",
    "collect_service_metrics",
    "read_gateway_metrics",
    "SERVICE_SCHEMA_VERSION",
    "HEAVY_EXPECTED_MODELS",
]
