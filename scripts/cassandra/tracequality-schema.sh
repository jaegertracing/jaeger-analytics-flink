#!/bin/bash

function usage {
    >&2 echo "Error: $1"
    >&2 echo "Usage: $0 {prod|test} [datacenter] | cqlsh"
    >&2 echo ""
    >&2 echo "Other parameters can be set via environment:"
    >&2 echo "  KEYSPACE - keyspace (default: jaeger_v1_$datacenter)"
    >&2 echo "  REPLICATION_FACTOR - replication factor for prod (default: 2)"
    exit 1
}

replication_factor=${REPLICATION_FACTOR:-3}

if [[ "$1" == "" ]]; then
    usage "missing environment"
elif [[ "$1" == "prod" ]]; then
    if [[ "$2" == "" ]]; then usage "missing datacenter"; fi
    datacenter=$2
    replication="{'class': 'NetworkTopologyStrategy', '$datacenter': '${replication_factor}' }"
elif [[ "$1" == "test" ]]; then
    datacenter=${2:-'test'}
    replication="{'class': 'SimpleStrategy', 'replication_factor': '1'}"
else
    usage "invalid environment $1, expecting prod or test"
fi

keyspace=${KEYSPACE:-"jaeger_tracequality_v1_${datacenter}"}

cat - <<EOF
CREATE KEYSPACE IF NOT EXISTS ${keyspace} WITH replication = ${replication};

CREATE TABLE ${keyspace}.quality_metrics (
    service text,
    time_bucket timestamp,
    metric text,
    submetric text,
    count bigint,
    trace_id text,
    PRIMARY KEY (service, time_bucket, metric, submetric)
) WITH CLUSTERING ORDER BY (time_bucket ASC, metric ASC, submetric ASC);
EOF
