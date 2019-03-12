CREATE TABLE quality_metrics (
    service text,
    time_bucket timestamp,
    metric text,
    submetric text,
    count bigint,
    trace_id text,
    PRIMARY KEY (service, time_bucket, metric, submetric)
) WITH CLUSTERING ORDER BY (time_bucket ASC, metric ASC, submetric ASC);