package io.jaegertracing.tracequality.model;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

/**
 * This is the DTO that is used to hold intermediate results for quality scores and to write to the
 * `quality_metrics` table.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "quality_metrics")
public class QualityScore implements Serializable {
    private static final long serialVersionUID = 0L;

    @Column(name = "time_bucket")
    private Date timeBucket;

    @Column(name = "service")
    private String service;

    @Column(name = "metric")
    private String metric;

    @Column(name = "submetric")
    private String submetric;

    @Column(name = "count")
    private Long count;

    @Column(name = "trace_id")
    private String traceId;

    public void increment() {
        this.count++;
    }
}
