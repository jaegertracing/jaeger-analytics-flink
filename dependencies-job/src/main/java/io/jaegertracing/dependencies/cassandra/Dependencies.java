package io.jaegertracing.dependencies.cassandra;

import com.datastax.driver.mapping.annotations.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

/**
 * DTO for cassandra.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "dependencies_v2")
public final class Dependencies implements Serializable {
  private static final long serialVersionUID = 0L;

  @Frozen
  @Column(name = "dependencies")
  private List<Dependency> dependencies;

  @Column(name = "ts")
  private Date ts;

  @Column(name = "ts_bucket")
  private Date tsBucket;
}
