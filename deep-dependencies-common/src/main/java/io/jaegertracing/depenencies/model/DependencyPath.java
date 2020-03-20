package io.jaegertracing.depenencies.model;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.Transient;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
@Table(name = DependencyPath.TABLE_NAME)
public class DependencyPath {

  public static final String TABLE_NAME = "path_based_dependencies";
  public static final String COLUMN_TS_BUCKET = "ts_bucket";
  public static final String COLUMN_FOCAL_NODE = "focal_node";
  public static final String COLUMN_ATTRIBUTES = "attributes";
  public static final String COLUMN_PATH = "path";

  static final int MAX_KEY_SIZE = 64000;

  private static final long serialVersionUID = 0L;

  @Column(name = COLUMN_TS_BUCKET)
  public Date timeBucket;

  @Column(name = COLUMN_FOCAL_NODE)
  public ServiceOperation focalNode;

  @Column(name = COLUMN_ATTRIBUTES)
  public List<KeyValue> attributes;

  @Column(name = COLUMN_PATH)
  public List<ServiceOperation> path;

  @Transient
  public void setPath(List<ServiceOperation> path) {
    int size = 0;
    for (ServiceOperation serviceOperation : path) {
      size += serviceOperation.length();
    }
    if (size > MAX_KEY_SIZE) {
      log.error("Exceeded max column key size, actual size is: {}", size);
      this.path = Collections.singletonList(new ServiceOperation("Exceeded", "Length"));
      // TODO: Make this a constanst and emit metrics from Flink later in the pipeline
    } else {
      this.path = path;
    }
  }
}
