/*
 * Copyright (c) 2018, The Jaeger Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.uber.jaeger.analytics.cassandra.sink;

import com.datastax.driver.core.Cluster;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.cassandra.CassandraPojoSink;

/**
 * A builder for {@link CassandraPojoSink}.
 *
 * @param <T> the type of the POJO that is being written.
 */

public class CassandraSinkBuilder<T> {
  private String contactPoint = "localhost";
  private String keyspace = "jaeger_v1_local";

  public static <T> CassandraSinkBuilder<T> builder() {
    return new CassandraSinkBuilder<>();
  }

  // Cassandra config
  public CassandraSinkBuilder<T> setContactPoint(String contactPoint) {
    this.contactPoint = contactPoint;
    return this;
  }

  public CassandraSinkBuilder<T> setKeyspace(String keyspace) {
    this.keyspace = keyspace;
    return this;
  }

  org.apache.flink.streaming.connectors.cassandra.ClusterBuilder getClusterBuilder() {
    return new ClusterBuilder(contactPoint);
  }

  public CassandraPojoSink<T> build(TypeInformation<T> typeInformation) {
    return new KeyspaceCassandraPojoSink<>(typeInformation.getTypeClass(), getClusterBuilder(), keyspace);
  }

  private static class ClusterBuilder extends org.apache.flink.streaming.connectors.cassandra.ClusterBuilder {
    private final String contactPoints;

    private ClusterBuilder(String contactPoints) {
      this.contactPoints = contactPoints;
    }

    @Override
    protected Cluster buildCluster(Cluster.Builder builder) {
      builder.addContactPoint(contactPoints);
      return builder.build();
    }

  }
}
