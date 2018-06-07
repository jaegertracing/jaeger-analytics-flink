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

import com.datastax.driver.mapping.MappingManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.cassandra.CassandraPojoSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

/**
 * A Cassandra sink that creates a session with a user defined keyspace.
 *
 * <p>This workaround exists because the Datastax driver's object mapping API can only read the
 * keyspace from a compile time annotation, in the version of the Cassandra driver packaged with
 * the Flink connector.
 *
 * @see <a href="https://datastax-oss.atlassian.net/browse/JAVA-1229">Datastax:JAVA-1229</a>
 */
public class KeyspaceCassandraPojoSink<T> extends CassandraPojoSink<T> {
  private final String keyspace;

  /**
   * Create a new Cassandra Sink.
   *
   * @param clazz    A class with datastax object mapping annotations.
   * @param keyspace The keyspace to write to.
   */
  KeyspaceCassandraPojoSink(Class<T> clazz,
                            ClusterBuilder builder,
                            String keyspace) {
    super(clazz, builder);
    this.keyspace = keyspace;
  }

  @Override
  public void open(Configuration configuration) {
    try {
      super.open(configuration);
    } catch (RuntimeException ignored) {
      // Exception is thrown by org.apache.flink.streaming.connectors.cassandra.CassandraSink
      // when trying to create a session without a keyspace.
      // This hack is so that we trigger
      // org.apache.flink.streaming.connectors.cassandra.CassandraSinkBase.open()
      // We can't directly inherit from CassandraSinkBase because the constructor is
      // package-private
    }
    this.session = cluster.connect(keyspace);
    // The following code is copied from
    // org.apache.flink.streaming.connectors.cassandra.CassandraSink
    try {
      this.mappingManager = new MappingManager(session);
      this.mapper = mappingManager.mapper(clazz);
    } catch (Exception e) {
      throw new RuntimeException(
          "Cannot create KeyspaceCassandraPojoSink with input: " + clazz.getSimpleName(), e);
    }
  }
}
