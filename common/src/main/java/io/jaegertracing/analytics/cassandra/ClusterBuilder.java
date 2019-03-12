package io.jaegertracing.analytics.cassandra;

import com.datastax.driver.core.Cluster;
import lombok.Builder;

@Builder
public class ClusterBuilder extends org.apache.flink.streaming.connectors.cassandra.ClusterBuilder {
    @Builder.Default private String contactPoints = "localhost";
    @Builder.Default private short port = 9042;
    private String username;
    private String password;

    @Override
    protected Cluster buildCluster(Cluster.Builder builder) {
        builder.addContactPoint(contactPoints)
                .withPort(port);
        if (username != null && password != null) {
            builder.withCredentials(username, password);
        }
        return builder.build();
    }
}
