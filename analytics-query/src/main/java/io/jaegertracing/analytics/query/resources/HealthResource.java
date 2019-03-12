package io.jaegertracing.analytics.query.resources;

import com.codahale.metrics.annotation.Timed;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

@Path("/health")
public class HealthResource {

    @GET
    @Timed
    public String getHealth() {
        return "OK";
    }
}
