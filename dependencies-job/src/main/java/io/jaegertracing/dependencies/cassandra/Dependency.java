package io.jaegertracing.dependencies.cassandra;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@UDT(name = "dependency")
public class Dependency {
    private static final long serialVersionUID = 0L;

    public Dependency(String parent, String child, long callCount){
        this.parent = parent;
        this.child = child;
        this.callCount = callCount;
        this.source = "jaeger";
    }

    @Field(name = "parent")
    String parent;

    @Field(name = "child")
    String child;

    @Field(name = "call_count")
    Long callCount;

    @Field(name = "source")
    String source;
}
