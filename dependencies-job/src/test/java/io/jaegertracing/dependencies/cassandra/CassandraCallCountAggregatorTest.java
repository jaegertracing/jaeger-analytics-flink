package io.jaegertracing.dependencies.cassandra;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.tuple;


public class CassandraCallCountAggregatorTest {
    private static final Dependency DEP_1 = new Dependency("parent_1", "child_1", (long) 10);
    private static final Dependency DEP_2 = new Dependency("parent_2", "child_2", (long) 20);
    private CassandraCallCountAggregator aggregator = new CassandraCallCountAggregator();

    @Test
    public void testCreate() {
        Map<Dependency, Long> result = aggregator.createAccumulator();
        Assertions.assertThat(result).isEmpty();
    }

    @Test
    public void testAddNewDependency() {
        Map<Dependency, Long> acc = new HashMap<>();
        aggregator.add(DEP_1, acc);
        Assertions.assertThat(acc).hasSize(1).containsEntry(DEP_1, 10L);
    }

    @Test
    public void testAddExistingDependency() {
        Map<Dependency, Long> acc = new HashMap<>();
        acc.put(DEP_1, 10L);

        aggregator.add(DEP_1, acc);
        Assertions.assertThat(acc).hasSize(1).containsEntry(DEP_1, 20L);
    }

    @Test
    public void testMergeAccumulators() {
        Map<Dependency, Long> acc1 = new HashMap<>();
        acc1.put(DEP_1, 10L);

        Map<Dependency, Long> acc2 = new HashMap<>();
        acc2.put(DEP_1, 10L);
        acc2.put(DEP_2, 20L);

        Map<Dependency, Long> result = aggregator.merge(acc1, acc2);

        Assertions.assertThat(result).hasSize(2).containsEntry(DEP_1, 20L).containsEntry(DEP_2, 20L);
    }

    @Test
    public void testGetResult() {
        Map<Dependency, Long> acc = new HashMap<>();
        acc.put(DEP_1, 10L);
        acc.put(DEP_2, 15L);

        Dependencies result = aggregator.getResult(acc);
        Assertions.assertThat(result.getDependencies())
                .extracting(Dependency::getParent,
                        Dependency::getChild,
                        Dependency::getCallCount)
                .containsExactlyInAnyOrder(
                        tuple(DEP_1.getParent(), DEP_1.getChild(), 10L),
                        tuple(DEP_2.getParent(), DEP_2.getChild(), 15L));
    }
}
