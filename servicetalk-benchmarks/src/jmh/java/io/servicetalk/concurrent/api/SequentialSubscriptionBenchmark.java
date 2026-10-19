/*
 * Copyright © 2026 Apple Inc. and the ServiceTalk project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.servicetalk.concurrent.api;

import io.servicetalk.concurrent.PublisherSource.Subscription;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Measures the demand-accounting hot path of {@link SequentialSubscription}.
 *
 * <ul>
 * <li>{@link #requestN(RequestNState)}: steady-state {@code request(n)} throughput with outstanding demand.</li>
 * <li>{@link #switchToWithDemand(SwitchToState)}: single-threaded {@code switchTo} cost while demand is
 * outstanding, including delivering that demand to the new delegate.</li>
 * <li>{@link #overlappingSwitchTo(OverlappingSwitchState)}: concurrent {@code request(n)} and {@code switchTo}
 * from multiple threads. Exercises the {@code pendingSwitch} contention path: a {@code switchTo} that races with
 * an in-progress switch must still deliver the outstanding demand exactly once.</li>
 * </ul>
 *
 * The delegates are reused across invocations, so the measured path allocates nothing; the only per-invocation
 * costs are the demand accounting itself and the (unavoidable) delegate invocation that records it.
 *
 * <p>Measured 2026-10-19 on 2 vCPU (AMD EPYC 9D25), 7 GB RAM, Temurin JDK 21.0.12.1, JMH 1.37,
 * {@code -prof gc}, {@code @Fork(2)} with 3x5s warmup and 5x5s measurement:
 *
 * <pre>
 * Benchmark                                        Mode  Cnt          Score         Error  Units
 * SequentialSubscriptionBenchmark.requestN        thrpt   10  29004065.788 ± 1692706.586  ops/s
 * SequentialSubscriptionBenchmark.switchToWithDemand thrpt 10 12332636.429 ±  880389.319  ops/s
 * </pre>
 *
 * <p>Baseline ({@code main} @ {@code 04cb4afc7}, same params):
 *
 * <pre>
 * Benchmark                                        Mode  Cnt          Score         Error  Units
 * SequentialSubscriptionBenchmark.requestN        thrpt   10  25566137.126 ± 4143063.829  ops/s
 * SequentialSubscriptionBenchmark.switchToWithDemand thrpt 10 14756781.633 ±  362977.179  ops/s
 * </pre>
 *
 * <p>{@code requestN}: +13.4% vs baseline (error bars overlap, no hot-path regression).
 * {@code switchToWithDemand}: -16.4% vs baseline (statistically significant; cost of the
 * {@code pendingSwitch} mechanism on the resubscribe path). Allocation is ~0 B/op on both.
 */
@Fork(2)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
public class SequentialSubscriptionBenchmark {

    /**
     * Steady-state {@code request(n)} with outstanding demand.
     *
     * @param state the benchmark state.
     * @return the total demand delivered to the delegate, for JMH to consume.
     */
    @Benchmark
    public long requestN(final RequestNState state) {
        return state.requestOne();
    }

    /**
     * Single-threaded {@code switchTo} while demand is outstanding. Every invocation requests one more item and
     * then switches to the other delegate, so every invocation delivers fresh demand to the new delegate.
     *
     * @param state the benchmark state.
     * @return the total demand delivered to the delegate that was switched to, for JMH to consume.
     */
    @Benchmark
    public long switchToWithDemand(final SwitchToState state) {
        return state.requestOneAndSwitch();
    }

    /**
     * Concurrent {@code request(n)} and {@code switchTo} from multiple threads, exercising the
     * {@code pendingSwitch} contention path.
     *
     * @param state the benchmark state.
     * @return the total demand delivered to the delegate that was switched to, for JMH to consume.
     */
    @Benchmark
    @Threads(4)
    public long overlappingSwitchTo(final OverlappingSwitchState state) {
        return state.requestOneAndSwitch();
    }

    /**
     * State for {@link SequentialSubscriptionBenchmark#requestN(RequestNState)}.
     */
    @State(Scope.Benchmark)
    public static class RequestNState {
        private final RecordingSubscription delegate = new RecordingSubscription();
        private final SequentialSubscription subscription = new SequentialSubscription(delegate);

        /**
         * Pre-requests demand and fails the trial setup if the accounting does not add up before measuring.
         */
        @Setup
        public void setup() {
            final int primingRequests = 1000;
            for (int i = 0; i < primingRequests; i++) {
                subscription.request(1);
            }
            if (delegate.demand() != primingRequests) {
                throw new AssertionError("demand accounting diverged during setup: expected " + primingRequests +
                        " but was " + delegate.demand());
            }
        }

        long requestOne() {
            subscription.request(1);
            return delegate.demand();
        }
    }

    /**
     * State for {@link SequentialSubscriptionBenchmark#switchToWithDemand(SwitchToState)}.
     */
    @State(Scope.Benchmark)
    public static class SwitchToState {
        private final RecordingSubscription initial = new RecordingSubscription();
        private final RecordingSubscription first = new RecordingSubscription();
        private final RecordingSubscription second = new RecordingSubscription();
        private final SequentialSubscription subscription = new SequentialSubscription(initial);
        private boolean switchToSecond = true;

        /**
         * Pre-requests demand and primes the switch path, failing the trial setup if the priming demand is not
         * delivered to the new delegate. A dedicated initial delegate is used so the priming request(1024),
         * which is forwarded to the current delegate, does not pollute the measured delegates' totals.
         */
        @Setup
        public void setup() {
            final long outstanding = 1024;
            subscription.request(outstanding);
            subscription.switchTo(first);
            if (first.demand() != outstanding) {
                throw new AssertionError("priming switch did not deliver outstanding demand: expected " + outstanding +
                        " but was " + first.demand());
            }
        }

        long requestOneAndSwitch() {
            subscription.request(1);
            final RecordingSubscription next = switchToSecond ? second : first;
            switchToSecond = !switchToSecond;
            subscription.switchTo(next);
            return next.demand();
        }
    }

    /**
     * State for {@link SequentialSubscriptionBenchmark#overlappingSwitchTo(OverlappingSwitchState)}.
     */
    @State(Scope.Benchmark)
    public static class OverlappingSwitchState {
        private static final int DELEGATE_COUNT = 8;
        private final RecordingSubscription[] delegates = new RecordingSubscription[DELEGATE_COUNT];
        private final SequentialSubscription subscription = new SequentialSubscription();
        private final AtomicLong totalRequested = new AtomicLong();
        private final AtomicLong nextDelegateIndex = new AtomicLong();

        /**
         * Creates the delegate pool.
         */
        @Setup
        public void setup() {
            for (int i = 0; i < delegates.length; i++) {
                delegates[i] = new RecordingSubscription();
            }
        }

        long requestOneAndSwitch() {
            subscription.request(1);
            totalRequested.incrementAndGet();
            final int index = (int) (nextDelegateIndex.getAndIncrement() & (DELEGATE_COUNT - 1));
            final RecordingSubscription next = delegates[index];
            subscription.switchTo(next);
            return next.demand();
        }

        /**
         * Switches to a fresh delegate (single-threaded) and fails the trial if it does not receive exactly the
         * outstanding demand: no demand may be lost, even under overlapping switches.
         * <p>
         * Note: demand is intentionally <em>not</em> summed across all delegates. Every {@code switchTo}
         * legitimately re-delivers the full outstanding demand to the new current subscription (that is the
         * #1672 fix working as designed); superseded subscriptions are cancelled or terminated by every
         * production call site ({@code PublisherSwitchMap#switchWhenDisposed},
         * {@code RedoPublisher}/{@code RedoWhenPublisher} resubscribe only after termination), so summing
         * across them double-counts by design.
         */
        @TearDown(Level.Trial)
        public void verifyNoLostDemand() {
            final RecordingSubscription drain = new RecordingSubscription();
            subscription.switchTo(drain);
            final long requested = totalRequested.get();
            final long delivered = drain.demand();
            if (delivered != requested) {
                throw new AssertionError("lost demand under overlapping switchTo: requested " + requested +
                        " but delivered " + delivered);
            }
        }
    }

    /**
     * A {@link Subscription} that records the total demand requested from it.
     */
    private static final class RecordingSubscription implements Subscription {
        private final AtomicLong totalDemand = new AtomicLong();

        @Override
        public void request(final long n) {
            totalDemand.addAndGet(n);
        }

        @Override
        public void cancel() {
            // These benchmarks never cancel.
        }

        long demand() {
            return totalDemand.get();
        }
    }
}
