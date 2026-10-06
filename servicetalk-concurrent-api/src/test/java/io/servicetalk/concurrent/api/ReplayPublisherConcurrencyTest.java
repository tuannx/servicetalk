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

import io.servicetalk.concurrent.internal.ConcurrentTestScenario;
import io.servicetalk.concurrent.test.internal.TestPublisherSubscriber;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.servicetalk.concurrent.api.SourceAdapters.toSource;
import static io.servicetalk.concurrent.internal.DeliberateException.DELIBERATE_EXCEPTION;
import static java.time.Duration.ofNanos;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

final class ReplayPublisherConcurrencyTest {
    @ParameterizedTest(name = "{displayName} [{index}]: overlap={0}, onError={1}")
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void expirationPreservesItemsAddedAtCurrentTime(boolean overlap, boolean onError) throws Exception {
        final TestExecutor clock = new TestExecutor(0);
        try (ConcurrentTestScenario scenario = new ConcurrentTestScenario(2)) {
            final ConcurrentTestScenario.Checkpoint expiring = scenario.checkpoint("timer reads expiration time");
            final AtomicReference<Thread> timerThread = new AtomicReference<>();
            final AtomicBoolean pauseTimer = new AtomicBoolean(overlap);
            final Executor timer = new DelegatingExecutor(clock) {
                @Override
                public long currentTime(TimeUnit unit) {
                    // Eager expiration reads the clock while holding its queue lock.
                    if (Thread.currentThread() == timerThread.get() && pauseTimer.compareAndSet(true, false)) {
                        expiring.pause();
                    }
                    return super.currentTime(unit);
                }
            };
            final TestPublisher<Integer> source = new TestPublisher<>();
            final Publisher<Integer> replay = source.replay(
                    ReplayStrategies.<Integer>historyTtlBuilder(2, ofNanos(10), timer, false).build());
            final TestPublisherSubscriber<Integer> live = new TestPublisherSubscriber<>();
            toSource(replay).subscribe(live);
            live.awaitSubscription().request(Long.MAX_VALUE);
            source.onNext(1);
            clock.advanceTimeByNoExecuteTasks(10, NANOSECONDS);
            final Future<?> expiration = scenario.actor("expire old item", () -> {
                timerThread.set(Thread.currentThread());
                clock.executeScheduledTasks();
            });
            if (overlap) {
                expiring.awaitReached();
            } else {
                scenario.awaitActor(expiration);
            }
            scenario.awaitActor(scenario.actor("add fresh item", () -> source.onNext(2)));
            expiring.release();
            scenario.awaitActor(expiration);

            final TestPublisherSubscriber<Integer> recent = new TestPublisherSubscriber<>();
            toSource(replay).subscribe(recent);
            recent.awaitSubscription().request(Long.MAX_VALUE);
            clock.advanceTimeBy(10, NANOSECONDS);
            final TestPublisherSubscriber<Integer> expired = new TestPublisherSubscriber<>();
            toSource(replay).subscribe(expired);
            expired.awaitSubscription().request(Long.MAX_VALUE);
            assertThat(live.takeOnNext(2), contains(1, 2));
            assertThat(recent.takeOnNext(), is(2));
            if (onError) {
                source.onError(DELIBERATE_EXCEPTION);
                assertThat(live.awaitOnError(), is(DELIBERATE_EXCEPTION));
                assertThat(recent.awaitOnError(), is(DELIBERATE_EXCEPTION));
                assertThat(expired.awaitOnError(), is(DELIBERATE_EXCEPTION));
            } else {
                source.onComplete();
                live.awaitOnComplete();
                recent.awaitOnComplete();
                expired.awaitOnComplete();
            }
            assertThat(expired.pollAllOnNext(), is(empty()));
        } finally {
            clock.closeAsync().toFuture().get();
        }
    }
}
