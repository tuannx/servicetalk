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
import io.servicetalk.concurrent.internal.ConcurrentTestScenario;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

final class SequentialSubscriptionConcurrencyTest {
    @ParameterizedTest(name = "{displayName} [{index}]: overlap={0}, requestMore={1}")
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void switchToTransfersOutstandingDemandOnce(boolean overlap, boolean requestMore) throws Exception {
        final SequentialSubscription subscription = new SequentialSubscription();
        final RecordingSubscription next = new RecordingSubscription();
        subscription.request(10);
        try (ConcurrentTestScenario scenario = new ConcurrentTestScenario(2)) {
            final ConcurrentTestScenario.Checkpoint checkpoint = scenario.checkpoint("old request unwinding");
            final ControlledSubscription old = new ControlledSubscription(subscription, checkpoint, requestMore);
            final Future<?> switching = scenario.actor("old switch", () -> subscription.switchTo(old));
            checkpoint.awaitReached();
            if (!overlap) {
                checkpoint.release();
                scenario.awaitActor(switching);
            }
            final Future<?> replacement = scenario.actor("new switch", () -> subscription.switchTo(next));
            scenario.awaitActor(replacement);
            checkpoint.release();
            scenario.awaitActors();

            assertThat("Outstanding demand must be transferred exactly once", next.requested.get(), is(10L));
            subscription.request(1);
            assertThat("Later demand must reach the new subscription", next.requested.get(), is(11L));
        }
    }

    @ParameterizedTest(name = "{displayName} [{index}]: cancelBeforeSwitch={0}, requestMore={1}")
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void switchToPreservesCancellation(boolean cancelBeforeSwitch, boolean requestMore) throws Exception {
        final SequentialSubscription subscription = new SequentialSubscription();
        final RecordingSubscription next = new RecordingSubscription();
        subscription.request(10);
        try (ConcurrentTestScenario scenario = new ConcurrentTestScenario(2)) {
            final ConcurrentTestScenario.Checkpoint checkpoint = scenario.checkpoint("old request unwinding");
            final ControlledSubscription old = new ControlledSubscription(subscription, checkpoint, requestMore);
            scenario.actor("old switch", () -> subscription.switchTo(old));
            checkpoint.awaitReached();
            final Future<?> replacement = scenario.actor("cancel and switch", () -> {
                if (cancelBeforeSwitch) {
                    subscription.cancel();
                }
                subscription.switchTo(next);
                if (!cancelBeforeSwitch) {
                    subscription.cancel();
                }
            });
            scenario.awaitActor(replacement);
            checkpoint.release();
            scenario.awaitActors();

            assertThat(next.cancelled, is(true));
            final long requestedBefore = next.requested.get();
            subscription.request(1);
            assertThat("Request after cancel must be ignored", next.requested.get(), is(requestedBefore));
            final RecordingSubscription afterCancel = new RecordingSubscription();
            subscription.switchTo(afterCancel);
            assertThat(afterCancel.cancelled, is(true));
            assertThat(afterCancel.requested.get(), is(0L));
        }
    }

    private static final class RecordingSubscription implements Subscription {
        private final AtomicLong requested = new AtomicLong();
        private volatile boolean cancelled;

        @Override
        public void request(long n) {
            requested.addAndGet(n);
        }

        @Override
        public void cancel() {
            cancelled = true;
        }
    }

    private static final class ControlledSubscription implements Subscription {
        private final SequentialSubscription subscription;
        private final ConcurrentTestScenario.Checkpoint checkpoint;
        private final boolean requestMore;
        private boolean terminated;

        ControlledSubscription(SequentialSubscription subscription,
                               ConcurrentTestScenario.Checkpoint checkpoint, boolean requestMore) {
            this.subscription = subscription;
            this.checkpoint = checkpoint;
            this.requestMore = requestMore;
        }

        @Override
        public void request(long n) {
            if (terminated) {
                return;
            }
            if (requestMore) {
                for (long i = 0; i < n; ++i) {
                    subscription.itemReceived();
                }
                subscription.request(n);
            }
            terminated = true;
            checkpoint.pause();
        }

        @Override
        public void cancel() {
        }
    }
}
