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
import io.servicetalk.concurrent.internal.FlowControlUtils;

import com.vmlens.api.AllInterleavings;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

/**
 * vmlens (happens-before data-race) variants of the concurrency scenarios in {@link SequentialSubscriptionTest}.
 * These scenarios are the memory-model complement of {@code SequentialSubscriptionFrayTest}: Fray controls the
 * schedule at synchronization points and judges demand accounting, while vmlens judges whether the plain
 * (non-volatile) fields of {@link SequentialSubscription} are ever accessed without a happens-before edge.
 * The race verdict lives in the report of the {@code vmlensReport} task; these tests assert that the
 * interleaving exploration actually ran, and the positive control in {@link VmlensPositiveControlTest} is
 * what makes a "Success" report trustworthy. Scenarios are deliberately tiny: vmlens drops racing
 * operations from its trace when a scenario loops too much.
 */
final class SequentialSubscriptionVmlensTest {

    /**
     * The issue #1672 pattern at the memory level: one thread drives demand into an emitting subscription
     * whose request callback reentrantly delivers items and requests more, while a second thread performs
     * an overlapping {@code switchTo} (a resubscribe).
     */
    @Test
    void switchToWhileAnotherSwitchUnwindsExploresMultipleInterleavings() throws Exception {
        int iterations = 0;
        try (AllInterleavings all = new AllInterleavings("seqsub-switch-race")) {
            while (all.hasNext()) {
                iterations++;
                final SequentialSubscription subscription = new SequentialSubscription();
                final EmittingSubscription emitting = new EmittingSubscription(subscription, 4, 2);
                final CountingSubscription resubscribed = new CountingSubscription();
                subscription.switchTo(emitting);
                final Thread driver = new Thread(() -> subscription.request(4));
                final Thread resubscriber = new Thread(() -> subscription.switchTo(resubscribed));
                driver.start();
                resubscriber.start();
                driver.join();
                resubscriber.join();
            }
        }
        assertThat("vmlens must explore more than one interleaving", iterations, greaterThan(1));
    }

    /**
     * {@code itemReceived()} on the subscriber thread increments the plain {@code sourceEmitted} counter
     * while a concurrent {@code switchTo} on another thread reads it to compute the outstanding demand
     * for the next subscription. Two items delivered against three requested must leave exactly one unit
     * of demand for the next subscription whenever the accesses are happens-before ordered.
     */
    @Test
    void itemReceivedRacingSwitchToExploresMultipleInterleavings() throws Exception {
        int iterations = 0;
        try (AllInterleavings all = new AllInterleavings("seqsub-emitted-race")) {
            while (all.hasNext()) {
                iterations++;
                final SequentialSubscription subscription = new SequentialSubscription();
                final CountingSubscription first = new CountingSubscription();
                final CountingSubscription second = new CountingSubscription();
                subscription.switchTo(first);
                subscription.request(3);
                final Thread subscriberThread = new Thread(() -> {
                    subscription.itemReceived();
                    subscription.itemReceived();
                });
                final Thread switcher = new Thread(() -> subscription.switchTo(second));
                subscriberThread.start();
                switcher.start();
                subscriberThread.join();
                switcher.join();
            }
        }
        assertThat("vmlens must explore more than one interleaving", iterations, greaterThan(1));
    }

    static final class CountingSubscription implements Subscription {
        private final AtomicLong requestedReceived = new AtomicLong();

        @Override
        public void request(final long n) {
            requestedReceived.accumulateAndGet(n, FlowControlUtils::addWithOverflowProtection);
        }

        @Override
        public void cancel() {
        }

        long requestedReceived() {
            return requestedReceived.get();
        }
    }

    static final class EmittingSubscription implements Subscription {
        private final SequentialSubscription subscription;
        private final int totalItems;
        private final int requestMoreEvery;
        private long demand;
        private int emitted;
        private boolean emitting;

        EmittingSubscription(final SequentialSubscription subscription, final int totalItems,
                             final int requestMoreEvery) {
            this.subscription = subscription;
            this.totalItems = totalItems;
            this.requestMoreEvery = requestMoreEvery;
        }

        @Override
        public void request(final long n) {
            demand += n;
            if (emitting) {
                return;
            }
            emitting = true;
            try {
                while (demand > 0 && emitted < totalItems) {
                    --demand;
                    ++emitted;
                    subscription.itemReceived();
                    if (emitted % requestMoreEvery == 0) {
                        subscription.request(requestMoreEvery);
                    }
                }
            } finally {
                emitting = false;
            }
        }

        @Override
        public void cancel() {
        }
    }
}
