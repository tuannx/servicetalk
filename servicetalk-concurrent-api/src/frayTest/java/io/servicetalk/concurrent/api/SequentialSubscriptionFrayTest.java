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

import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.min;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Fray controlled-scheduling variants of the concurrency scenarios in {@link SequentialSubscriptionTest}.
 * Fray systematically explores thread interleavings instead of relying on lucky timing, so a lost-demand
 * interleaving that a stress loop only hits occasionally should be found deterministically.
 */
@ExtendWith(FrayTestExtension.class)
final class SequentialSubscriptionFrayTest {
    private static final int EMIT_BATCH = 10;

    /**
     * The issue #1672 scenario: a resubscribe (e.g. from a timer thread) overlaps a {@code switchTo} that is
     * still unwinding on another thread. Outstanding demand must be requested from the newest subscription.
     */
    @FrayTest(iterations = 1000)
    void switchToWhileAnotherSwitchUnwindsRequestsOutstandingDemand() throws Exception {
        final SequentialSubscription subscription = new SequentialSubscription();
        subscription.switchTo(new CountingSubscription());
        // The previous source terminated while EMIT_BATCH was outstanding.
        subscription.request(EMIT_BATCH);
        final EmittingSubscription source = new EmittingSubscription(subscription, 2 * EMIT_BATCH, EMIT_BATCH);
        final CountingSubscription resubscribed = new CountingSubscription();
        final Thread resubscriber = new Thread(() -> {
            while (!source.isTerminated()) {
                Thread.yield();
            }
            subscription.switchTo(resubscribed);
        });
        resubscriber.start();
        subscription.switchTo(source);
        resubscriber.join();
        assertThat("Demand that is outstanding after the last switch must be requested from the new subscription",
                resubscribed.requestedReceived(), is((long) EMIT_BATCH));
    }

    /**
     * Concurrent request(n) from one thread while another thread keeps switching subscriptions and delivering
     * items: every requested item must eventually be accounted as received.
     */
    @FrayTest(iterations = 50)
    void concurrentRequestAndSwitchDeliversAllItems() throws Exception {
        final int totalItems = 50;
        final int maxDeliveryPerSubscription = 5;
        final SequentialSubscription subscription = new SequentialSubscription();
        final CyclicBarrier allStarted = new CyclicBarrier(3);
        final AtomicLong totalSent = new AtomicLong();
        final Thread requester = new Thread(() -> {
            await(allStarted);
            for (int i = 0; i < totalItems; ++i) {
                subscription.request(1);
            }
        });
        final Thread sender = new Thread(() -> {
            await(allStarted);
            CountingSubscription current = new CountingSubscription();
            subscription.switchTo(current);
            int guard = 0;
            while (totalSent.get() < totalItems && guard++ < 200_000) {
                final long reqRecvd = current.requestedReceived();
                if (reqRecvd != 0) {
                    final long deliver = min(reqRecvd, maxDeliveryPerSubscription);
                    for (long i = 0; i < deliver && totalSent.get() < totalItems; ++i) {
                        subscription.itemReceived();
                        totalSent.incrementAndGet();
                    }
                }
                current = new CountingSubscription();
                subscription.switchTo(current);
                if (reqRecvd == 0) {
                    Thread.yield();
                }
            }
        });
        requester.start();
        sender.start();
        allStarted.await();
        requester.join();
        sender.join();
        assertThat("Unexpected itemReceived.", totalSent.get(), is((long) totalItems));
    }

    private static void await(final CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static final class CountingSubscription implements Subscription {
        private final AtomicLong requestedReceived = new AtomicLong();

        @Override
        public void request(final long n) {
            if (n <= 0) {
                requestedReceived.set(Long.MAX_VALUE);
                throw new AssertionError("Invalid request(n): " + n);
            }
            requestedReceived.accumulateAndGet(n, FlowControlUtils::addWithOverflowProtection);
        }

        @Override
        public void cancel() {
            requestedReceived.set(Long.MAX_VALUE);
        }

        long requestedReceived() {
            return requestedReceived.get();
        }
    }

    private static final class EmittingSubscription implements Subscription {
        private final SequentialSubscription subscription;
        private final int totalItems;
        private final int requestMoreEvery;
        private long demand;
        private int emitted;
        private boolean emitting;
        private volatile boolean terminated;

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
            if (emitted == totalItems) {
                terminated = true;
            }
        }

        @Override
        public void cancel() {
            terminated = true;
        }

        boolean isTerminated() {
            return terminated;
        }
    }
}
