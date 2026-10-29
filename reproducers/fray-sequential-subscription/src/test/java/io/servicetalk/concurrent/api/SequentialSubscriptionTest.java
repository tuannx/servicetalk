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

import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@ExtendWith(FrayTestExtension.class)
final class SequentialSubscriptionTest {
    @FrayTest(iterations = 10000)
    void switchToAfterSourceTerminationTransfersOutstandingDemand() throws Exception {
        SequentialSubscription subscription = new SequentialSubscription();
        CountDownLatch terminated = new CountDownLatch(1);
        AtomicBoolean emitted = new AtomicBoolean();
        AtomicLong received = new AtomicLong();
        Subscription source = new Subscription() {
            @Override
            public void request(long n) {
                if (emitted.compareAndSet(false, true)) {
                    subscription.itemReceived();
                    subscription.request(2);
                    terminated.countDown();
                }
            }

            @Override
            public void cancel() {
                // No resources to release.
            }
        };
        Subscription next = new Subscription() {
            @Override
            public void request(long n) {
                received.addAndGet(n);
            }

            @Override
            public void cancel() {
                // No resources to release.
            }
        };
        subscription.request(1);
        Thread switching = new Thread(() -> subscription.switchTo(source));
        Thread replacing = new Thread(() -> {
            try {
                terminated.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
            subscription.switchTo(next);
        });
        switching.start();
        replacing.start();
        switching.join();
        replacing.join();
        assertThat(received.get(), is(2L));
    }
}
