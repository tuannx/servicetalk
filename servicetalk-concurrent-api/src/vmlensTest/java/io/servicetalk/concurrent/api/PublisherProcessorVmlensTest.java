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

import io.servicetalk.concurrent.PublisherSource.Processor;
import io.servicetalk.concurrent.PublisherSource.Subscriber;
import io.servicetalk.concurrent.PublisherSource.Subscription;

import com.vmlens.api.AllInterleavings;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;

/**
 * vmlens (happens-before data-race) scenario for {@link PublisherProcessor}: two producer threads emit
 * items while one of them also requests more demand from the downstream subscription. The processor's
 * shared state (the consumer/emitting/pending updaters, the signals-holder queue and its buffered
 * counter, and the plain fields documented as "visible via emitting") is exercised exactly along the
 * paths its lock discipline is supposed to protect. The race verdict lives in the report of the
 * {@code vmlensReport} task; this test asserts the deterministic behavioral invariant that every
 * emitted item is delivered once demand covers it.
 */
final class PublisherProcessorVmlensTest {

    @Test
    void concurrentEmissionAndRequestDeliversAllItems() throws Exception {
        int iterations = 0;
        try (AllInterleavings all = new AllInterleavings("publisher-processor-race")) {
            while (all.hasNext()) {
                iterations++;
                final Processor<Integer, Integer> processor = Processors.newPublisherProcessor(8);
                final List<Integer> received = Collections.synchronizedList(new ArrayList<>());
                final AtomicReference<Subscription> downstream = new AtomicReference<>();
                processor.onSubscribe(new Subscription() {
                    @Override
                    public void request(final long n) {
                    }

                    @Override
                    public void cancel() {
                    }
                });
                processor.subscribe(new Subscriber<Integer>() {
                    @Override
                    public void onSubscribe(final Subscription subscription) {
                        downstream.set(subscription);
                        subscription.request(2);
                    }

                    @Override
                    public void onNext(final Integer item) {
                        received.add(item);
                    }

                    @Override
                    public void onError(final Throwable t) {
                    }

                    @Override
                    public void onComplete() {
                    }
                });
                final Thread producer1 = new Thread(() -> {
                    processor.onNext(1);
                    processor.onNext(2);
                });
                final Thread producer2 = new Thread(() -> {
                    processor.onNext(3);
                    downstream.get().request(1);
                });
                producer1.start();
                producer2.start();
                producer1.join();
                producer2.join();
                assertThat("All emitted items must be delivered once demand covers them",
                        received, containsInAnyOrder(1, 2, 3));
            }
        }
        assertThat("vmlens must explore more than one interleaving", iterations, greaterThan(1));
    }
}
