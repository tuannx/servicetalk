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

import io.servicetalk.concurrent.PublisherSource.Subscriber;
import io.servicetalk.concurrent.PublisherSource.Subscription;
import io.servicetalk.concurrent.api.BufferStrategy.Accumulator;

import com.vmlens.api.AllInterleavings;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

/**
 * vmlens (happens-before data-race) scenario for {@link PublisherBuffer}, shaped after the race class
 * targeted by {@code PublisherBufferConcurrencyTest}: an item being accumulated racing with the
 * delivery of the next boundary. One thread delivers an item into the active accumulator while a
 * second thread delivers the next boundary; the operator's {@code State} hand-off protocol (the
 * {@code ADDING} marker and the next-accumulator holder) is what is supposed to keep the plain
 * accumulator state consistent between the two. The race verdict lives in the report of the
 * {@code vmlensReport} task; this test asserts the deterministic behavioral invariant that the item
 * is counted by exactly one accumulator, whichever one wins the race.
 */
final class PublisherBufferVmlensTest {

    @Test
    void itemRacingBoundaryDeliveryIsCountedExactlyOnce() throws Exception {
        int iterations = 0;
        try (AllInterleavings all = new AllInterleavings("publisher-buffer-race")) {
            while (all.hasNext()) {
                iterations++;
                final TestPublisher<Integer> source = new TestPublisher<>();
                final TestPublisher<Accumulator<Integer, Integer>> boundaries = new TestPublisher<>();
                final List<Integer> received = Collections.synchronizedList(new ArrayList<>());
                SourceAdapters.toSource(source.buffer(new BufferStrategy<Integer,
                        Accumulator<Integer, Integer>, Integer>() {
                    @Override
                    public Publisher<Accumulator<Integer, Integer>> boundaries() {
                        return boundaries;
                    }

                    @Override
                    public int bufferSizeHint() {
                        return 8;
                    }
                })).subscribe(new Subscriber<Integer>() {
                    @Override
                    public void onSubscribe(final Subscription subscription) {
                        subscription.request(Long.MAX_VALUE);
                    }

                    @Override
                    public void onNext(final Integer bufferResult) {
                        received.add(bufferResult);
                    }

                    @Override
                    public void onError(final Throwable t) {
                    }

                    @Override
                    public void onComplete() {
                    }
                });
                boundaries.onNext(new IntAccumulator());
                final Thread itemsThread = new Thread(() -> source.onNext(1));
                final Thread boundaryThread = new Thread(() -> boundaries.onNext(new IntAccumulator()));
                itemsThread.start();
                boundaryThread.start();
                itemsThread.join();
                boundaryThread.join();
                // Terminate the items source and deliver one last boundary so every accumulator is
                // finished and delivered before asserting (a terminal signal is only emitted once a
                // boundary completes, as in PublisherBufferConcurrencyTest).
                source.onComplete();
                boundaries.onNext(new IntAccumulator());
                int total = 0;
                for (Integer result : received) {
                    total += result;
                }
                assertThat("The item must be counted by exactly one accumulator", total, is(1));
            }
        }
        assertThat("vmlens must explore more than one interleaving", iterations, greaterThan(1));
    }

    private static final class IntAccumulator implements Accumulator<Integer, Integer> {
        private int sum;

        @Override
        public void accumulate(@Nullable final Integer item) {
            if (item != null) {
                sum += item;
            }
        }

        @Override
        public Integer finish() {
            return sum;
        }
    }
}
