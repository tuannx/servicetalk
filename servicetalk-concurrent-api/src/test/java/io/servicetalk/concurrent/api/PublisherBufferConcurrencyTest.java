/*
 * Copyright © 2020-2026 Apple Inc. and the ServiceTalk project authors
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

import io.servicetalk.concurrent.api.BufferStrategy.Accumulator;
import io.servicetalk.concurrent.test.internal.TestPublisherSubscriber;
import io.servicetalk.context.api.ContextMap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import javax.annotation.Nullable;

import static io.servicetalk.concurrent.api.Completable.failed;
import static io.servicetalk.concurrent.api.ExecutorExtension.withCachedExecutor;
import static io.servicetalk.concurrent.api.SourceAdapters.toSource;
import static io.servicetalk.concurrent.internal.DeliberateException.DELIBERATE_EXCEPTION;
import static io.servicetalk.context.api.ContextMap.Key.newKey;
import static io.servicetalk.utils.internal.ThrowableUtils.throwException;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.UnaryOperator.identity;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;

class PublisherBufferConcurrencyTest {
    private static final String THREAD_NAME_PREFIX = "buffer-concurrency-test";
    private static final ContextMap.Key<Integer> CTX_KEY = newKey("foo", Integer.class);

    @RegisterExtension
    static final ExecutorExtension<Executor> EXEC = withCachedExecutor(THREAD_NAME_PREFIX).setClassLevel(true);

    @Test
    void largeRun() throws Exception {
        runTest(identity(), identity());
    }

    @Test
    void executorIsPreserved() throws Exception {
        final Executor executor = EXEC.executor();
        runTest(beforeBuffer -> beforeBuffer.publishOn(executor).beforeOnNext(__ ->
                        assertThat("Unexpected thread in onNext.", Thread.currentThread().getName(),
                                startsWith(THREAD_NAME_PREFIX)))
                        .beforeOnComplete(() ->
                                assertThat("Unexpected thread in onComplete.", Thread.currentThread().getName(),
                                        startsWith(THREAD_NAME_PREFIX))
                        ),
                afterBuffer -> afterBuffer);
    }

    @Test
    void contextIsPreserved() throws Exception {
        AsyncContext.put(CTX_KEY, 0);
        runTest(beforeBuffer -> beforeBuffer.beforeOnSubscribe(__ -> AsyncContext.put(CTX_KEY, 1)),
                afterBuffer -> afterBuffer.beforeOnNext(__ ->
                        assertThat("Unexpected value in context.", AsyncContext.get(CTX_KEY),
                                is(1)))
                        .beforeOnComplete(() ->
                            assertThat("Unexpected value in context.", AsyncContext.get(CTX_KEY),
                                    is(1))));
    }

    @Test
    void addingAndBoundaryEmission() throws Exception {
        TestPublisher<Integer> original = new TestPublisher<>();
        TestPublisher<Accumulator<Integer, Integer>> boundaries = new TestPublisher<>();
        TestPublisherSubscriber<Integer> subscriber = new TestPublisherSubscriber<>();
        CountDownLatch waitForBoundary = new CountDownLatch(1);
        CountDownLatch waitForAdd = new CountDownLatch(1);
        Accumulator<Integer, Integer> accumulator = new Accumulator<Integer, Integer>() {
            private int added;
            @Override
            public void accumulate(@Nullable final Integer integer) {
                waitForAdd.countDown();
                try {
                    waitForBoundary.await();
                    if (integer == null) {
                        return;
                    }
                    added = integer;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throwException(e);
                }
            }

            @Override
            public Integer finish() {
                return added;
            }
        };
        toSource(original.buffer(new BufferStrategy<Integer, Accumulator<Integer, Integer>, Integer>() {
            @Override
            public Publisher<Accumulator<Integer, Integer>> boundaries() {
                return boundaries;
            }

            @Override
            public int bufferSizeHint() {
                return 8;
            }
        })).subscribe(subscriber);
        subscriber.awaitSubscription().request(1);
        boundaries.onNext(accumulator); // initial boundary
        assertThat(subscriber.pollOnNext(10, MILLISECONDS), is(nullValue()));

        CountDownLatch waitForOnNextReturn = new CountDownLatch(1);
        EXEC.executor().submit(() -> original.onNext(1))
            .beforeFinally(waitForOnNextReturn::countDown).subscribe();
        waitForAdd.await();
        subscriber.awaitSubscription().request(1);
        boundaries.onNext(new SummingAccumulator());
        waitForBoundary.countDown();
        waitForOnNextReturn.await();

        boundaries.onNext(new SummingAccumulator()); // Last accumulator will be overwritten by add()
        assertThat("Unexpected result.", subscriber.takeOnNext(), is(1));

        original.onComplete();
        boundaries.onNext(new SummingAccumulator()); // Boundary has to complete for terminal to be emitted
        assertThat("Unexpected result.", subscriber.takeOnNext(), is(0)); // empty accumulator

        subscriber.awaitOnComplete();
    }

    @ParameterizedTest(name = "{displayName} [{index}]: fail={0}")
    @ValueSource(booleans = {false, true})
    void terminalDuringBoundaryDeliveryIsSignaledAfterDeliveryReturns(boolean fail) throws Exception {
        final TestPublisher<Integer> original = new TestPublisher<>();
        final TestPublisher<Accumulator<Integer, Integer>> boundaries = new TestPublisher<>();
        final TestPublisherSubscriber<Integer> subscriber = new TestPublisherSubscriber<>();
        final CountDownLatch deliveryStarted = new CountDownLatch(1);
        final CountDownLatch releaseDelivery = new CountDownLatch(1);
        final CountDownLatch boundaryReturned = new CountDownLatch(1);
        final AtomicBoolean firstDelivery = new AtomicBoolean(true);
        toSource(original.buffer(strategyOf(boundaries)).beforeOnNext(__ -> {
            if (firstDelivery.getAndSet(false)) {
                deliveryStarted.countDown();
                await(releaseDelivery);
            }
        })).subscribe(subscriber);
        subscriber.awaitSubscription().request(Long.MAX_VALUE);
        boundaries.onNext(new SummingAccumulator());
        original.onNext(1);

        // The next boundary closes the first buffer, and its delivery blocks inside onNext.
        EXEC.executor().submit(() -> boundaries.onNext(new SummingAccumulator()))
                .beforeFinally(boundaryReturned::countDown).subscribe();
        await(deliveryStarted);
        original.onNext(2);
        terminate(original, fail);
        releaseDelivery.countDown();
        await(boundaryReturned);

        // Every thread that signals is done, nothing is pending: assert the order that was actually observed.
        assertThat(subscriber.pollAllOnNext(), contains(1, 2));
        assertTerminal(subscriber, fail);
    }

    @ParameterizedTest(name = "{displayName} [{index}]: fail={0}")
    @ValueSource(booleans = {false, true})
    void terminalDuringBoundaryFinishIsSignaledAfterBufferIsDelivered(boolean fail) throws Exception {
        final TestPublisher<Integer> original = new TestPublisher<>();
        final TestPublisher<Accumulator<Integer, Integer>> boundaries = new TestPublisher<>();
        final TestPublisherSubscriber<Integer> subscriber = new TestPublisherSubscriber<>();
        final CountDownLatch finishStarted = new CountDownLatch(1);
        final CountDownLatch releaseFinish = new CountDownLatch(1);
        final CountDownLatch boundaryReturned = new CountDownLatch(1);
        toSource(original.buffer(strategyOf(boundaries))).subscribe(subscriber);
        subscriber.awaitSubscription().request(Long.MAX_VALUE);
        boundaries.onNext(new SummingAccumulator() {
            @Override
            public Integer finish() {
                finishStarted.countDown();
                await(releaseFinish);
                return super.finish();
            }
        });
        original.onNext(1);

        // The next boundary closes the first buffer, and the thread that emits it is blocked in finish().
        EXEC.executor().submit(() -> boundaries.onNext(new SummingAccumulator()))
                .beforeFinally(boundaryReturned::countDown).subscribe();
        await(finishStarted);
        original.onNext(2);
        terminate(original, fail);
        releaseFinish.countDown();
        await(boundaryReturned);

        // Every thread that signals is done, nothing is pending: assert the order that was actually observed.
        assertThat(subscriber.pollAllOnNext(), contains(1, 2));
        assertTerminal(subscriber, fail);
    }

    @ParameterizedTest(name = "{displayName} [{index}]: fail={0}")
    @ValueSource(booleans = {false, true})
    void terminalWithoutNewItemsDuringBoundaryDeliveryIsSignaledAfterDeliveryReturns(boolean fail) throws Exception {
        final TestPublisher<Integer> original = new TestPublisher<>();
        final TestPublisher<Accumulator<Integer, Integer>> boundaries = new TestPublisher<>();
        final TestPublisherSubscriber<Integer> subscriber = new TestPublisherSubscriber<>();
        final CountDownLatch deliveryStarted = new CountDownLatch(1);
        final CountDownLatch releaseDelivery = new CountDownLatch(1);
        final CountDownLatch boundaryReturned = new CountDownLatch(1);
        final AtomicBoolean firstDelivery = new AtomicBoolean(true);
        toSource(original.buffer(strategyOf(boundaries)).beforeOnNext(__ -> {
            if (firstDelivery.getAndSet(false)) {
                deliveryStarted.countDown();
                await(releaseDelivery);
            }
        })).subscribe(subscriber);
        subscriber.awaitSubscription().request(Long.MAX_VALUE);
        boundaries.onNext(new SummingAccumulator());
        original.onNext(1);

        EXEC.executor().submit(() -> boundaries.onNext(new SummingAccumulator()))
                .beforeFinally(boundaryReturned::countDown).subscribe();
        await(deliveryStarted);
        terminate(original, fail);
        releaseDelivery.countDown();
        await(boundaryReturned);

        assertThat(subscriber.pollAllOnNext(), contains(1));
        assertTerminal(subscriber, fail);
    }

    private static BufferStrategy<Integer, Accumulator<Integer, Integer>, Integer> strategyOf(
            final Publisher<Accumulator<Integer, Integer>> boundaries) {
        return new BufferStrategy<Integer, Accumulator<Integer, Integer>, Integer>() {
            @Override
            public Publisher<Accumulator<Integer, Integer>> boundaries() {
                return boundaries;
            }

            @Override
            public int bufferSizeHint() {
                return 8;
            }
        };
    }

    private static void terminate(final TestPublisher<Integer> original, final boolean fail) {
        if (fail) {
            original.onError(DELIBERATE_EXCEPTION);
        } else {
            original.onComplete();
        }
    }

    private static void assertTerminal(final TestPublisherSubscriber<Integer> subscriber, final boolean fail) {
        if (fail) {
            assertThat(subscriber.awaitOnError(), is(sameInstance(DELIBERATE_EXCEPTION)));
        } else {
            subscriber.awaitOnComplete();
        }
    }

    private static void await(final CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throwException(e);
        }
    }

    private void runTest(final UnaryOperator<Publisher<Integer>> beforeBuffer,
                         final UnaryOperator<Publisher<Iterable<Integer>>> afterBuffer) throws Exception {
        final int maxRange = 1000;
        final int repeatMax = 100;
        final Executor executor = EXEC.executor();
        Publisher<Integer> original = Publisher.range(0, maxRange)
                .repeatWhen(count -> count == repeatMax ? failed(DELIBERATE_EXCEPTION) :
                        executor.timer(ofMillis(1)));

        Publisher<Iterable<Integer>> buffered = beforeBuffer.apply(original)
                .buffer(BufferStrategies.forCountOrTime(maxRange / 20, ofMillis(500)));

        afterBuffer.apply(buffered).beforeOnNext(new Consumer<Iterable<Integer>>() {
            private int lastValue = -1;

            @Override
            public void accept(final Iterable<Integer> ints) {
                for (Integer anInt : ints) {
                    assertThat("Unexpected value", anInt, greaterThan(lastValue));
                    lastValue = anInt == (maxRange - 1) ? -1 : anInt;
                }
            }
        })
                .ignoreElements()
                .toFuture()
                .get();
    }

    private static class SummingAccumulator implements Accumulator<Integer, Integer> {
        private int sum;

        @Override
        public void accumulate(@Nullable final Integer item) {
            if (item == null) {
                return;
            }
            sum += item;
        }

        @Override
        public Integer finish() {
            return sum;
        }
    }
}
