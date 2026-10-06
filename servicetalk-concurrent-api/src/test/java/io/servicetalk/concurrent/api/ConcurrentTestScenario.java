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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.servicetalk.concurrent.internal.TimeoutTracingInfoExtension.DEFAULT_TIMEOUT_SECONDS;
import static io.servicetalk.concurrent.test.internal.AwaitUtils.await;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Test-local POC for explicitly scheduled actors. The test thread owns registration and joins;
 * actors may reach checkpoints. Domain objects and invariants belong to the calling test.
 * This coordinates callback boundaries, not arbitrary instructions inside production code.
 */
final class ConcurrentTestScenario implements AutoCloseable {
    private final int actorCount;
    private final long timeout;
    private final TimeUnit unit;
    private final ExecutorService executor;
    private final List<Future<?>> actors = new ArrayList<>();
    private final List<Checkpoint> checkpoints = new ArrayList<>();
    private boolean closed;

    ConcurrentTestScenario(int actorCount) {
        this(actorCount, DEFAULT_TIMEOUT_SECONDS, SECONDS);
    }

    ConcurrentTestScenario(int actorCount, long timeout, TimeUnit unit) {
        if (actorCount <= 0 || timeout <= 0) {
            throw new IllegalArgumentException("Actor count and timeout must be positive");
        }
        this.actorCount = actorCount;
        this.timeout = timeout;
        this.unit = requireNonNull(unit);
        executor = newFixedThreadPool(actorCount);
    }

    Checkpoint checkpoint(String name) {
        checkOpen();
        final Checkpoint checkpoint = new Checkpoint(name, timeout, unit);
        checkpoints.add(checkpoint);
        return checkpoint;
    }

    Future<?> actor(String name, Runnable action) {
        checkOpen();
        requireNonNull(name);
        requireNonNull(action);
        if (actors.size() == actorCount) {
            throw new IllegalStateException("All actor slots are already registered");
        }
        final Future<?> actor = executor.submit(() -> {
            try {
                action.run();
            } catch (Throwable cause) {
                throw new AssertionError("Actor failed: " + name, cause);
            }
        });
        actors.add(actor);
        return actor;
    }

    void awaitActor(Future<?> actor) throws Exception {
        actor.get(timeout, unit);
    }

    void awaitActors() throws Exception {
        for (Future<?> actor : actors) {
            awaitActor(actor);
        }
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        for (Checkpoint checkpoint : checkpoints) {
            checkpoint.release();
        }
        try {
            awaitActors();
        } finally {
            executor.shutdownNow();
            assertThat("Scenario actors did not terminate", executor.awaitTermination(timeout, unit), is(true));
        }
    }

    private void checkOpen() {
        if (closed) {
            throw new IllegalStateException("Scenario is closed");
        }
    }

    /** A one-shot pause for one actor, with a separately observable arrival. */
    static final class Checkpoint {
        private final String name;
        private final long timeout;
        private final TimeUnit unit;
        private final AtomicBoolean entered = new AtomicBoolean();
        private final CountDownLatch reached = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        private Checkpoint(String name, long timeout, TimeUnit unit) {
            this.name = requireNonNull(name);
            this.timeout = timeout;
            this.unit = unit;
        }

        void pause() {
            if (!entered.compareAndSet(false, true)) {
                throw new IllegalStateException("Checkpoint already used: " + name);
            }
            reached.countDown();
            assertThat("Checkpoint was not released: " + name, await(released, timeout, unit), is(true));
        }

        void awaitReached() {
            assertThat("Checkpoint was not reached: " + name, await(reached, timeout, unit), is(true));
        }

        void release() {
            released.countDown();
        }
    }
}
