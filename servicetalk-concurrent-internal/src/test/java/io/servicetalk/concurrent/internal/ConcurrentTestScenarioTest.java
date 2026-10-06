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
package io.servicetalk.concurrent.internal;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.servicetalk.concurrent.internal.DeliberateException.DELIBERATE_EXCEPTION;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ConcurrentTestScenarioTest {
    @Test
    void closeReleasesAndJoinsPausedActor() throws Exception {
        final AtomicBoolean finished = new AtomicBoolean();
        try (ConcurrentTestScenario scenario = new ConcurrentTestScenario(1)) {
            final ConcurrentTestScenario.Checkpoint checkpoint = scenario.checkpoint("worker entered");
            scenario.actor("worker", () -> {
                checkpoint.pause();
                finished.set(true);
            });
            checkpoint.awaitReached();
            assertThat(finished.get(), is(false));
        }
        assertThat(finished.get(), is(true));
    }

    @Test
    void closePropagatesActorFailure() {
        final ExecutionException failure = assertThrows(ExecutionException.class, () -> {
            try (ConcurrentTestScenario scenario = new ConcurrentTestScenario(1)) {
                scenario.actor("failing worker", () -> {
                    throw DELIBERATE_EXCEPTION;
                });
            }
        });
        assertThat(failure.getCause().getMessage(), containsString("failing worker"));
        assertThat(failure.getCause().getCause(), is(sameInstance(DELIBERATE_EXCEPTION)));
    }

    @Test
    void awaitUnreachedCheckpointFails() throws Exception {
        try (ConcurrentTestScenario scenario = new ConcurrentTestScenario(1, 1, MILLISECONDS)) {
            final AssertionError failure = assertThrows(AssertionError.class,
                    () -> scenario.checkpoint("missing event").awaitReached());
            assertThat(failure.getMessage(), containsString("missing event"));
        }
    }

    @Test
    void pauseWithoutReleaseFails() throws Exception {
        try (ConcurrentTestScenario scenario = new ConcurrentTestScenario(1, 1, MILLISECONDS)) {
            final AssertionError failure = assertThrows(AssertionError.class,
                    () -> scenario.checkpoint("missing release").pause());
            assertThat(failure.getMessage(), containsString("missing release"));
        }
    }

    @Test
    void pauseAtUsedCheckpointIsRejected() throws Exception {
        try (ConcurrentTestScenario scenario = new ConcurrentTestScenario(1)) {
            final ConcurrentTestScenario.Checkpoint checkpoint = scenario.checkpoint("one-shot");
            checkpoint.release();
            checkpoint.pause();
            assertThrows(IllegalStateException.class, checkpoint::pause);
        }
    }

    @Test
    void actorBeyondConfiguredCapacityIsRejected() throws Exception {
        try (ConcurrentTestScenario scenario = new ConcurrentTestScenario(1)) {
            final ConcurrentTestScenario.Checkpoint checkpoint = scenario.checkpoint("occupied slot");
            scenario.actor("first", checkpoint::pause);
            checkpoint.awaitReached();
            assertThrows(IllegalStateException.class, () -> scenario.actor("extra", () -> { }));
        }
    }

    @Test
    void closeCollectsFailuresFromAllActors() {
        final ConcurrentTestScenario scenario = new ConcurrentTestScenario(2);
        scenario.actor("first", () -> {
            throw DELIBERATE_EXCEPTION;
        });
        scenario.actor("second", () -> {
            throw DELIBERATE_EXCEPTION;
        });
        final ExecutionException failure = assertThrows(ExecutionException.class, scenario::close);
        assertThat(failure.getCause().getMessage(), containsString("first"));
        assertThat(failure.getSuppressed().length, is(1));
        assertThat(failure.getSuppressed()[0].getCause().getMessage(), containsString("second"));
    }

    @Test
    void actorPropagatesCheckedException() {
        final IOException expected = new IOException("controlled I/O failure");
        final ConcurrentTestScenario scenario = new ConcurrentTestScenario(1);
        scenario.actor("I/O", () -> {
            throw expected;
        });
        final ExecutionException failure = assertThrows(ExecutionException.class, scenario::close);
        assertThat(failure.getCause().getCause(), is(sameInstance(expected)));
    }

    @Test
    void invalidCapacityAndTimeoutAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new ConcurrentTestScenario(0));
        assertThrows(IllegalArgumentException.class, () -> new ConcurrentTestScenario(1, 0, MILLISECONDS));
    }

    @Test
    void registrationAfterCloseIsRejected() throws Exception {
        final ConcurrentTestScenario scenario = new ConcurrentTestScenario(1);
        scenario.close();
        assertThrows(IllegalStateException.class, () -> scenario.checkpoint("late"));
        assertThrows(IllegalStateException.class, () -> scenario.actor("late", () -> { }));
        scenario.close();
    }

    @Test
    void closePreservesFailureWhenAnotherActorWasCancelled() throws Exception {
        final ConcurrentTestScenario scenario = new ConcurrentTestScenario(2);
        final Future<?> failed = scenario.actor("failed", () -> {
            throw DELIBERATE_EXCEPTION;
        });
        assertThrows(ExecutionException.class, () -> scenario.awaitActor(failed));
        final ConcurrentTestScenario.Checkpoint paused = scenario.checkpoint("cancelled actor");
        final Future<?> cancelled = scenario.actor("cancelled", paused::pause);
        paused.awaitReached();
        assertThat(cancelled.cancel(true), is(true));
        final ExecutionException failure = assertThrows(ExecutionException.class, scenario::close);
        assertThat(failure.getCause().getCause(), is(sameInstance(DELIBERATE_EXCEPTION)));
        assertThat(failure.getSuppressed().length, is(1));
        assertThat(failure.getSuppressed()[0], instanceOf(CancellationException.class));
    }

    @Test
    void interruptedCheckpointPreservesInterrupt() throws Exception {
        try (ConcurrentTestScenario scenario = new ConcurrentTestScenario(1)) {
            try {
                Thread.currentThread().interrupt();
                final AssertionError failure = assertThrows(AssertionError.class,
                        () -> scenario.checkpoint("interrupted").awaitReached());
                assertThat(failure.getCause(), instanceOf(InterruptedException.class));
                assertThat(Thread.currentThread().isInterrupted(), is(true));
            } finally {
                Thread.interrupted();
            }
        }
    }

    @Test
    void arrivalDoesNotWaitForRelease() throws Exception {
        try (ConcurrentTestScenario scenario = new ConcurrentTestScenario(1)) {
            final ConcurrentTestScenario.Checkpoint checkpoint = scenario.checkpoint("nonblocking arrival");
            scenario.awaitActor(scenario.actor("callback", checkpoint::arrive));
            checkpoint.awaitReached();
            assertThrows(IllegalStateException.class, checkpoint::arrive);
        }
    }
}
