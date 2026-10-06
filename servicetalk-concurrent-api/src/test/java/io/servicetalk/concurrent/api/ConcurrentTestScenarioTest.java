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

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.servicetalk.concurrent.internal.DeliberateException.DELIBERATE_EXCEPTION;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
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
}
