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

import io.servicetalk.concurrent.internal.JdiTestScenario;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static io.servicetalk.concurrent.internal.DeliberateException.DELIBERATE_EXCEPTION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class JdiTestScenarioTest {
    @Test
    void runCapturesBothOutputStreams() throws Exception {
        final String output = JdiTestScenario.run(Probe.class, "success", (scenario, vm) -> vm.resume());
        assertThat(output, containsString("stdout marker"));
        assertThat(output, containsString("stderr marker"));
    }

    @Test
    void runPropagatesChildAssertion() {
        final AssertionError failure = assertThrows(AssertionError.class,
                () -> JdiTestScenario.run(Probe.class, "fail", (scenario, vm) -> vm.resume()));
        assertThat(failure.getMessage(), containsString("child assertion"));
    }

    @Test
    void runTerminatesSuspendedChildWhenScheduleFails() {
        final AtomicReference<Process> process = new AtomicReference<>();
        final Exception failure = assertThrows(Exception.class, () ->
                JdiTestScenario.run(Probe.class, "success", (scenario, vm) -> {
                    process.set(vm.process());
                    throw DELIBERATE_EXCEPTION;
                }));
        assertThat(failure, is(sameInstance(DELIBERATE_EXCEPTION)));
        assertThat(process.get().isAlive(), is(false));
    }

    @Test
    void runTerminatesSuspendedChildAndPreservesInterrupt() {
        final AtomicReference<Process> process = new AtomicReference<>();
        try {
            assertThrows(InterruptedException.class, () ->
                    JdiTestScenario.run(Probe.class, "success", (scenario, vm) -> {
                process.set(vm.process());
                Thread.currentThread().interrupt();
                throw new InterruptedException("interrupted schedule");
            }));
            assertThat(Thread.currentThread().isInterrupted(), is(true));
            assertThat(process.get().isAlive(), is(false));
        } finally {
            Thread.interrupted();
        }
    }

    @SuppressWarnings("PMD.PublicMemberInNonPublicType") // JVM entry point for isolated lifecycle tests.
    public static final class Probe {
        private Probe() {
        }

        @SuppressWarnings("PMD.SystemPrintln") // Parent verifies that both pipes are drained.
        public static void main(String[] args) {
            if ("fail".equals(args[0])) {
                throw new AssertionError("child assertion");
            }
            System.out.println("stdout marker");
            System.err.println("stderr marker");
        }
    }
}
