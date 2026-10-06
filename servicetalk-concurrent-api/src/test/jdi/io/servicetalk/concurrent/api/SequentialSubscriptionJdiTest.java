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
import io.servicetalk.concurrent.internal.ConcurrentTestScenario;

import com.sun.jdi.BooleanValue;
import com.sun.jdi.Bootstrap;
import com.sun.jdi.LocalVariable;
import com.sun.jdi.Location;
import com.sun.jdi.ReferenceType;
import com.sun.jdi.VirtualMachine;
import com.sun.jdi.connect.Connector;
import com.sun.jdi.connect.LaunchingConnector;
import com.sun.jdi.event.BreakpointEvent;
import com.sun.jdi.event.ClassPrepareEvent;
import com.sun.jdi.event.Event;
import com.sun.jdi.event.EventSet;
import com.sun.jdi.event.VMDeathEvent;
import com.sun.jdi.event.VMDisconnectEvent;
import com.sun.jdi.request.BreakpointRequest;
import com.sun.jdi.request.ClassPrepareRequest;
import com.sun.jdi.request.EventRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.servicetalk.concurrent.internal.TimeoutTracingInfoExtension.DEFAULT_TIMEOUT_SECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Experimental, explicitly enabled JDK 17 schedules; no production hooks or field writes. */
final class SequentialSubscriptionJdiTest {
    @ParameterizedTest(name = "{displayName} [{index}]: afterReentryCheck={0}")
    @ValueSource(booleans = {false, true})
    void switchToTransfersOutstandingDemand(boolean afterReentryCheck) throws Exception {
        run(handoffLine(afterReentryCheck), afterReentryCheck);
    }

    @Test
    void missingBreakpointFailsAndTerminatesDebuggee() {
        final AssertionError failure = assertThrows(AssertionError.class, () -> run(-1, false));
        assertThat(failure.getMessage(), containsString("Expected one executable handoff location"));
    }

    private static void run(int line, boolean afterReentryCheck) throws Exception {
        final LaunchingConnector connector = Bootstrap.virtualMachineManager().defaultConnector();
        final Map<String, Connector.Argument> arguments = connector.defaultArguments();
        arguments.get("main").setValue(Probe.class.getName());
        arguments.get("home").setValue(System.getProperty("java.home"));
        arguments.get("options").setValue("-cp \"" + System.getProperty("jdi.poc.classpath") + "\"");
        final VirtualMachine vm = connector.launch(arguments);
        final Process process = vm.process();
        final AtomicReference<String> stdout = new AtomicReference<>();
        final AtomicReference<String> stderr = new AtomicReference<>();
        try (ConcurrentTestScenario readers = new ConcurrentTestScenario(2)) {
            try {
                readers.actor("debuggee stdout", () -> stdout.set(read(process.getInputStream())));
                readers.actor("debuggee stderr", () -> stderr.set(read(process.getErrorStream())));
                drive(vm, line, afterReentryCheck);
                assertThat("Debuggee did not exit", process.waitFor(DEFAULT_TIMEOUT_SECONDS, SECONDS), is(true));
                readers.awaitActors();
                final String output = stdout.get() + stderr.get();
                assertThat(output, containsString("expected=10 actual="));
                assertThat(output, process.exitValue(), is(0));
                assertThat(output, containsString("expected=10 actual=10"));
            } finally {
                // Destroy even a JVM suspended at a breakpoint; do not leave it behind on assertion/timeout.
                process.destroyForcibly();
                final boolean interrupted = Thread.interrupted();
                try {
                    assertThat("Debuggee cleanup timed out",
                            process.waitFor(DEFAULT_TIMEOUT_SECONDS, SECONDS), is(true));
                } finally {
                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    private static int handoffLine(boolean afterReentryCheck) throws IOException {
        final List<String> lines = Files.readAllLines(Paths.get(System.getProperty("jdi.poc.source")),
                StandardCharsets.UTF_8);
        final String check = "                final boolean reentry = beforeSubscription != subscription;";
        final String write = "                    subscription = next;";
        assertThat("Source changed: reentry anchor must be unique",
                lines.stream().filter(check::equals).count(), is(1L));
        assertThat("Source changed: handoff anchor must be unique",
                lines.stream().filter(write::equals).count(), is(1L));
        final int checkLine = lines.indexOf(check);
        final int writeLine = lines.indexOf(write);
        assertThat("Source changed: handoff must follow the reentry check", writeLine > checkLine, is(true));
        return (afterReentryCheck ? writeLine : checkLine) + 1;
    }

    private static void drive(VirtualMachine vm, int line, boolean afterReentryCheck) throws Exception {
        final ClassPrepareRequest prepare = vm.eventRequestManager().createClassPrepareRequest();
        prepare.addClassFilter("io.servicetalk.concurrent.api.*");
        prepare.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
        prepare.enable();
        final long deadline = System.nanoTime() + SECONDS.toNanos(DEFAULT_TIMEOUT_SECONDS);
        EventSet owner = null;
        EventSet replacement = null;
        boolean releasedReplacement = false;
        boolean returned = false;
        for (;;) {
            final long remaining = deadline - System.nanoTime();
            assertThat("JDI schedule timed out", remaining > 0, is(true));
            final EventSet events = vm.eventQueue().remove(Math.max(1, NANOSECONDS.toMillis(remaining)));
            assertThat("Missing JDI event before deadline", events != null, is(true));
            boolean hold = false;
            boolean exited = false;
            for (Event event : events) {
                if (event instanceof ClassPrepareEvent) {
                    final ReferenceType type = ((ClassPrepareEvent) event).referenceType();
                    if (type.name().equals(SequentialSubscription.class.getName())) {
                        final List<Location> locations = type.locationsOfLine(line);
                        assertThat("Expected one executable handoff location", locations, hasSize(1));
                        assertThat(locations.get(0).method().name(), is("switchTo"));
                        breakpoint(vm, locations.get(0));
                    } else if (type.name().equals(Probe.class.getName())) {
                        breakpoint(vm, type.methodsByName("beginReplacement").get(0).location());
                        breakpoint(vm, type.methodsByName("replacementReturned").get(0).location());
                    }
                } else if (event instanceof BreakpointEvent) {
                    final BreakpointEvent hit = (BreakpointEvent) event;
                    hit.request().disable();
                    final String method = hit.location().method().name();
                    if ("switchTo".equals(method)) {
                        assertThat(hit.thread().name(), is("switch-owner"));
                        if (afterReentryCheck) {
                            final LocalVariable reentry = hit.thread().frame(0).visibleVariableByName("reentry");
                            assertThat("Missing local variable debug information for reentry",
                                    reentry != null, is(true));
                            assertThat("Old owner must have observed no reentry",
                                    ((BooleanValue) hit.thread().frame(0).getValue(reentry)).value(), is(false));
                        }
                        owner = events;
                        hold = true;
                    } else if ("beginReplacement".equals(method)) {
                        replacement = events;
                        hold = true;
                    } else if ("replacementReturned".equals(method)) {
                        assertThat("Replacement was not released by the schedule", releasedReplacement, is(true));
                        assertThat("Owner was not paused", owner != null, is(true));
                        returned = true;
                        owner.resume();
                    } else {
                        throw new AssertionError("Unexpected breakpoint: " + hit.location());
                    }
                } else if (event instanceof VMDeathEvent || event instanceof VMDisconnectEvent) {
                    exited = true;
                }
            }
            if (owner != null && replacement != null && !releasedReplacement) {
                releasedReplacement = true;
                replacement.resume();
            }
            if (exited) {
                assertThat("Replacement must return while the old owner is paused", returned, is(true));
                return;
            }
            if (!hold) {
                events.resume();
            }
        }
    }

    private static void breakpoint(VirtualMachine vm, Location location) {
        final BreakpointRequest request = vm.eventRequestManager().createBreakpointRequest(location);
        request.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
        request.enable();
    }

    private static String read(InputStream stream) {
        try (InputStream input = stream) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException cause) {
            throw new UncheckedIOException(cause);
        }
    }

    /** Runs in a fresh child JVM; the debugger changes scheduling only. */
    @SuppressWarnings("PMD.PublicMemberInNonPublicType") // The child JVM requires a public main entry point.
    public static final class Probe {
        private Probe() {
        }

        @SuppressWarnings("PMD.SystemPrintln") // Captured by the parent as the demand assertion diagnostic.
        public static void main(String[] args) throws Exception {
            final SequentialSubscription subscription = new SequentialSubscription();
            subscription.request(10);
            final RecordingSubscription old = new RecordingSubscription();
            final RecordingSubscription next = new RecordingSubscription();
            try (ConcurrentTestScenario scenario = new ConcurrentTestScenario(1)) {
                scenario.actor("old switch owner", () -> {
                    Thread.currentThread().setName("switch-owner");
                    subscription.switchTo(old);
                });
                beginReplacement();
                subscription.switchTo(next);
                replacementReturned();
                scenario.awaitActors();
                System.out.println("expected=10 actual=" + next.requested.get());
                assertThat("Outstanding demand on replacement", next.requested.get(), is(10L));
            }
        }

        private static void beginReplacement() {
            // JDI entry breakpoint: hold this thread until the old owner reaches the selected production location.
        }

        private static void replacementReturned() {
            // JDI entry breakpoint: the switch returned; the old owner may now resume.
        }
    }

    private static final class RecordingSubscription implements Subscription {
        private final AtomicLong requested = new AtomicLong();

        @Override
        public void request(long n) {
            requested.addAndGet(n);
        }

        @Override
        public void cancel() {
        }
    }
}
