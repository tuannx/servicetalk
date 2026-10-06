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
import io.servicetalk.concurrent.internal.JdiTestScenario;

import com.sun.jdi.BooleanValue;
import com.sun.jdi.LocalVariable;
import com.sun.jdi.Location;
import com.sun.jdi.ReferenceType;
import com.sun.jdi.VirtualMachine;
import com.sun.jdi.event.BreakpointEvent;
import com.sun.jdi.event.ClassPrepareEvent;
import com.sun.jdi.event.Event;
import com.sun.jdi.event.EventSet;
import com.sun.jdi.event.VMDeathEvent;
import com.sun.jdi.event.VMDisconnectEvent;
import com.sun.jdi.request.ClassPrepareRequest;
import com.sun.jdi.request.EventRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Experimental, explicitly enabled JDK 17 schedules; no production hooks or field writes. */
final class SequentialSubscriptionJdiTest {
    @ParameterizedTest(name = "{displayName} [{index}]: afterReentryCheck={0}, demand={1}")
    @CsvSource({"false,0", "true,0", "false,1", "true,1", "false,10", "true,10",
            "false,9223372036854775807", "true,9223372036854775807"})
    void switchToTransfersOutstandingDemand(boolean afterReentryCheck, long demand) throws Exception {
        run(handoffLine(afterReentryCheck), afterReentryCheck, demand);
    }

    @Test
    void missingBreakpointFailsAndTerminatesDebuggee() {
        final AssertionError failure = assertThrows(AssertionError.class, () -> run(-1, false, 10));
        assertThat(failure.getMessage(), containsString("Expected one executable handoff location"));
    }

    private static void run(int line, boolean afterReentryCheck, long demand) throws Exception {
        final String output = JdiTestScenario.run(Probe.class, Long.toString(demand), (scenario, vm) ->
                drive(scenario, vm, line, afterReentryCheck));
        assertThat(output, containsString("expected=" + demand + " actual=" + demand));
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

    private static void drive(JdiTestScenario scenario, VirtualMachine vm, int line, boolean afterReentryCheck)
            throws Exception {
        final ClassPrepareRequest prepare = vm.eventRequestManager().createClassPrepareRequest();
        prepare.addClassFilter("io.servicetalk.concurrent.api.*");
        prepare.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
        prepare.enable();
        EventSet owner = null;
        EventSet replacement = null;
        boolean releasedReplacement = false;
        boolean returned = false;
        for (;;) {
            final EventSet events = scenario.nextEvents();
            boolean hold = false;
            boolean exited = false;
            for (Event event : events) {
                if (event instanceof ClassPrepareEvent) {
                    final ReferenceType type = ((ClassPrepareEvent) event).referenceType();
                    if (type.name().equals(SequentialSubscription.class.getName())) {
                        final List<Location> locations = type.locationsOfLine(line);
                        assertThat("Expected one executable handoff location", locations, hasSize(1));
                        assertThat(locations.get(0).method().name(), is("switchTo"));
                        scenario.breakpoint(locations.get(0));
                    } else if (type.name().equals(Probe.class.getName())) {
                        scenario.breakpoint(type.methodsByName("beginReplacement").get(0).location());
                        scenario.breakpoint(type.methodsByName("replacementReturned").get(0).location());
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

    /** Runs in a fresh child JVM; the debugger changes scheduling only. */
    @SuppressWarnings("PMD.PublicMemberInNonPublicType") // The child JVM requires a public main entry point.
    public static final class Probe {
        private Probe() {
        }

        @SuppressWarnings("PMD.SystemPrintln") // Captured by the parent as the demand assertion diagnostic.
        public static void main(String[] args) throws Exception {
            final SequentialSubscription subscription = new SequentialSubscription();
            final long demand = Long.parseLong(args[0]);
            if (demand != 0) {
                subscription.request(demand);
            }
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
                System.out.println("expected=" + demand + " actual=" + next.requested.get());
                assertThat("Outstanding demand on replacement", next.requested.get(), is(demand));
                if (demand != Long.MAX_VALUE) {
                    subscription.request(1);
                    assertThat("Later demand reaches replacement", next.requested.get(), is(demand + 1));
                }
                subscription.cancel();
                assertThat("Cancellation reaches replacement", next.cancelled, is(true));
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
        private volatile boolean cancelled;

        @Override
        public void request(long n) {
            requested.addAndGet(n);
        }

        @Override
        public void cancel() {
            cancelled = true;
        }
    }
}
