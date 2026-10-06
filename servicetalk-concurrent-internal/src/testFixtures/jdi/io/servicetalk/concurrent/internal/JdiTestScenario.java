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

import com.sun.jdi.Bootstrap;
import com.sun.jdi.Location;
import com.sun.jdi.VirtualMachine;
import com.sun.jdi.connect.Connector;
import com.sun.jdi.connect.LaunchingConnector;
import com.sun.jdi.event.EventSet;
import com.sun.jdi.request.BreakpointRequest;
import com.sun.jdi.request.EventRequest;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static io.servicetalk.concurrent.internal.TimeoutTracingInfoExtension.DEFAULT_TIMEOUT_SECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Opt-in JDK 17 fixture. Owns a debuggee, output readers and an event deadline.
 * The caller owns the breakpoint schedule, held event sets and domain assertions.
 * This fixture provides no field writes, method invocation or exhaustive schedule exploration.
 */
public final class JdiTestScenario {
    private final VirtualMachine vm;
    private final long deadline = System.nanoTime() + SECONDS.toNanos(DEFAULT_TIMEOUT_SECONDS);

    private JdiTestScenario(VirtualMachine vm) {
        this.vm = vm;
    }

    public static String run(Class<?> mainClass, String arguments, Schedule schedule) throws Exception {
        final LaunchingConnector connector = Bootstrap.virtualMachineManager().defaultConnector();
        final Map<String, Connector.Argument> options = connector.defaultArguments();
        options.get("main").setValue(mainClass.getName() + " " + arguments);
        options.get("home").setValue(System.getProperty("java.home"));
        options.get("options").setValue("-cp \"" + System.getProperty("jdi.poc.classpath") + "\"");
        final VirtualMachine vm = connector.launch(options);
        final Process process = vm.process();
        final AtomicReference<String> stdout = new AtomicReference<>();
        final AtomicReference<String> stderr = new AtomicReference<>();
        try (ConcurrentTestScenario readers = new ConcurrentTestScenario(2)) {
            try {
                readers.actor("debuggee stdout", () -> stdout.set(read(process.getInputStream())));
                readers.actor("debuggee stderr", () -> stderr.set(read(process.getErrorStream())));
                schedule.drive(new JdiTestScenario(vm), vm);
                assertTrue(process.waitFor(DEFAULT_TIMEOUT_SECONDS, SECONDS), "Debuggee did not exit");
                readers.awaitActors();
                final String output = stdout.get() + stderr.get();
                assertTrue(process.exitValue() == 0, "Debuggee failed: " + output);
                return output;
            } finally {
                // A suspended JVM must be killed as well as an ordinarily running child.
                process.destroyForcibly();
                final boolean interrupted = Thread.interrupted();
                try {
                    assertTrue(process.waitFor(DEFAULT_TIMEOUT_SECONDS, SECONDS), "Debuggee cleanup timed out");
                } finally {
                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    public EventSet nextEvents() throws InterruptedException {
        final long remaining = deadline - System.nanoTime();
        assertTrue(remaining > 0, "JDI schedule timed out");
        final EventSet events = vm.eventQueue().remove(Math.max(1, NANOSECONDS.toMillis(remaining)));
        assertTrue(events != null, "Missing JDI event before deadline");
        return events;
    }

    public void breakpoint(Location location) {
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

    @FunctionalInterface
    public interface Schedule {
        void drive(JdiTestScenario scenario, VirtualMachine vm) throws Exception;
    }
}
