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

import com.vmlens.api.AllInterleavings;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Positive control for the vmlens pass (see {@code vmlensCheck} in {@code build.gradle}): the canonical
 * unsynchronized counter increment, which any working vmlens setup must flag as a data race in the report
 * produced by the {@code vmlensReport} task. A "Success" verdict for any other test in this source set is
 * only trustworthy if this control is flagged in the same run: vmlens silently reports success when a
 * scenario is too large for its loop threshold and the racing operations never reach the trace.
 */
final class VmlensPositiveControlTest {

    @Test
    void plainCounterIncrementAcrossThreadsExploresMultipleInterleavings() throws Exception {
        int iterations = 0;
        try (AllInterleavings all = new AllInterleavings("vmlens-positive-control")) {
            while (all.hasNext()) {
                iterations++;
                final Counter counter = new Counter();
                final Thread first = new Thread(counter::inc);
                final Thread second = new Thread(counter::inc);
                first.start();
                second.start();
                first.join();
                second.join();
            }
        }
        assertThat("vmlens must explore more than one interleaving for the control to be meaningful",
                iterations, greaterThan(1));
    }

    private static final class Counter {
        private int value;

        void inc() {
            value++;
        }
    }
}
