/*
 * Copyright © 2018-2026 Apple Inc. and the ServiceTalk project authors
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
import io.servicetalk.concurrent.internal.FlowControlUtils;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.annotation.Nullable;

import static io.servicetalk.concurrent.internal.EmptySubscriptions.EMPTY_SUBSCRIPTION_NO_THROW;
import static io.servicetalk.concurrent.internal.SubscriberUtils.isRequestNValid;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * A {@link Subscription} that delegates all {@link Subscription} calls to a <strong>current</strong>
 * {@link Subscription} instance which can be changed using {@link #switchTo(Subscription)}.
 *
 * <h2>Request-N</h2>
 * Between two {@link Subscription}s, any pending requested items, i.e. items requested via {@link #request(long)} and
 * not received via {@link #itemReceived()}, will be requested from the next {@link Subscription}.
 *
 * <h2>Cancel</h2>
 * If this {@link Subscription} is cancelled, then any other {@link Subscription} set via
 * {@link #switchTo(Subscription)} will be cancelled.
 */
final class SequentialSubscription implements Subscription {
    private static final long SWITCHING = -1;
    private static final long REQUESTED = -2;
    private static final long CANCELLED = -3;
    private static final AtomicLongFieldUpdater<SequentialSubscription> requestedUpdater =
            newUpdater(SequentialSubscription.class, "requested");
    private static final AtomicLongFieldUpdater<SequentialSubscription> sourceRequestedUpdater =
            newUpdater(SequentialSubscription.class, "sourceRequested");
    private static final AtomicReferenceFieldUpdater<SequentialSubscription, Subscription> pendingUpdater =
            AtomicReferenceFieldUpdater.newUpdater(SequentialSubscription.class, Subscription.class,
                    "pendingSubscription");

    private Subscription subscription;
    private long sourceEmitted;
    private volatile long requested;
    private volatile long sourceRequested;
    /**
     * The latest {@link Subscription} passed to {@link #switchTo(Subscription)} that the switch owner has not yet
     * picked up. Only the switch owner writes {@link #subscription}.
     */
    @Nullable
    private volatile Subscription pendingSubscription;

    /**
     * New instance.
     */
    SequentialSubscription() {
        this(EMPTY_SUBSCRIPTION_NO_THROW);
    }

    /**
     * New instance.
     *
     * @param delegate {@link Subscription} to use as <em>current</em>.
     */
    SequentialSubscription(Subscription delegate) {
        this.subscription = requireNonNull(delegate);
    }

    @Override
    public void request(long n) {
        final long currRequested;
        if (isRequestNValid(n)) {
            currRequested = requestedUpdater.accumulateAndGet(this, n,
                    FlowControlUtils::addWithOverflowProtectionIfNotNegative);
        } else {
            currRequested = sanitizeInvalidRequestN(n);
            requested = currRequested;
        }

        for (;;) {
            final long currSourceRequested = sourceRequested;
            if (currSourceRequested == CANCELLED) {
                break;
            } else if (currSourceRequested < 0) {
                assert currSourceRequested == SWITCHING || currSourceRequested == REQUESTED;
                if (sourceRequestedUpdater.compareAndSet(this, currSourceRequested, REQUESTED)) {
                    break;
                }
            } else {
                // We must read the subscription BEFORE the CAS (which involves a read barrier). This ensures if the
                // sourceRequested value is visible then the subscription (which may have been switched by
                // another thread) is also visible.
                final Subscription currSubscription = subscription;
                if (isRequestNValid(currRequested)) {
                    // sourceRequested ...[delta]... requested
                    final long delta = currRequested - currSourceRequested;
                    if (sourceRequestedUpdater.compareAndSet(this, currSourceRequested, currSourceRequested + delta)) {
                        // sourceRequested is either monotonically increasing, or set to an invalid value
                        // (e.g. negative) if a Subscription switch is on going and atomically set to requestN to
                        // preserve the monotonic increasing property. If the CAS worked that means the value of
                        // subscription before will be visible if there was previously a switch. We also know there is
                        // no concurrent interaction on the subscription because currSourceRequested is known not to be
                        // SWITCHING, and the value would have increased.
                        if (delta != 0) {
                            currSubscription.request(delta);
                        }
                        break;
                    }
                } else if (sourceRequestedUpdater.compareAndSet(this, currSourceRequested, CANCELLED)) {
                    currSubscription.request(currRequested);
                    break;
                }
            }
        }
    }

    @Override
    public void cancel() {
        final long currSourceRequested = sourceRequestedUpdater.getAndSet(this, CANCELLED);
        // To avoid concurrent invocation with the switch thread we defer to that thread to cancel.
        if (currSourceRequested >= 0) {
            // No switch was in progress, and none can start once cancelled. Reading the subscription after the
            // getAndSet observes the latest completed switch.
            subscription.cancel();
        }
    }

    /**
     * Switches <strong>current</strong> {@link Subscription} to {@code next}. It is assumed the {@link Subscriber}
     * associated with the previous {@link Subscription} will no longer call {@link #itemReceived()}.
     * <p>
     * Only can be called in the {@link Subscriber} thread! A request callback of the previous {@link Subscription} may
     * still be unwinding on another thread; that thread then completes the switch to the latest {@code next}.
     * @param next {@link Subscription} that should now be <strong>current</strong>.
     */
    void switchTo(Subscription next) {
        // Publishing next also publishes sourceEmitted to the thread that owns the switch.
        pendingSubscription = requireNonNull(next);
        for (;;) {
            final long currSourceRequested = sourceRequested;
            if (currSourceRequested == CANCELLED) {
                terminatePending();
                break;
            } else if (currSourceRequested < 0) {
                assert currSourceRequested == SWITCHING || currSourceRequested == REQUESTED;
                // A switch is in progress, either higher in this stack or on a thread still unwinding a request
                // callback. Mark pending work so the owner picks up next before it finishes.
                if (sourceRequestedUpdater.compareAndSet(this, currSourceRequested, REQUESTED)) {
                    break;
                }
            } else if (sourceRequestedUpdater.compareAndSet(this, currSourceRequested, SWITCHING)) {
                switchOwned(currSourceRequested);
                break;
            }
        }
    }

    /**
     * Delivers demand to the latest pending {@link Subscription} until no more work is pending.
     * <p>
     * Only the thread which set {@link #sourceRequested} to {@link #SWITCHING} calls this method.
     * @param prevSourceRequested the total requested from {@link #subscription} before the switch started.
     */
    private void switchOwned(long prevSourceRequested) {
        Subscription current = subscription;
        // How much has been requested from current, in terms of total requested.
        long effectiveSourceRequested = prevSourceRequested;
        for (;;) {
            final Subscription next = pendingUpdater.getAndSet(this, null);
            if (next != null) {
                // The new source starts where the previous one stopped emitting.
                current = next;
                effectiveSourceRequested = sourceEmitted;
            }
            final long currRequested = requested;
            if (currRequested < 0) { // invalid requestN is pending.
                subscription = current;
                sourceRequested = CANCELLED;
                current.request(currRequested);
                terminatePending();
                return;
            }

            // effectiveSourceRequested ...[delta]... requested
            final long delta = currRequested - effectiveSourceRequested;
            assert delta >= 0;
            if (delta != 0) {
                // Commit before the callback, so reentrant or concurrent calls only add pending work.
                effectiveSourceRequested = currRequested;
                current.request(delta);
            }
            // Make the subscription visible before restoring the state of sourceRequested. If the Subscription thread
            // observes the sourceRequested change it will also observe the subscription change.
            subscription = current;
            if (sourceRequestedUpdater.compareAndSet(this, SWITCHING, currRequested)) {
                return;
            }
            // request(n) or switchTo(next) marked pending work, or cancel() deferred the cancel to this thread.
            for (;;) {
                final long currSourceRequested = sourceRequested;
                if (currSourceRequested == CANCELLED) {
                    // Only the latest subscription is terminated; a replaced one is not cancelled.
                    final Subscription latest = pendingUpdater.getAndSet(this, null);
                    terminate(latest == null ? current : latest);
                    return;
                }
                assert currSourceRequested == REQUESTED;
                if (sourceRequestedUpdater.compareAndSet(this, REQUESTED, SWITCHING)) {
                    break;
                }
            }
        }
    }

    /**
     * Terminates the pending {@link Subscription}, if any, after {@link #sourceRequested} is {@link #CANCELLED}. The
     * atomic take ensures each pending {@link Subscription} is terminated once.
     */
    private void terminatePending() {
        final Subscription next = pendingUpdater.getAndSet(this, null);
        if (next != null) {
            terminate(next);
        }
    }

    private void terminate(Subscription target) {
        final long currRequested = requested;
        if (currRequested >= 0) {
            target.cancel();
        } else { // invalid requestN is pending, deliver to each subscription.
            target.request(currRequested);
        }
    }

    /**
     * Callback when an item is received by the associated {@link Subscriber}.
     * <p>
     * Only can be called in the {@link Subscriber} thread!
     */
    void itemReceived() {
        ++sourceEmitted;
        // There is no limit to how much we request from the current Subscription, so no need to check if we need to
        // request any more here.
    }

    private static long sanitizeInvalidRequestN(long n) {
        return n == 0 ? Long.MIN_VALUE : n;
    }
}
