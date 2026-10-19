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
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

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

    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<SequentialSubscription, Subscription> pendingSwitchUpdater =
            newUpdater(SequentialSubscription.class, Subscription.class, "pendingSwitch");

    private Subscription subscription;
    /**
     * A {@link Subscription} handed over by a {@link #switchTo(Subscription)} call that overlapped with a switch which
     * is still in progress. The thread that is switching delivers the outstanding demand to it.
     */
    @Nullable
    private volatile Subscription pendingSwitch;
    private long sourceEmitted;
    private volatile long requested;
    private volatile long sourceRequested;

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
        final Subscription currSubscription = subscription;
        final long currSourceRequested = sourceRequestedUpdater.getAndSet(this, CANCELLED);
        // To avoid concurrent invocation with the switch thread we defer to that thread to cancel.
        if (currSourceRequested >= 0) {
            currSubscription.cancel();
        }
    }

    /**
     * Switches <strong>current</strong> {@link Subscription} to {@code next}. It is assumed the {@link Subscriber}
     * associated with the previous {@link Subscription} will no longer call {@link #itemReceived()}.
     * <p>
     * Only can be called in the {@link Subscriber} thread!
     * @param next {@link Subscription} that should now be <strong>current</strong>.
     */
    void switchTo(final Subscription next) {
        requireNonNull(next);
        for (;;) {
            final long currSourceRequested = sourceRequested;
            if (currSourceRequested == CANCELLED) {
                cancelOrRequest(next);
                return;
            } else if (currSourceRequested >= 0) {
                if (sourceRequestedUpdater.compareAndSet(this, currSourceRequested, SWITCHING)) {
                    switchLoop(next, currSourceRequested);
                    return;
                }
            } else {
                break; // SWITCHING or REQUESTED
            }
        }
        // A switch is in progress: either a re-entry from within request(n), or a resubscribe on another thread that
        // raced with a switch which is still unwinding. Publish next BEFORE looking at the state again, so that either
        // the thread that is switching observes it, or we observe that it finished and take over.
        pendingSwitch = next;
        for (;;) {
            final long currSourceRequested = sourceRequested;
            if (currSourceRequested == REQUESTED) {
                return; // the switching thread loops again and consumes pendingSwitch
            } else if (currSourceRequested == SWITCHING) {
                if (sourceRequestedUpdater.compareAndSet(this, SWITCHING, REQUESTED)) {
                    return;
                }
            } else if (currSourceRequested == CANCELLED) {
                final Subscription pending = pendingSwitchUpdater.getAndSet(this, null);
                if (pending != null) {
                    cancelOrRequest(pending);
                }
                return;
            } else if (sourceRequestedUpdater.compareAndSet(this, currSourceRequested, SWITCHING)) {
                switchLoop(null, currSourceRequested); // the previous switch finished, drain what was published
                return;
            }
        }
    }

    private void cancelOrRequest(final Subscription subscription) {
        final long currRequested = requested;
        if (currRequested >= 0) {
            subscription.cancel();
        } else { // invalid requestN is pending, deliver to each subscription.
            subscription.request(currRequested);
        }
    }

    /**
     * Runs on the single thread that moved {@link #sourceRequested} to {@link #SWITCHING}.
     *
     * @param first the subscription to switch to, or {@code null} to only drain {@link #pendingSwitch}.
     * @param delivered the value of {@link #sourceRequested} before it was set to {@link #SWITCHING}.
     */
    private void switchLoop(@Nullable Subscription first, long delivered) {
        Subscription next = first;
        // sourceEmitted is stable here because we are on the Subscriber thread. We want to request the difference
        // between total requested and what has been emitted from the new subscription.
        long effectiveSourceRequested = sourceEmitted;
        for (;;) {
            final Subscription pending = pendingSwitchUpdater.getAndSet(this, null);
            if (pending != null && pending != next) {
                // There is a more recent subscription, deliver demand to it instead.
                next = pending;
                effectiveSourceRequested = sourceEmitted;
            }
            if (next != null) {
                final long currRequested = requested;
                if (currRequested < 0) { // invalid requestN is pending.
                    sourceRequested = CANCELLED;
                    next.request(currRequested);
                    return;
                }
                // effectiveSourceRequested ...[delta]... requested
                final long delta = currRequested - effectiveSourceRequested;
                assert delta >= 0;
                if (delta != 0) {
                    // There maybe concurrency with the Subscription thread, or synchronous delivery of data from
                    // request(n). In these cases we want to avoid "double request" from requested, so we track how
                    // much we have already requested and decrement it on future loop iterations.
                    effectiveSourceRequested = currRequested;
                    next.request(delta);
                }
                if (pendingSwitch == null) {
                    // Make the subscription visible before restoring the state of sourceRequested. If the Subscription
                    // thread observes the sourceRequested change it will also observe the subscription change.
                    subscription = next;
                }
                // sourceRequested must be monotonically increasing (besides control values) to prevent the
                // Subscription thread from requesting from an old subscription.
                delivered = currRequested;
            }
            if (sourceRequestedUpdater.compareAndSet(this, SWITCHING, delivered)) {
                if (pendingSwitch == null) {
                    return;
                }
                // A switchTo published after we consumed. It either observes that we finished and takes over, or we do.
                final long cur = sourceRequested;
                if (cur >= 0 && sourceRequestedUpdater.compareAndSet(this, cur, SWITCHING)) {
                    delivered = cur;
                    continue;
                }
                return;
            }
            // The Subscription thread, or a concurrent switchTo, was active in the mean time.
            for (;;) {
                final long cur = sourceRequested;
                if (cur == CANCELLED) {
                    final Subscription latest = pendingSwitchUpdater.getAndSet(this, null);
                    final Subscription target = latest != null ? latest : next != null ? next : subscription;
                    cancelOrRequest(target);
                    return;
                }
                if (cur == REQUESTED && sourceRequestedUpdater.compareAndSet(this, REQUESTED, SWITCHING)) {
                    break;
                }
            }
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
