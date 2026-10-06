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
package io.servicetalk.http.netty;

import io.servicetalk.buffer.api.Buffer;
import io.servicetalk.concurrent.api.Single;
import io.servicetalk.concurrent.api.TestPublisher;
import io.servicetalk.concurrent.api.TestSubscription;
import io.servicetalk.concurrent.internal.ConcurrentTestScenario;
import io.servicetalk.http.api.BlockingHttpClient;
import io.servicetalk.http.api.HttpRequest;
import io.servicetalk.http.api.HttpResponse;
import io.servicetalk.http.api.StreamingHttpResponse;
import io.servicetalk.transport.api.ServerContext;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.Future;
import java.util.stream.Stream;

import static io.servicetalk.buffer.netty.BufferAllocators.DEFAULT_ALLOCATOR;
import static io.servicetalk.concurrent.api.Single.succeeded;
import static io.servicetalk.concurrent.internal.DeliberateException.DELIBERATE_EXCEPTION;
import static io.servicetalk.http.api.HttpResponseStatus.OK;
import static io.servicetalk.http.netty.HttpProtocolConfigs.h1Default;
import static io.servicetalk.http.netty.HttpProtocolConfigs.h2Default;
import static io.servicetalk.transport.netty.internal.AddressUtils.localAddress;
import static io.servicetalk.transport.netty.internal.AddressUtils.serverHostAndPort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class NettyHttpServerConnectionDrainScenarioTest {
    private static Stream<Arguments> requestBodyTerminatesAfterControlledResponse() {
        return Stream.of(false, true).flatMap(fails -> Stream.of(false, true).flatMap(consume ->
                Stream.of(false, true).flatMap(payload -> Stream.of(false, true).map(h2 ->
                        Arguments.of(fails, consume, payload, h2)))));
    }

    @ParameterizedTest(name = "{displayName} [{index}]: fails={0}, consume={1}, payload={2}, h2={3}")
    @MethodSource
    void requestBodyTerminatesAfterControlledResponse(boolean responseFails, boolean consumeRequest,
                                                     boolean payload, boolean h2) throws Exception {
        try (ConcurrentTestScenario scenario = new ConcurrentTestScenario(1)) {
            final ConcurrentTestScenario.Checkpoint requestTerminated = scenario.checkpoint("request body terminated");
            final TestSubscription responseDemand = new TestSubscription();
            final TestPublisher<Buffer> responseBody = new TestPublisher.Builder<Buffer>()
                    .disableAutoOnSubscribe().build(subscriber -> {
                        subscriber.onSubscribe(responseDemand);
                        return subscriber;
                    });
            try (ServerContext server = HttpServers.forAddress(localAddress(0))
                    .protocols(h2 ? h2Default() : h1Default())
                    .listenStreamingAndAwait((ctx, request, factory) -> {
                        // Report arrival without waiting on a transport or service thread.
                        request.transformMessageBody(body -> body.afterFinally(requestTerminated::arrive));
                        final Single<StreamingHttpResponse> response = succeeded(
                                factory.ok().payloadBody(responseBody));
                        return consumeRequest ? request.messageBody().ignoreElements().concat(response) : response;
                    });
                 BlockingHttpClient client = HttpClients.forSingleAddress(serverHostAndPort(server))
                         .protocols(h2 ? h2Default() : h1Default()).buildBlocking()) {
                final Future<?> exchange = scenario.actor("client exchange", () -> {
                    final HttpRequest request = payload ? client.post("/")
                            .payloadBody(DEFAULT_ALLOCATOR.fromAscii("request payload")) : client.get("/");
                    if (responseFails) {
                        assertThrows(Exception.class, () -> client.request(request));
                    } else {
                        final HttpResponse response = client.request(request);
                        assertThat(response.status(), is(OK));
                        assertThat(response.payloadBody().toString(UTF_8), is("OK"));
                    }
                });
                responseDemand.awaitRequestN(1);
                responseBody.onNext(DEFAULT_ALLOCATOR.fromAscii("OK"));
                if (responseFails) {
                    responseBody.onError(DELIBERATE_EXCEPTION);
                } else {
                    responseBody.onComplete();
                }
                scenario.awaitActor(exchange);
                // Client completion alone is not proof that the server's afterFinally callback has returned.
                requestTerminated.awaitReached();
            }
        }
    }
}
