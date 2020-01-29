package com.github.hejcz;

import java.time.Duration;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.ByteBufPayload;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

interface Message {
    String name();
}

class ImportantMessage implements Message {

    @Override
    public String name() {
        return "important";
    }
}

class NotImportantMessage implements Message {

    @Override
    public String name() {
        return "not-important";
    }
}

/**
 * This example emits important and not important messages. It drops not
 * important messages on backpressure and stores dropped important messages in
 * intermediate collection. Then it publishes dropped important messages
 * with fixed interval.
 */
public class Example1 {

    public static void main(String[] args) throws InterruptedException {
        ScheduledExecutorService es = Executors.newScheduledThreadPool(1);
        AbstractRSocket handler = new AbstractRSocket() {
            AtomicBoolean skipNotImportant = new AtomicBoolean(false);
            Random random = new Random();
            Set<Message> droppedImportant = new HashSet<>();

            @Override
            public Flux<Payload> requestStream(Payload payload) {
                return Flux.interval(Duration.ofMillis(5))
                    .map(
                        it -> random.nextInt(10) < 8
                            ? new NotImportantMessage()
                            : new ImportantMessage())
                    .filter(
                        it -> !skipNotImportant.get()
                            || isImportant(it))
                    .mergeWith(
                        Flux.interval(Duration.ofSeconds(1))
                            .switchMap(ignored -> getPublisher()))
                    .onBackpressureDrop(m -> {
                        if (isImportant(m)) {
                            droppedImportant.add(m);
                        }
                        if (!skipNotImportant.getAndSet(true)) {
                            es.schedule(() -> skipNotImportant.set(false),
                                500, TimeUnit.MILLISECONDS);
                        }
                    })
                    .map(it -> ByteBufPayload.create(it.name()));
            }

            private Publisher<? extends Message> getPublisher() {
                if (droppedImportant.isEmpty()) {
                    return Flux.empty();
                }
                System.out.println("REWIND: " + droppedImportant.size());
                HashSet<Message> dropped = new HashSet<>(droppedImportant);
                droppedImportant.removeAll(dropped);
                return Flux.fromIterable(dropped);
            }

            private boolean isImportant(Message m) {
                return m.name().equals("important");
            }
        };

        RSocketFactory.receive()
                .acceptor((setup, sendingSocket) -> Mono.just(handler))
                .transport(TcpServerTransport.create(8080))
                .start().block();

        RSocket client = RSocketFactory.connect()
                .transport(TcpClientTransport.create(8080))
                .start().block();

        if (client == null) {
            System.out.println("Client is null");
            return;
        }

        client.requestStream(ByteBufPayload.create("hello"))
                .subscribe(new Subscriber<Payload>() {
                    private final Random random = new Random();
                    private Subscription s;

                    @Override
                    public void onSubscribe(Subscription s) {
                        this.s = s;
                        s.request(200L);
                    }

                    @Override
                    public void onNext(Payload payload) {
                        System.out.println(payload.getDataUtf8());
                        try {
                            Thread.sleep(random.nextInt(30));
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        s.request(1L);
                    }

                    @Override
                    public void onError(Throwable t) {

                    }

                    @Override
                    public void onComplete() {

                    }
                });

        Thread.sleep(10_000);
        es.shutdownNow();
        es.awaitTermination(10, TimeUnit.SECONDS);
    }

}
