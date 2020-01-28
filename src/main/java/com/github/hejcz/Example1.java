package com.github.hejcz;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

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

public class Example1 {

    public static void main(String[] args) throws InterruptedException {
        AtomicInteger dropped = new AtomicInteger();

        AbstractRSocket handler = new AbstractRSocket() {
            @Override
            public Flux<Payload> requestStream(Payload payload) {
                return Flux.interval(Duration.ofMillis(10))
                        .map(it -> ByteBufPayload.create("hello from server"))
                        .onBackpressureDrop(p -> System.out.println(
                                "dropped " + dropped.incrementAndGet()));
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
                        s.request(10L);
                    }

                    @Override
                    public void onNext(Payload payload) {
                        System.out.println(payload.getDataUtf8());
                        try {
                            Thread.sleep(random.nextInt(20));
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
    }

}
