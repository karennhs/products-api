package com.karendev.course.springcloud.kafka.api.messaging;

import com.karendev.course.springcloud.kafka.api.models.Reply;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ReplyInbox is a component that manages the registration and completion of
 * replies in a concurrent environment.
 * It uses a ConcurrentHashMap to store pending
 * CompletableFuture instances associated with correlation IDs.
 * When a reply is received, the corresponding CompletableFuture is completed, allowing any waiting threads to proceed.
 *
 */
@Component
public class ReplyInbox {

    private final ConcurrentHashMap<String, CompletableFuture<Reply<?>>> pending = new ConcurrentHashMap<>();

    public CompletableFuture<Reply<?>> register(String correlationId) {
        CompletableFuture<Reply<?>> future = new CompletableFuture<>();
        pending.put(correlationId, future);
        return future;
    }

    public void complete(String correlationId, Reply<?> reply) {

        if (correlationId == null) {
            throw new NullPointerException("correlationId can't be null");
        }

        CompletableFuture<Reply<?>> future = pending.remove(correlationId);
        if (future != null) {
            future.complete(reply);
        }
    }

}
