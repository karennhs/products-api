package com.karendev.course.springcloud.kafka.api.services;

import com.karendev.course.springcloud.kafka.api.messaging.ReplyInbox;
import com.karendev.course.springcloud.kafka.api.models.Command;
import com.karendev.course.springcloud.kafka.api.models.CommandType;
import com.karendev.course.springcloud.kafka.api.models.Reply;
import com.karendev.course.springcloud.kafka.api.models.dto.ProductDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class ProductCommandServiceImpl implements ProductCommandService {
    private final StreamBridge bridge;
    private final ReplyInbox replyInbox;
    private static final Logger logger = LoggerFactory.getLogger(ProductCommandServiceImpl.class);

    public ProductCommandServiceImpl(StreamBridge bridge, ReplyInbox replyInbox) {
        this.bridge = bridge;
        this.replyInbox = replyInbox;
    }

    @Override
    public Reply<?> sendCreateAndAwait(ProductDto dto, Duration timeout) {
        return sendAndWait(new Command<>(CommandType.CREATE, null, dto), timeout);

    }

    @Override
    public Reply<?> sendReadAndAwait(Long id, Duration timeout) {
        return sendAndWait(new Command<>(CommandType.READ, id, null), timeout);

    }

    @Override
    public Reply<?> sendReadAllAndAwait(Duration timeout) {
        return sendAndWait(new Command<>(CommandType.READ_ALL, null, null), timeout);
    }

    @Override
    public Reply<?> sendUpdateAndAwait(Long id, ProductDto dto, Duration timeout) {
        return sendAndWait(new Command<>(CommandType.UPDATE, id, dto), timeout);
    }

    @Override
    public Reply<?> sendDeleteAndAwait(Long id, Duration timeout) {
        return sendAndWait(new Command<>(CommandType.DELETE, id, null), timeout);
    }

    private Reply<?> sendAndWait(Command<ProductDto> cmd, Duration timeout) {
        String correlationId = UUID.randomUUID().toString();
        logger.info("Api Products Client {} product with correlationId {}", cmd.type(), correlationId);

        //CompletableFuture<Reply<?>> future = replyInbox.register(correlationId);

        var future = replyInbox.register(correlationId);
        var msg = MessageBuilder.withPayload(cmd).setHeader("correlationId", correlationId).build();

        boolean sent = this.bridge.send("commands-out-0", msg);

        if (!sent) {
            throw new IllegalStateException("No se pudo enviar el comando a kafka");
        }

        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("Timeout esperando respuesta de products-command desde Kafka", e);
        }
    }
}
