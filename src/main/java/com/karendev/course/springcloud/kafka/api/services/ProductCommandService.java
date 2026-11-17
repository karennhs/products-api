package com.karendev.course.springcloud.kafka.api.services;

import com.karendev.course.springcloud.kafka.api.models.Reply;
import com.karendev.course.springcloud.kafka.api.models.dto.ProductDto;

import java.time.Duration;

public interface ProductCommandService {

    Reply<?> sendCreateAndAwait(ProductDto dto, Duration timeout);

    Reply<?> sendReadAndAwait(Long id, Duration timeout);

    Reply<?> sendReadAllAndAwait(Duration timeout);

    Reply<?> sendUpdateAndAwait(Long id, ProductDto dto, Duration timeout);

    Reply<?> sendDeleteAndAwait(Long id, Duration timeout);

}
