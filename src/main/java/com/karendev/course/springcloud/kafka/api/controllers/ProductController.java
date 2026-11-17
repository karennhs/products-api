package com.karendev.course.springcloud.kafka.api.controllers;

import com.karendev.course.springcloud.kafka.api.models.Reply;
import com.karendev.course.springcloud.kafka.api.models.dto.ProductDto;
import com.karendev.course.springcloud.kafka.api.services.ProductCommandService;
import jakarta.validation.Valid;
import org.jspecify.annotations.NonNull;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.Map;

@RestController
@RequestMapping("/products")
public class ProductController {
    private final ProductCommandService commandService;

    public ProductController(ProductCommandService commandService) {
        this.commandService = commandService;

    }

    @PostMapping
    public ResponseEntity<?> create(@Valid @RequestBody ProductDto dto) {
        return getResponseEntity(commandService.sendCreateAndAwait(dto, Duration.ofSeconds(5)));
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> getById(@PathVariable Long id) {

        return getResponseEntity(commandService.sendReadAndAwait(id, Duration.ofSeconds(5)));

    }

    @GetMapping
    public ResponseEntity<?> getAll() {

        return getResponseEntity(commandService.sendReadAllAndAwait(Duration.ofSeconds(5)));

    }

    @PutMapping("/{id}")
    public ResponseEntity<?> update(@PathVariable Long id, @Valid @RequestBody ProductDto dto) {
        return getResponseEntity(commandService.sendUpdateAndAwait(id, dto, Duration.ofSeconds(5)));

    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> delete(@PathVariable Long id) {
        Reply<?> reply = commandService.sendDeleteAndAwait(id, Duration.ofSeconds(5));
        return getResponseEntity(reply);
    }


    private static @NonNull ResponseEntity<?> getResponseEntity(Reply<?> reply) {
        if (reply.status().isSuccess()) {
            return ResponseEntity.ok(reply.body());
        }
        return ResponseEntity.badRequest().body(Map.of("error", reply.message()));
    }
}
