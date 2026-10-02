package com.enterprise.performance.controller;

import com.enterprise.performance.dto.TransactionRequest;
import com.enterprise.performance.dto.TransactionResponse;
import com.enterprise.performance.service.TransactionService;
import jakarta.validation.Valid;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

@RestController
@RequestMapping("/api/v1/transactions")
public class TransactionController {

    private final TransactionService service;

    public TransactionController(TransactionService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<TransactionResponse> create(@Valid @RequestBody TransactionRequest request) {
        TransactionResponse response = service.create(request);
        URI location = ServletUriComponentsBuilder.fromCurrentRequest()
                .path("/{id}")
                .buildAndExpand(response.id())
                .toUri();
        return ResponseEntity.created(location).body(response);
    }

    @GetMapping("/{id}")
    public TransactionResponse get(@PathVariable UUID id) {
        return service.getById(id);
    }

    @GetMapping("/account/{accountId}")
    public CompletableFuture<List<TransactionResponse>> listByAccount(
            @PathVariable String accountId,
            @RequestParam(required = false) Instant before,
            @RequestParam(defaultValue = "50") int limit) {
        return service.listByAccount(accountId, before, limit);
    }
}
