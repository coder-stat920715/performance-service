package com.enterprise.performance.dto;

import com.enterprise.performance.entity.Transaction;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;

public record TransactionRequest(
        @NotBlank @Size(max = 64) String reference,
        @NotBlank @Size(max = 64) String accountId,
        @NotNull @DecimalMin(value = "0.0001") @Digits(integer = 15, fraction = 4) BigDecimal amount,
        @NotBlank @Pattern(regexp = "^[A-Z]{3}$", message = "must be a 3-letter upper-case ISO currency code") String currency,
        @NotNull Transaction.Type type,
        @Size(max = 255) String description) {
}
