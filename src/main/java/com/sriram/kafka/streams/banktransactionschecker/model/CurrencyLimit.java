package com.sriram.kafka.streams.banktransactionschecker.model;

import lombok.Data;

@Data
public class CurrencyLimit {
    private String country;
    private Double limit;
}
