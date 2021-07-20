package com.sriram.kafka.streams.banktransactionschecker.model;

import lombok.Data;

@Data
public class BankTransferDTO {
    private String country;
    private Double amount;
}
