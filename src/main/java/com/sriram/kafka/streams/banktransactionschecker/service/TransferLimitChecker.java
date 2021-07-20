package com.sriram.kafka.streams.banktransactionschecker.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sriram.kafka.streams.banktransactionschecker.common.Constants;
import com.sriram.kafka.streams.banktransactionschecker.model.BankTransferDTO;
import com.sriram.kafka.streams.banktransactionschecker.model.CurrencyLimit;
import com.sriram.kafka.streams.banktransactionschecker.serde.JsonDeserializer;
import com.sriram.kafka.streams.banktransactionschecker.serde.JsonSerializer;
import com.sriram.kafka.streams.banktransactionschecker.serde.WrapperSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class TransferLimitChecker {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "transferLimitChecker");
        //properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        //properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, BankTransferDTO> source = builder.stream(Constants.INPUT_TOPIC, Consumed.with(Serdes.String(), new BankTransferDTOSerde()));
        KTable<String, CurrencyLimit> currencyLimitKTable = builder.table(Constants.CURRENCY_LIMIT_TOPIC, Consumed.with(Serdes.String(), new CurrencyLimitSerde()));


        KStream<String, BankTransferDTO> filteredTransactions = source.join(currencyLimitKTable, (transaction, currency) -> {
            if (transaction.getAmount() > currency.getLimit()) {
                return transaction;
            } else {
                return new BankTransferDTO();
            }

        });
        filteredTransactions.to(Constants.OUTPUT_TOPIC,  Produced.with(Serdes.String(), new BankTransferDTOSerde()));

        try (KafkaStreams streams = new KafkaStreams(builder.build(), properties)) {

            streams.cleanUp();
            streams.start();

            Thread.sleep(10000);

        }

    }


    static public final class BankTransferDTOSerde extends WrapperSerde<BankTransferDTO> {
        public  BankTransferDTOSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(BankTransferDTO.class));
        }
    }

    static public final class CurrencyLimitSerde extends WrapperSerde<CurrencyLimit> {
        public  CurrencyLimitSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(CurrencyLimit.class));
        }
    }
}

