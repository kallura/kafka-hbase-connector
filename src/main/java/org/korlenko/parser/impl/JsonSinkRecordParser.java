package org.korlenko.parser.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.connect.sink.SinkRecord;
import org.korlenko.parser.SinkRecordParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Json sink record parser
 *
 * @author Kseniia Orlenko
 * @version 5/19/18
 */
public class JsonSinkRecordParser implements SinkRecordParser {

    private static final Logger logger = LoggerFactory.getLogger(JsonSinkRecordParser.class);
    private final static ObjectMapper mapper = new ObjectMapper();

    /**
     * Reads key values from string using json object mapper
     * Converts key values to bytes
     * Returns the key values
     *
     * @param record the sink record received from kafka topic
     * @return values in bytes
     */
    @Override
    public Map<String, byte[]> getKey(SinkRecord record) {
        try {
            Map<String, Object> keys = mapper.readValue((String) record.key(),
                    new TypeReference<Map<String, Object>>() {
                    });
            return parseToBytes(keys);
        } catch (IOException e) {
            logger.error("Mapper was failed when parsing the record key: [{}]", record);
        }
        return null;
    }

    /**
     * Reads values from string using json object mapper
     * Converts values values to bytes
     * Returns the values
     *
     * @param record the sink record received from kafka topic
     * @return values in bytes
     */
    @Override
    public Map<String, byte[]> getValue(SinkRecord record) {
        try {
            Map<String, Object> keys = mapper.readValue((String) record.value(),
                    new TypeReference<Map<String, Object>>() {
                    });
            return parseToBytes(keys);
        } catch (IOException e) {
            logger.error("Mapper was failed when parsing the record value: [{}]", record);
        }
        return null;
    }

    private Map<String, byte[]> parseToBytes(Map<String, Object> data) {
        return data.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        this::getBytes
                ));
    }

    private byte[] getBytes(Map.Entry<String, Object> filed) {
        if (filed.getValue() instanceof String) {
            return ((String) filed.getValue()).getBytes();
        } else if (filed.getValue() instanceof Integer) {
            return Bytes.toBytes((int) filed.getValue());
        } else if (filed.getValue() instanceof Long) {
            return Bytes.toBytes((long) filed.getValue());
        } else if (filed.getValue() instanceof Double) {
            return Bytes.toBytes((long) filed.getValue());
        }
        return new byte[] {};
    }
}
