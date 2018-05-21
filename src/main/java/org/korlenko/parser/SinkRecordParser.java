package org.korlenko.parser;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

/**
 * Sink record parser
 *
 * @author Kseniia Orlenko
 * @version 5/19/18
 */
public interface SinkRecordParser {

    /**
     * Returns the key value
     *
     * @param record the sink record received from kafka topic
     * @return
     */
    Map<String, byte[]> getKey(SinkRecord record);

    /**
     * Returns the values
     *
     * @param record the sink record received from kafka topic
     * @return
     */
    Map<String, byte[]> getValue(SinkRecord record);
}
