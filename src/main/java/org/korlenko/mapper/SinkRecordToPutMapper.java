package org.korlenko.mapper;

import static org.korlenko.configuration.HBaseSinkConfiguration.TABLE_COLUMN_FAMILY_TEMPLATE;
import static org.korlenko.configuration.HBaseSinkConfiguration.TABLE_ROW_KEY_COLUMNS_TEMPLATE;
import static org.korlenko.configuration.HBaseSinkConfiguration.TABLE_ROW_KEY_DELIMITER_TEMPLATE;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.connect.sink.SinkRecord;
import org.korlenko.parser.SinkRecordParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Function;

/**
 * Sink records to put mapper
 *
 * @author Kseniia Orlenko
 * @version 5/19/18
 */
public class SinkRecordToPutMapper implements Function<SinkRecord, Put> {

    private static final Logger logger = LoggerFactory.getLogger(SinkRecordToPutMapper.class);
    private static final String DELIMITER = ",";


    private final Map<String, String> props;
    private final SinkRecordParser parser;

    public SinkRecordToPutMapper(Map<String, String> props, SinkRecordParser parser) {
        this.props = props;
        this.parser = parser;
    }

    /**
     * Transforms Kafka sink records to HBase put
     *
     * @param record a record from topic
     * @return the put to HBase
     */
    @Override
    public Put apply(SinkRecord record) {
        logger.info("Mapping row data : [{}]", record.value().toString());
        final String table = record.topic();
        final String columnFamily = props.get(String.format(TABLE_COLUMN_FAMILY_TEMPLATE, table));
        final String delimiter = props.get(String.format(TABLE_ROW_KEY_DELIMITER_TEMPLATE, table));
        final String[] rowColumn =
                props.get(String.format(TABLE_ROW_KEY_COLUMNS_TEMPLATE, table)).split(DELIMITER);

        Map<String, byte[]> values = parser.getValue(record);
        byte[] rowKey = getRowKey(values, rowColumn, delimiter.getBytes());
        final Put put = new Put(rowKey);
        values.forEach((columnName, value) -> {
            put.addColumn(Bytes.toBytes(columnFamily), columnName.getBytes(), value);
        });
        return put;
    }

    private byte[] getRowKey(final Map<String, byte[]> valuesMap, final String[] columns,
                             final byte[] delimiterBytes) {
        byte[] rowKey = null;
        for (String column : columns) {
            byte[] columnValue = valuesMap.get(column);
            if (rowKey == null) {
                rowKey = columnValue;
            } else {
                rowKey = Bytes.add(rowKey, delimiterBytes, columnValue);
            }
        }
        return rowKey;
    }
}
