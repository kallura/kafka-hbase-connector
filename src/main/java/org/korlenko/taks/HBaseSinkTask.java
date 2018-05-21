package org.korlenko.taks;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.korlenko.configuration.HBaseSinkConfiguration.MAP_FUNCTION_CLASS;
import static org.korlenko.configuration.HBaseSinkConfiguration.TABLE_COLUMN_FAMILY_TEMPLATE;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.korlenko.configuration.HBaseSinkConfiguration;
import org.korlenko.helper.VersionHelper;
import org.korlenko.mapper.SinkRecordToPutMapper;
import org.korlenko.parser.SinkRecordParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * HBase sink task
 *
 * @author Kseniia Orlenko
 * @version 5/19/18
 */
public class HBaseSinkTask extends SinkTask {

    private static final Logger logger = LoggerFactory.getLogger(HBaseSinkTask.class);
    private static final String DELIMITER = ",";

    private Connection connection;
    private SinkRecordToPutMapper mapper;

    /***
     * Returns the task version
     * @return task version
     */
    @Override
    public String version() {
        return VersionHelper.getProjectVersion();
    }

    /**
     * Starts the sink task
     *
     * @param props the task properties
     */
    @Override
    public void start(Map<String, String> props) {
        final HBaseSinkConfiguration sinkConfiguration = new HBaseSinkConfiguration(props);
        sinkConfiguration.validate();
        SinkRecordParser parser =
                sinkConfiguration.getConfiguredInstance(MAP_FUNCTION_CLASS, SinkRecordParser.class);
        mapper = new SinkRecordToPutMapper(props, parser);

        String zookeeperQuorum = sinkConfiguration
                .getString(HBaseSinkConfiguration.ZOOKEEPER_QUORUM_CONFIG);
        String compressor = sinkConfiguration.getString(HBaseSinkConfiguration.TABLES_COMPRESSOR);
        String topicsStr = sinkConfiguration.getString(HBaseSinkConfiguration.TOPICS_CONFIG);
        String[] topics = topicsStr.split(DELIMITER);

        Configuration hBaseConfiguration = HBaseConfiguration.create();
        hBaseConfiguration.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum);

        try {
            HBaseAdmin.available(hBaseConfiguration);
            connection = ConnectionFactory.createConnection(hBaseConfiguration);
        } catch (IOException e) {
            logger.error("HBase connection failed! [{}]", hBaseConfiguration, e);
        }
        try {
            for (String topic : topics) {
                final String columnFamily =
                        props.get(String.format(TABLE_COLUMN_FAMILY_TEMPLATE, topic));
                createTable(connection, topic, columnFamily, compressor);
            }
        } catch (IOException e) {
            logger.error("HBase was failed when create tables for topics [{}]! ", topics, e);
        }
    }

    /**
     * Puts records to HBase
     *
     * @param records the records from Kafka topic
     */
    @Override
    public void put(Collection<SinkRecord> records) {
        Map<String, List<SinkRecord>> groupedByTopic = records.stream()
                .collect(groupingBy(SinkRecord::topic));

        Map<String, List<Put>> groupedByTable = groupedByTopic.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, (e) ->
                        e.getValue().stream().map(mapper).collect(toList())));

        groupedByTable.entrySet().parallelStream().forEach(entry -> {
            TableName tableName = TableName.valueOf(entry.getKey());
            try {
                final BufferedMutator mutator = connection.getBufferedMutator(tableName);
                mutator.mutate(entry.getValue());
                mutator.flush();
            } catch (IOException e) {
                logger.error("HBase was failed when write data to table [{}]! ",
                        tableName.toString(), e);
            }
        });
    }

    /**
     * Stops the taks
     */
    @Override
    public void stop() {
    }

    private static void createTable(Connection connection, String table,
                                    String columnFamily, String compressor) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(table);
        if (!admin.tableExists(tableName)) {
            TableDescriptor tableDescriptor = TableDescriptorBuilder
                    .newBuilder(tableName)
                    .setCoprocessor(compressor)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamily))
                    .build();
            admin.createTable(tableDescriptor);
        }
    }
}
