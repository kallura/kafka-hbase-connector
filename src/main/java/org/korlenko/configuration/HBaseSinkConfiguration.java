package org.korlenko.configuration;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.korlenko.parser.impl.JsonSinkRecordParser;

import java.util.Map;

/**
 * HBase connector configuration class
 *
 * @author Kseniia Orlenko
 * @version 5/18/18
 */
public class HBaseSinkConfiguration extends AbstractConfig {

    private static final ConfigDef CONFIG_DEF = new ConfigDef();

    public static final String TOPICS_CONFIG = "topics";
    public static final String ZOOKEEPER_QUORUM_CONFIG = "zookeeper.quorum";
    public static final String MAP_FUNCTION_CLASS = "record.parser.class";

    public static final String TABLES_COMPRESSOR = "base.compressor.class";
    public static final String TABLE_ROW_KEY_COLUMNS_TEMPLATE = "hbase.%s.rowkey.columns";
    public static final String TABLE_ROW_KEY_DELIMITER_TEMPLATE = "hbase.%s.rowkey.delimiter";
    public static final String TABLE_COLUMN_FAMILY_TEMPLATE = "hbase.%s.family";

    private final Map<String, String> props;

    static {
        CONFIG_DEF
                .define(ZOOKEEPER_QUORUM_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Zookeeper quorum for cluster")
                .define(MAP_FUNCTION_CLASS, ConfigDef.Type.CLASS, JsonSinkRecordParser.class,
                        ConfigDef.Importance.HIGH, "Parser class for sink records")
                .define(TOPICS_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Kafka topic")
                .define(TABLES_COMPRESSOR, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "HBase table compressor");
    }

    public HBaseSinkConfiguration(Map<String, String> props) {
        super(CONFIG_DEF, props);
        this.props = props;
    }

    /**
     * Checks row key property is configured per table
     */
    public void validate() {
        final String topicsAsStr = props.get(TOPICS_CONFIG);
        final String[] topics = topicsAsStr.split(",");
        for (String topic : topics) {
            String key = String.format(TABLE_ROW_KEY_COLUMNS_TEMPLATE, topic);
            System.out.println(key);
            if (!props.containsKey(key)) {
                throw new ConfigException(
                        String.format(" No row key has been found for table [%s]", key));
            }
        }
    }

    /**
     * Creates default config
     *
     * @return default config
     */
    public static ConfigDef getConfigDef() {
        return CONFIG_DEF;
    }
}
