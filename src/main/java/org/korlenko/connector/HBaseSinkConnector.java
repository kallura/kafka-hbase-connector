package org.korlenko.connector;

import static org.korlenko.configuration.HBaseSinkConfiguration.getConfigDef;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.korlenko.helper.VersionHelper;
import org.korlenko.taks.HBaseSinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HBase sink connector implementation
 *
 * @author Kseniia Orlenko
 * @version 5/19/18
 */
public class HBaseSinkConnector extends SinkConnector {

    private static final Logger logger = LoggerFactory.getLogger(HBaseSinkConnector.class);

    private final ConfigDef config = getConfigDef();
    private Map<String, String> props;

    /**
     * Returns the connector version
     *
     * @return the version
     */
    @Override
    public String version() {
        return VersionHelper.getProjectVersion();
    }

    /**
     * Starts the connector
     *
     * @param props configuration settings
     */
    @Override
    public void start(Map<String, String> props) {
        logger.info("Start the HBase sink connector");
        this.props = props;

    }

    /**
     * Returns source task implementation
     *
     * @return SinkTask class instance
     */
    @Override
    public Class<? extends Task> taskClass() {
        return HBaseSinkTask.class;
    }


    /**
     * Returns tasks configurations
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for tasks
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskProps = new HashMap<>(props);
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    /**
     * Stops the connector
     */
    @Override
    public void stop() {
        logger.info("Stop the HBase sink connector");
    }

    /**
     * Returns the config
     *
     * @return default config
     */
    @Override
    public ConfigDef config() {
        return config;
    }
}
