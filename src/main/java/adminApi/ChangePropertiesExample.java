package adminApi;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ChangePropertiesExample {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        AdminClient client = AdminClient.create(prop);

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "tweet");

        // get the current topic configuration
        DescribeConfigsResult describeConfigsResult = client.describeConfigs(Collections.singleton(resource));

        Map<ConfigResource, Config> config;

        try {

            config = describeConfigsResult.all().get();
            System.out.println(config);

            // create a new entry for updating the retention.ms value on the same topic
            ConfigEntry retentionEntry = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "60000");
            Map<ConfigResource, Config> updateConfig = new HashMap<ConfigResource, Config>();
            updateConfig.put(resource, new Config(Collections.singleton(retentionEntry)));

            AlterConfigsResult alterConfigsResult = client.alterConfigs(updateConfig);
            alterConfigsResult.all();

            describeConfigsResult = client.describeConfigs(Collections.singleton(resource));

            config = describeConfigsResult.all().get();

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
