package adminApi;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ListingBrokerProperties {
    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        AdminClient admin = AdminClient.create(config);

        try {
            for (Node node : admin.describeCluster().nodes().get()) {
                System.out.println("-- node: " + node.id() + " --");

                ConfigResource cr = new ConfigResource(ConfigResource.Type.BROKER, node.idString());

                DescribeConfigsResult dcr = admin.describeConfigs(Collections.singleton(cr));

                dcr.all().get().forEach((k, c) -> {
                    c.entries()
                            .forEach(configEntry -> {
                                System.out.println(configEntry.name() + "= " + configEntry.value());
                            });
                });
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
