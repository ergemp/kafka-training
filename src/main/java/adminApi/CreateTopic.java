package adminApi;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CreateTopic {
    public static void main(String[] args) {
        try {
            Properties config = new Properties();
            config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            AdminClient admin = AdminClient.create(config);

            //creating new topic
            System.out.println("-- creating --");
            NewTopic newTopic = new NewTopic("my-new-topic", 1, (short) 1);
            admin.createTopics(Collections.singleton(newTopic));

            //listing
            System.out.println("-- listing --");

            admin.listTopics().names().get().forEach(System.out::println);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
