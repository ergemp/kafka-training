package adminApi;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CreateTopic {
    public static void main(String[] args) {
        try {
            Properties config = new Properties();
            config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "10.211.55.9:9092");
            config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

            AdminClient admin = AdminClient.create(config);

            //creating new topic
            System.out.println("-- creating --");
            NewTopic newTopic = new NewTopic("my-new-topic", 1, (short) 1);
            admin.createTopics(Collections.singleton(newTopic));
            Thread.sleep(5000);

            //listing
            System.out.println("-- listing --");
            admin.listTopics().names().get().forEach(System.out::println);

            //delete the topic
            System.out.println("-- deleting --");
            KafkaFuture<Void> future = admin.deleteTopics(Collections.singleton("my-new-topic")).all();
            Thread.sleep(5000);

            //listing
            System.out.println("-- listing --");
            admin.listTopics().names().get().forEach(System.out::println);

            try {
                future.get();
            }
            catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
