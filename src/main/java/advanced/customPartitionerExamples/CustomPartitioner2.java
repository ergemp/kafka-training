package advanced.customPartitionerExamples;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class CustomPartitioner2 implements Partitioner {
    //private IUserService userService;

    public CustomPartitioner2() {
        // userService = new UserServiceImpl();
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
                         Cluster cluster) {

        int partition = 0;
        String userName = (String) key;
        // Find the id of current user based on the username
        //Integer userId = userService.findUserId(userName);
        Integer userId = null;
        // If the userId not found, default partition is 0
        if (userId != null) {
            partition = userId;
        }
        return partition;
    }

    @Override
    public void close() {
    }
}
