package org.ken.terracotta.queue;

import org.ken.terracotta.config.Config;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages nodeId<->Queue map, wich contains all available queues in cluster
 *
 * @author: bagmanov
 */
public class QueueManager {
    final Map<Object, BatchQueue<String>> queues = new ConcurrentHashMap<Object, BatchQueue<String>>();
    final List<Object> queuesId = new ArrayList<Object>();

    public BatchQueue<String> getOrCreateQueueFor(Object nodeId) {
        
        synchronized (queues) {
            if (queues.containsKey(nodeId)) {
                return queues.get(nodeId);
            }
            BatchQueue<String> queue = new BatchQueue<String>(Config.BATCH_SIZE);
            queues.put(nodeId, queue);
            queuesId.add(nodeId);
            return queue;
        }
    }

    public void removeQueue(Object nodeId) {
        synchronized (queues) {
            if (queues.containsKey(nodeId)) {
                queues.get(nodeId).clear();
                queues.remove(nodeId);
                queuesId.remove(nodeId);
            }
        }
    }

    public void put(Object nodeId, String message){
        queues.get(nodeId).put(message);
    }

    public Map<Object, BatchQueue<String>> getQueues() {
        return queues;
    }

    public int queuesCount() {
        synchronized (queues){
            return queues.size();
        }
    }

    public List<Object> getQueuesId() {
        return Collections.unmodifiableList(queuesId);
    }
}
