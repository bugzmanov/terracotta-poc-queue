package org.ken.terracotta.router;

import org.ken.terracotta.queue.QueueManager;
import org.ken.terracotta.config.Config;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.locks.ReentrantReadWriteLock;


import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import com.tc.injection.annotations.InjectedDsoInstance;
import com.tc.cluster.DsoCluster;
import com.tc.cluster.DsoClusterListener;
import com.tc.cluster.DsoClusterEvent;
import com.tcclient.cluster.DsoNode;

/**
 * Batch aware message router, watches for nodes available in cluster, and use batching round-robin
 * algorithm to route messages in available queues
 *
 * @author: bagmanov
 */
public class MessageRouter implements DsoClusterListener {

    private transient static final Log log = LogFactory.getLog(MessageRouter.class);
    private ExecutorService threadPool = Executors.newCachedThreadPool();
    private final QueueManager queueManager;

    private int currentQueue = 0;
    private int messagesInCurrentQueue = 0;
    private int availableQueuesCount = 0; //its much faster to track current queues in cluster events, than quering terracotta everytime

    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    @InjectedDsoInstance
    private DsoCluster cluster;

    protected MessageRouter(QueueManager queueManager) {
        this.queueManager = queueManager;
        cluster.addClusterListener(this);
    }

    //todo:consider router initialization in nodeJoined event listener, as it done in SqlWriter
    public static MessageRouter create(QueueManager queueManager) {
        MessageRouter instance = new MessageRouter(queueManager);
        instance.reinitRoutes();
        return instance;
    }

    public void routeMessage(String message) {
        rwLock.readLock().lock();
        try {
            waitForAtLeastOneQueueRegistered();
            int queueIndex = 0;
            synchronized (this) {
                if (++messagesInCurrentQueue > Config.BATCH_SIZE) {
                    currentQueue++;
                    messagesInCurrentQueue = 0;
                    if (currentQueue >= availableQueuesCount) {
                        currentQueue = 0;
                    }
                }
                queueIndex = currentQueue;
            }
            final Object queueId = queueManager.getQueuesId().get(queueIndex);
            queueManager.put(queueId, message);

        } finally {
            rwLock.readLock().unlock();
        }
    }


    public synchronized void registerNewRoute(Object nodeId) {
        log.info(String.valueOf(new Date().getTime()) + " node " + nodeId.toString() + " has joined the cluster");
        queueManager.getOrCreateQueueFor(nodeId);
        availableQueuesCount = queueManager.queuesCount();
        synchronized (queueManager) {
            queueManager.notifyAll();
        }
    }


    public void unregisterRoute(Object nodeId) {
        rwLock.writeLock().lock();
        try {
            System.out.println("remove " + nodeId.toString());
            availableQueuesCount = queueManager.queuesCount();
            while (availableQueuesCount <= 1) { //wait until there will be available queues where messages can be rerouted
               synchronized (queueManager) {
                  try {
                      queueManager.wait(3000);
                      availableQueuesCount = queueManager.queuesCount();
                  } catch (InterruptedException e) {/*do nothing*/}
                }
            }
            List<String> messages = getAllPendingMessages(nodeId);
            queueManager.removeQueue(nodeId);
            availableQueuesCount = queueManager.queuesCount();
            if (currentQueue >= availableQueuesCount) {
                 messagesInCurrentQueue = 0;
                 currentQueue = 0;
            }
            for (String message : messages) {
                routeMessage(message);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public synchronized void reinitRoutes() {
        Collection<DsoNode> nodes = cluster.getClusterTopology().getNodes();
        for (DsoNode node : nodes) {
            System.out.println("id = " + node.getId() + " - " + node.getIp());
        }
        List<Object> queueIds = new ArrayList<Object>();
        queueIds.addAll(queueManager.getQueuesId());
        List<String> messages = new ArrayList<String>();
        for (Object queueId : queueIds) {
            if (!nodes.contains(queueId)) {
                messages.addAll(getAllPendingMessages(queueId));
                queueManager.removeQueue(queueId);

            }
        }
        availableQueuesCount = queueManager.queuesCount();
        for (String message : messages) {
            routeMessage(message);
        }
    }

    private void waitForAtLeastOneQueueRegistered() {
        if (availableQueuesCount > 0) {
            return;
        }
        while (availableQueuesCount == 0) {
            synchronized (queueManager) {
                try {
                    queueManager.wait(3000);
                } catch (InterruptedException e) {/*do nothing*/}
            }
        }
    }

    private List<String> getAllPendingMessages(Object nodeId) {
        List<String> messages = new ArrayList<String>();
        while (queueManager.getOrCreateQueueFor(nodeId).size() > 0) {
            try {
                messages.add(queueManager.getOrCreateQueueFor(nodeId).take());
            } catch (InterruptedException e) {/* do nothing. just try again */ }
        }
        return messages;
    }


    public void nodeJoined(final DsoClusterEvent dsoClusterEvent) {
        if (!dsoClusterEvent.getNode().equals(cluster.getCurrentNode())) {
            this.registerNewRoute(dsoClusterEvent.getNode());
        }
    }

    public void nodeLeft(final DsoClusterEvent dsoClusterEvent) {
        if (!dsoClusterEvent.getNode().equals(cluster.getCurrentNode())) {
            threadPool.execute(new Runnable() { // don't want to block notification thread
                public void run() {
                    unregisterRoute(dsoClusterEvent.getNode());
                }
            });
        }
    }

    public void operationsEnabled(DsoClusterEvent dsoClusterEvent) { /* do nothing */ }

    public void operationsDisabled(DsoClusterEvent dsoClusterEvent) { /* do nothing*/ }
}
