package org.ken.terracotta.writer;

import org.ken.terracotta.queue.QueueManager;
import org.ken.terracotta.queue.SimpleQueue;
import org.ken.terracotta.queue.BatchQueue;
import org.ken.terracotta.config.Config;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import java.sql.Statement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.BatchUpdateException;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;

import com.tc.injection.annotations.InjectedDsoInstance;
import com.tc.cluster.DsoCluster;
import com.tc.cluster.DsoClusterListener;
import com.tc.cluster.DsoClusterEvent;
import org.perf4j.LoggingStopWatch;
import org.perf4j.log4j.Log4JStopWatch;

/**
 * SqlWriter is responsible for reading sql expressions from nodes queue and executing them in batch .
 * <p/>
 * It is assumed that there is only one queue per node, so if one node contains two or more SqlWriters, they will
 * be reading from the same Queue.
 * <p/>
 * SqlWriter will create queue, if it is not exist.
 * Appropriate way to shutdown thread with SqlWriter as runnable is calling {@link .stop()} method
 *
 * @author bagmanov
 */
public class SqlWriter implements Runnable, DsoClusterListener {
    private transient static final Log log = LogFactory.getLog(SqlWriter.class);

    private BatchQueue<String> queue;
    private QueueManager manager;
    private boolean isRunning;

    @InjectedDsoInstance
    private DsoCluster cluster;
    private Connection connection;
    private final int batchSize;

    public SqlWriter(Connection connection, QueueManager manager, int batchSize) {
        this.batchSize = batchSize;
        this.connection = connection;
        this.manager = manager;
        cluster.addClusterListener(this);
    }

    public void stop() {
        if (isRunning) {
            isRunning = false;
            try {
                synchronized (this) {
                    wait();
                }
            } catch (InterruptedException e) {/* do nothing */ }
        } else {
            throw new IllegalStateException("SqlWriter is not running");
        }
    }


    public void run() {
        waitForRegistrationInCluster();
        List<String> bulkStrings = new ArrayList<String>();
        try {
            isRunning = true;
            String writerName = "writer-" + cluster.getCurrentNode().getId();
            LoggingStopWatch stopWatch = new Log4JStopWatch(writerName);
            while (isRunning) {
                bulkStrings.clear();
                Statement statement = connection.createStatement();
                for (int i = 0; i < batchSize; i++) {
                    try {
                        String sql = queue.take();
                        bulkStrings.add(sql);
                        statement.addBatch(sql);
                    } catch (InterruptedException e) {
                        log.warn("couldn't take message from queue", e);
                        i--;
                    }
                }
                statement.executeBatch();
                statement.close();
                stopWatch.lap(writerName, "ok");
            }
        } catch (SQLException ex) {
            putFailedExpressionsBackToQueue(bulkStrings, ex);
        } finally {
            closeConnection(); //TODO: it is not appropriate to close injected conection
            flushCache();
            synchronized (this) {
                notifyAll();
            }
        }
    }

    private void putFailedExpressionsBackToQueue(List<String> bulkStrings, SQLException ex) {
        log.error(ex.getMessage(), ex);
        if (ex instanceof BatchUpdateException) {
            int[] results = ((BatchUpdateException) ex).getUpdateCounts();
            for (int i = 0; i < results.length; i++) {
                if (results[i] != Statement.EXECUTE_FAILED) {
                    bulkStrings.remove(i);
                }
            }
        }
        for (String sql : bulkStrings) {
            queue.put(sql);
        }
    }

    private void flushCache() {
        while (true) {
            try {
                queue.flushCache();
                break;
            } catch (InterruptedException e) {
                log.warn("couldn't flush cache. Try again after 5 seconds", e);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e1) {/* do nothing */}
            }

        }
    }

    private void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.error("couldn't close database connection", e);
            }
        }
    }

    private synchronized void waitForRegistrationInCluster() {
        while (queue == null) {
            try {
                wait(3000);
            } catch (InterruptedException e) { /* do nothin() */}
        }
    }

    public synchronized void nodeJoined(DsoClusterEvent dsoClusterEvent) {
        if (!dsoClusterEvent.getNode().equals(cluster.getCurrentNode())) {
            return; // do not bother about other nodes in cluster
        }
        //this is more safe way than just try to create queue in SqlWriter consturctor with cluster.getCurrentNode()
        Object nodeId = dsoClusterEvent.getNode();
        queue = manager.getOrCreateQueueFor(nodeId);
        log.info(String.valueOf(new Date().getTime()) + " i am");
        notifyAll();
    }

    public void nodeLeft(DsoClusterEvent dsoClusterEvent) { /* do nothing */ }

    public void operationsEnabled(DsoClusterEvent dsoClusterEvent) { /* do nothing */ }

    public void operationsDisabled(DsoClusterEvent dsoClusterEvent) { /* do nothing */ }
}
