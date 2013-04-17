package org.ken.terracotta.queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

/**
 * Batching queue wrapper, collects {@link org.ken.terracotta.config.Config} BATCH_SIZE itemes until sends them to
 * terractotta-clustered queue
 *
 * @author: bagmanov
 */
public class BatchQueue<T> implements SimpleQueue<T> {

    private final static transient Log log = LogFactory.getLog(BatchQueue.class);

    private final int batchSize;
    private final LinkedBlockingQueue<T[]> queue;

    private transient ExecutorService threadPool;

    private final Object batchPutLock = new Object();
    private final Object batchGetLock = new Object();

    private transient List<T> batchPut = null;
    private transient T[] batchGet = null;

    private transient volatile int index = 0;

    public BatchQueue(final int batchSize) {
        threadPool = Executors.newCachedThreadPool();
        this.batchSize = batchSize;
        queue = new LinkedBlockingQueue<T[]>();
    }

    public void initThreadPool() {
        threadPool = Executors.newCachedThreadPool();
    }

    public T put(T item){
        List<T> ready = null;

        synchronized (batchPutLock) {
            if (batchPut == null) {
                batchPut = new ArrayList<T>(batchSize);
            }

            batchPut.add(item);
            if (batchPut.size() >= batchSize) {
                ready = batchPut;
                batchPut = null;
            }
        }

        if (ready != null) {
            final List<T> toSend = ready;
            threadPool.execute(new Runnable() {
                public void run() {
                    try {
                        queue.put((T[]) toSend.toArray());
                    } catch (InterruptedException e) {
                        log.warn("couldn't put messages to queue", e);
                        synchronized (batchPutLock) {
                            if (batchPut == null) {
                                batchPut = toSend;
                            } else {
                                batchPut.addAll(toSend);
                            }
                        }
                    }
                }
            });
        }
        return item;
    }

    public void flush() throws InterruptedException {
        List<T> ready = null;
        synchronized (batchPutLock) {
            if (batchPut != null && batchPut.size() > 0) {
                ready = batchPut;
                batchPut = null;
            }
        }
        if(ready != null){
            queue.put((T[]) ready.toArray());
        }

    }

    public void flushCache()throws InterruptedException  {
        synchronized (batchGetLock){
            for (int i = index; i < batchGet.length; i++){
                this.put(batchGet[i]);
            }
            List<T> ready = null;
            synchronized (batchPutLock) {
                if (batchPut != null && batchPut.size() > 0) {
                    ready = batchPut;
                    batchPut = null;
                }
            }
            if(ready != null){
                queue.put((T[]) ready.toArray());
            }

        }
    }
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        synchronized (batchGetLock) {
            batchPoll(timeout, unit);
            return batchGet[index++];
        }
    }

    public T take() throws InterruptedException {
        synchronized (batchGetLock) {
            if (batchGet == null || index >= batchGet.length) {
                batchGet = queue.take();
                index = 0;
            }
            final T workItem = batchGet[index];
            index++;
            return workItem;
        }
    }

    public int size() {
        return queue.size();
    }

    public void clear() {
        queue.clear();
    }

    private void batchPoll(long timeout, TimeUnit unit) throws InterruptedException {
        if (batchGet == null || index >= batchGet.length) {
            batchGet = queue.poll(timeout, unit);
            index = 0;
        }
    }

}
