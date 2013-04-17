package org.ken.terracotta.main;

import org.ken.terracotta.router.MessageRouter;
import org.ken.terracotta.router.RoutingException;
import org.ken.terracotta.queue.QueueManager;
import org.perf4j.LoggingStopWatch;
import org.perf4j.log4j.Log4JStopWatch;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Date: 31.05.2009
 *
 * @author bagmanov
 */
public class StartLoadPublisher {
    static MessageRouter router = MessageRouter.create(new QueueManager());
    static LoggingStopWatch stopWatch = new Log4JStopWatch("publisher");
    static AtomicLong number = new AtomicLong(1L);

    public static void main(String [] argz){
        if(argz.length == 1){ 
            Integer threadsCount = Integer.valueOf(argz[0]);
            System.out.println(threadsCount);
            for (int i = 0; i < threadsCount; i++) {
                new Thread(new Runnable() {
                    public void run() {
                        load(router);
                    }
                }).start();

            }
        } else {
            load(router);
        }
    }


    private static void load(final MessageRouter router) {
        while(true) {
            long value = number.decrementAndGet();
            router.routeMessage("insert into terra_test (test) values('" +String.valueOf(value) + "');");
            stopWatch.lap("publisher", "ok");
        }
    }
}
