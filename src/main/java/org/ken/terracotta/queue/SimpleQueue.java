package org.ken.terracotta.queue;

import java.util.concurrent.TimeUnit;

/**
 * Queue interface (cause theres is no need for all java.util.Queue facilities) 
 *
 * @author: bagmanov
 */
public interface SimpleQueue<T> {
	T poll(long timeout, TimeUnit unit) throws InterruptedException;

	T put(T item);

	T take() throws InterruptedException;

	int size();

	void clear();

    void flush() throws InterruptedException;
}
