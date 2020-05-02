/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;


/**
 * A pool of ByteBuffers kept under a given memory limit. This class is fairly specific to the needs of the producer. In
 * particular it has the following properties:
 * <ol>
 * <li>There is a special "poolable size" and buffers of this size are kept in a free list and recycled
 * <li>It is fair. That is all memory is given to the longest waiting thread until it has sufficient memory. This
 * prevents starvation or deadlock when a thread asks for a large chunk of memory and needs to block until multiple
 * buffers are deallocated.
 * </ol>
 */
public final class BufferPool {

    // buffer.memory  默认 32MB
    private final long totalMemory;

    // batch.size  默认 16KB
    private final int poolableSize;

    private final ReentrantLock lock;

    // Deque作为队列，缓存了一些ByteBuffer，也就是缓存了一批内存空间，可以用来复用的
    private final Deque<ByteBuffer> free;

    private final Deque<Condition> waiters;

    /**
     * availableMemory 初始化32MB
     *
     * 如果消息不到16kb，则默认使用free列表里标准化的ByteBuffer
     * 如果消息大于 16KB，就要从 availableMemory变量申请内存，然后回头释放
     */
    private long availableMemory;

    private final Metrics metrics;
    private final Time time;
    private final Sensor waitTime;

    /**
     * Create a new buffer pool
     * 
     * @param memory The maximum amount of memory that this buffer pool can allocate
     * @param poolableSize The buffer size to cache in the free list rather than deallocating
     * @param metrics instance of Metrics
     * @param time time instance
     * @param metricGrpName logical group name for metrics
     */
    public BufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName) {

        // 使用 batch.size 初始化
        this.poolableSize = poolableSize;

        this.lock = new ReentrantLock();

        this.free = new ArrayDeque<ByteBuffer>();

        this.waiters = new ArrayDeque<Condition>();

        // 初始化都是使用设置的 buffer.memory 设置
        this.totalMemory = memory;
        this.availableMemory = memory;

        this.metrics = metrics;
        this.time = time;
        this.waitTime = this.metrics.sensor("bufferpool-wait-time");
        MetricName metricName = metrics.metricName("bufferpool-wait-ratio",
                                                   metricGrpName,
                                                   "The fraction of time an appender waits for space allocation.");
        this.waitTime.add(metricName, new Rate(TimeUnit.NANOSECONDS));
    }

    /**
     * Allocate a buffer of the given size. This method blocks if there is not enough memory and the buffer pool
     * is configured with blocking mode.
     * 
     * @param size The buffer size to allocate in bytes 申请的内存大小（字节）
     * @param maxTimeToBlockMs The maximum time in milliseconds to block for buffer memory to be available
     * @return The buffer
     * @throws InterruptedException If the thread is interrupted while blocked
     * @throws IllegalArgumentException if size is larger than the total memory controlled by the pool (and hence we would block
     *         forever)
     */
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        if (size > this.totalMemory)
            throw new IllegalArgumentException("Attempt to allocate " + size
                                               + " bytes, but there is a hard limit of "
                                               + this.totalMemory
                                               + " on memory allocations.");

        this.lock.lock();
        try {
            // check if we have a free buffer of the right size pooled
            if (size == poolableSize && !this.free.isEmpty())
                // 申请的内存等于默认的batch size，并且空闲列表不为空，直接返回一个 ByteBuffer 就可以了
                // 相当于复用的概念
                return this.free.pollFirst();

            // now check if the request is immediately satisfiable with the
            // memory on hand or if we need to block
            int freeListSize = this.free.size() * this.poolableSize;

            if (this.availableMemory + freeListSize >= size) {
                // 有足够的空间可以分配
                // we have enough unallocated or pooled memory to immediately
                // satisfy the request
                freeUp(size);
                //
                this.availableMemory -= size;
                lock.unlock();
                return ByteBuffer.allocate(size);
            } else {

                /**
                 * 不停地写数据，内存耗尽
                 * 没有足够空间的时候，会阻塞一段时间
                 */
                // we are out of memory and will have to block
                int accumulated = 0;
                ByteBuffer buffer = null;
                Condition moreMemory = this.lock.newCondition();
                long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs /*等待的最长时间*/ );

                this.waiters.addLast(moreMemory); // 加入等待的Condition队列
                // loop over and over until we have a buffer or have reserved
                // enough memory to allocate one
                while (accumulated < size) { // 只要累积的内存< 需要申请的内存size，就一直循环
                    long startWaitNs = time.nanoseconds();
                    long timeNs;
                    boolean waitingTimeElapsed;
                    try {
                        // 就在这阻塞等待一段时间
                        waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        this.waiters.remove(moreMemory);
                        throw e;
                    } finally {
                        long endWaitNs = time.nanoseconds();
                        timeNs = Math.max(0L, endWaitNs - startWaitNs);
                        this.waitTime.record(timeNs, time.milliseconds());
                    }

                    if (waitingTimeElapsed) {
                        this.waiters.remove(moreMemory);
                        throw new TimeoutException("Failed to allocate memory within the configured max blocking time " + maxTimeToBlockMs + " ms.");
                    }

                    remainingTimeToBlockNs -= timeNs;
                    // check if we can satisfy this request from the free list,
                    // otherwise allocate memory
                    if (accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
                        // just grab a buffer from the free list
                        buffer = this.free.pollFirst();
                        accumulated = size;
                    } else {
                        // we'll need to allocate memory, but we may only get
                        // part of what we need on this iteration
                        freeUp(size - accumulated);
                        int got = (int) Math.min(size - accumulated, this.availableMemory);
                        this.availableMemory -= got;
                        accumulated += got;
                    }
                }

                // remove the condition for this thread to let the next thread
                // in line start getting memory
                Condition removed = this.waiters.removeFirst();
                if (removed != moreMemory)
                    throw new IllegalStateException("Wrong condition: this shouldn't happen.");

                // signal any additional waiters if there is more memory left
                // over for them
                if (this.availableMemory > 0 || !this.free.isEmpty()) {
                    if (!this.waiters.isEmpty())
                        this.waiters.peekFirst().signal();
                }

                // unlock and return the buffer
                lock.unlock();
                if (buffer == null)
                    return ByteBuffer.allocate(size);
                else
                    return buffer;
            }
        } finally {
            if (lock.isHeldByCurrentThread())
                lock.unlock();
        }
    }

    /**
     * Attempt to ensure we have at least the requested number of bytes of memory for allocation by deallocating pooled
     * buffers (if needed)
     */
    private void freeUp(int size) {
        while (!this.free.isEmpty() && this.availableMemory < size)
            this.availableMemory += this.free.pollLast().capacity();
    }

    /**
     * Return buffers to the pool. If they are of the poolable size add them to the free list, otherwise just mark the
     * memory as free.
     * 
     * @param buffer The buffer to return
     * @param size The size of the buffer to mark as deallocated, note that this maybe smaller than buffer.capacity
     *             since the buffer may re-allocate itself during in-place compression
     */
    public void deallocate(ByteBuffer buffer, int size) {
        lock.lock();
        try {
            if (size == this.poolableSize && size == buffer.capacity()) {
                buffer.clear();
                // 放回到 free 队列
                this.free.add(buffer);
            } else {
                /**
                 * 万一有人的消息是大于16KB的，比如分配的那个 ByteBuffer 是52KB
                 * 如何对这个52KB的 ByteBuffer进行处理？他会直接释放这块内存，不去管它，让gc掉，只是把 availableMemory 加回去就行了
                 */
                this.availableMemory += size;
            }

            /**
             * 这个地方很精妙，通知那边等待申请内存的线程，有空间可用了
             * 线程间通信，相当于 notify
             */
            Condition moreMem = this.waiters.peekFirst();
            if (moreMem != null)
                moreMem.signal();

        } finally {
            lock.unlock();
        }
    }

    public void deallocate(ByteBuffer buffer) {
        deallocate(buffer, buffer.capacity());
    }

    /**
     * the total free memory both unallocated and in the free list
     */
    public long availableMemory() {
        lock.lock();
        try {
            return this.availableMemory + this.free.size() * this.poolableSize;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the unallocated memory (not in the free list or in use)
     */
    public long unallocatedMemory() {
        lock.lock();
        try {
            return this.availableMemory;
        } finally {
            lock.unlock();
        }
    }

    /**
     * The number of threads blocked waiting on memory
     * 阻塞等待内存分配的线程数
     */
    public int queued() {
        lock.lock();
        try {
            return this.waiters.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The buffer size that will be retained in the free list after use
     */
    public int poolableSize() {
        return this.poolableSize;
    }

    /**
     * The total memory managed by this pool
     */
    public long totalMemory() {
        return this.totalMemory;
    }

    // package-private method used only for testing
    Deque<Condition> waiters() {
        return this.waiters;
    }
}
