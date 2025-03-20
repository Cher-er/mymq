package com.cher.mymq.bio;

import java.io.*;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageQueuePerformanceTest {

    // 测试参数
    private static final String SERVER_HOST = "localhost";
    private static final int SERVER_PORT = 9999;
    private static final String QUEUE_NAME = "test_queue";
    private static final int NUM_PUBLISHER_THREADS = 30;
    private static final int NUM_CONSUMER_THREADS = 30;
    private static final int MESSAGES_PER_PUBLISHER = 1000;
    private static final int THREADS_NUM_PER_QUEUE = 10;

    public static void main(String[] args) {
        // 用于统计延迟的集合（单位：毫秒）
        List<Long> publishLatencies = Collections.synchronizedList(new ArrayList<>());
        List<Long> consumeLatencies = Collections.synchronizedList(new ArrayList<>());

        // 用于统计已发布和已消费消息数
        AtomicInteger publishedCount = new AtomicInteger(0);
        AtomicInteger consumedCount = new AtomicInteger(0);
        int totalMessages = NUM_PUBLISHER_THREADS * MESSAGES_PER_PUBLISHER;

        // 创建线程池
        ExecutorService publisherExecutor = Executors.newFixedThreadPool(NUM_PUBLISHER_THREADS);
        ExecutorService consumerExecutor = Executors.newFixedThreadPool(NUM_CONSUMER_THREADS);

        long startTime = System.currentTimeMillis();

        // 提交发布任务，每个任务持续发送 MESSAGES_PER_PUBLISHER 条消息
        List<Future<?>> publisherFutures = new ArrayList<>();
        for (int i = 0; i < NUM_PUBLISHER_THREADS; i++) {
            int ii = i;
            publisherFutures.add(publisherExecutor.submit(() -> {
                // 每个线程有一个独立的连接
                try (Socket socket = new Socket(SERVER_HOST, SERVER_PORT);
                     BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                     PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

                    String queueName = QUEUE_NAME + "_" + (ii / THREADS_NUM_PER_QUEUE);

                    for (int j = 0; j < MESSAGES_PER_PUBLISHER; j++) {
                        String message = "Message from " + Thread.currentThread().getName() + " #" + j;
                        String command = "PUBLISH " + queueName + " " + message;
                        long sendTime = System.nanoTime();
                        out.println(command);
                        String response = in.readLine();
                        long receiveTime = System.nanoTime();
                        long latency = (receiveTime - sendTime) / 1_000_000; // 毫秒
                        publishLatencies.add(latency);
                        publishedCount.incrementAndGet();
                    }
                } catch (IOException e) {
                    System.err.println("Publish error: " + e.getMessage());
                }
            }));
        }

        // 提交消费任务，持续从队列中消费直到达到总消息数
        List<Future<?>> consumerFutures = new ArrayList<>();
        for (int i = 0; i < NUM_CONSUMER_THREADS; i++) {
            int ii = i;
            consumerFutures.add(consumerExecutor.submit(() -> {
                // 每个线程有一个独立的连接
                try (Socket socket = new Socket(SERVER_HOST, SERVER_PORT);
                     BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                     PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                    String queueName = QUEUE_NAME + "_" + (ii / THREADS_NUM_PER_QUEUE);
                    while (consumedCount.get() < totalMessages) {
                        String command = "CONSUME " + queueName;
                        long sendTime = System.nanoTime();
                        out.println(command);
                        String response = in.readLine();
                        long receiveTime = System.nanoTime();
                        long latency = (receiveTime - sendTime) / 1_000_000; // 毫秒
                        // 如果返回的响应以 "MESSAGE:" 开头，则说明成功消费了一条消息
                        if (response != null && response.startsWith("MESSAGE:")) {
                            consumeLatencies.add(latency);
                            consumedCount.incrementAndGet();
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Consume error: " + e.getMessage());
                }
            }));
        }

        // 等待所有发布任务完成
        for (Future<?> future : publisherFutures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                System.err.println("Publisher task error: " + e.getMessage());
            }
        }
        publisherExecutor.shutdown();

        // 等待所有消费任务完成
        while (consumedCount.get() < totalMessages) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        consumerExecutor.shutdown();

        long endTime = System.currentTimeMillis();
        long totalTimeMillis = endTime - startTime;

        // 计算吞吐量：单位秒内发布的消息数
        double throughput = publishedCount.get() / (totalTimeMillis / 1000.0);

        // 计算平均发布延迟
        long sumPublishLatency = publishLatencies.stream().mapToLong(Long::longValue).sum();
        double avgPublishLatency = publishLatencies.isEmpty() ? 0 : sumPublishLatency / (double) publishLatencies.size();

        // 计算平均消费延迟
        long sumConsumeLatency = consumeLatencies.stream().mapToLong(Long::longValue).sum();
        double avgConsumeLatency = consumeLatencies.isEmpty() ? 0 : sumConsumeLatency / (double) consumeLatencies.size();

        // 将测试结果写入报告文件
        String dateTimePattern = "yyyy-MM-dd_HH-mm-ss";
        SimpleDateFormat dateTimeFormat  = new SimpleDateFormat(dateTimePattern);
        String dateString = dateTimeFormat.format(new Date());
        String reportFileName = "report/performance_report_" + dateString + ".txt";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(reportFileName))) {
            writer.write("MessageQueueServer 性能测试报告\n");
            writer.write("===================================\n");
            writer.write("总耗时 (ms): " + totalTimeMillis + "\n");
            writer.write("发布线程数：" + NUM_PUBLISHER_THREADS + "\n");
            writer.write("消费线程数：" + NUM_CONSUMER_THREADS + "\n");
            writer.write("每条队列连接数：" + THREADS_NUM_PER_QUEUE + "\n");
            writer.write("总发布消息数: " + publishedCount.get() + "\n");
            writer.write("总消费消息数: " + consumedCount.get() + "\n");
            writer.write(String.format("吞吐量 (消息/秒): %.2f\n", throughput));
            writer.write(String.format("平均发布延迟 (ms): %.2f\n", avgPublishLatency));
            writer.write(String.format("平均消费延迟 (ms): %.2f\n", avgConsumeLatency));
            writer.write("测试结束\n");
        } catch (IOException e) {
            System.err.println("写入报告失败: " + e.getMessage());
        }

        System.out.println("测试完成，报告已写入 " + reportFileName);
    }
}
