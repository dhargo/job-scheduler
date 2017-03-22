package services;

import org.junit.Assert;
import org.junit.Before;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SimpleFutureJobServiceTest {

    private SimpleFutureJobService jobService;

    private Queue<Integer> testResult;

    @Before
    public void setUp() throws Exception {
        jobService = new SimpleFutureJobService();
        testResult = new LinkedList<>();
    }

    @org.junit.Test
    public void testScheduleOneJob() throws Exception {
        LocalDateTime executeAt = LocalDateTime.now();
        jobService.schedule(executeAt, () -> testResult.add(1));
        Thread.sleep(1000);
        Assert.assertEquals("executed only one time", 1, testResult.size());
        Assert.assertEquals("executed first", Integer.valueOf(1), testResult.poll());
    }

    @org.junit.Test
    public void testScheduleInPast() throws Exception {
        LocalDateTime executeAt = LocalDateTime.now().minusSeconds(1000);
        jobService.schedule(executeAt, () -> testResult.add(1));
        Thread.sleep(1000);
        Assert.assertEquals("executed only one time", 1, testResult.size());
        Assert.assertEquals("executed first", Integer.valueOf(1), testResult.poll());
    }

    @org.junit.Test
    public void testStormOrderedSequence() throws Exception {
        LocalDateTime executeAt = LocalDateTime.now().plusSeconds(1);
        int testCount = 100;
        List<Integer> checkList = IntStream.range(0, 100).boxed().collect(Collectors.toList());
        for (Integer i : checkList) {
            jobService.schedule(executeAt, new TestTask(i));
        }
        Thread.sleep(2000);
        Assert.assertEquals("executed " + testCount + " times", testCount, testResult.size());
        for (Integer i : checkList) {
            Assert.assertEquals("executed in order: with " + i, i, testResult.poll());
        }
    }

    @org.junit.Test
    public void testConcurrency() throws Exception {
        int testCount = 100;
        int threadsCount = 4;
        Set<Integer> checkList = IntStream.range(0, testCount).boxed().collect(Collectors.toSet());
        ConcurrentLinkedQueue<Integer> sharedQueue = new ConcurrentLinkedQueue<>(checkList);
        LocalDateTime executeAt = LocalDateTime.now().plusSeconds(1);
        for (int i = 0; i < threadsCount; i++) {
            new Thread(new Tester(sharedQueue, executeAt), "TesterThread" + i).start();
        }
        Thread.sleep(2000);
        Assert.assertEquals("executed " + testCount + " times", testCount, testResult.size());
        while (!testResult.isEmpty()) {
            Integer x = testResult.poll();
            boolean exist = checkList.remove(x);
            Assert.assertTrue("executed with " + x, exist);
        }
        Assert.assertEquals("executed all entries", 0, checkList.size());

    }

    private class TestTask implements Callable {
        int x;

        TestTask(int x) {
            this.x = x;
        }

        @Override
        public Object call() throws Exception {
            testResult.offer(x);
            return null;
        }
    }

    private class Tester implements Runnable {
        Queue<Integer> sharedQueue;
        LocalDateTime executeAt;

        Tester(Queue<Integer> sharedQueue, LocalDateTime executeAt) {
            this.sharedQueue = sharedQueue;
            this.executeAt = executeAt;
        }

        @Override
        public void run() {
            while (!sharedQueue.isEmpty()) {
                Integer x = sharedQueue.poll();
                if (x != null) {
                    jobService.schedule(executeAt, new TestTask(x));
                } else {
                    break;
                }
            }
        }
    }

}