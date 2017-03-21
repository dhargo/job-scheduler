package services;

import org.junit.Assert;
import org.junit.Before;

import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Callable;

public class SimpleFutureTaskServiceTest {

    private SimpleFutureJobService jobService;

    private Queue<Integer> testResult;

    @Before
    public void setUp() throws Exception {
        jobService = new SimpleFutureJobService();
        testResult = new LinkedList<>();
    }

    @org.junit.Test
    public void testScheduleOneTask() throws Exception {
        LocalDateTime executeAt = LocalDateTime.now();
        jobService.schedule(executeAt, () -> testResult.add(1));
        Thread.sleep(1000);
        Assert.assertEquals("executed only one time", 1, testResult.size());
        Assert.assertEquals("executed first", Integer.valueOf(1), testResult.poll());
    }

    @org.junit.Test
    public void testScheduleTaskInPast() throws Exception {
        LocalDateTime executeAt = LocalDateTime.now().minusSeconds(1000);
        jobService.schedule(executeAt, () -> testResult.add(1));
        Thread.sleep(1000);
        Assert.assertEquals("executed only one time", 1, testResult.size());
        Assert.assertEquals("executed first", Integer.valueOf(1), testResult.poll());
    }

    @org.junit.Test
    public void testScheduleStormTasksSequence() throws Exception {
        LocalDateTime executeAt = LocalDateTime.now().plusSeconds(1);
        int testCount = 100;
        for (int i = 0; i < testCount; i++) {
            jobService.schedule(executeAt, new TestTask(i));
        }
        Thread.sleep(2000);
        Assert.assertEquals("executed " + testCount + " times", testCount, testResult.size());
        for (int i = 0; i < testCount; i++) {
            Assert.assertEquals("executed with " + i, Integer.valueOf(i), testResult.poll());
        }
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

}