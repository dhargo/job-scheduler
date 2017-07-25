package services;

import java.time.LocalDateTime;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

class Job implements Comparable<Job> {

    private static final AtomicLong seq = new AtomicLong(0);

    private final Callable task;
    private final LocalDateTime time;
    private final long seqNum;

    Job(Callable task, LocalDateTime time) {
        this.task = task;
        this.time = time;
        this.seqNum = seq.getAndIncrement();
    }

    public Callable getTask() {
        return task;
    }

    public LocalDateTime getTime() {
        return time;
    }

    @Override
    public int compareTo(Job that) {
        int result = this.time.compareTo(that.time);
        if (result == 0) result = Long.compare(this.seqNum, that.seqNum);
        return result;
    }

}
