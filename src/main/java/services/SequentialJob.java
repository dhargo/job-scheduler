package services;

import java.time.LocalDateTime;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Обеспечивает упорядоченность выполнения задач при помощи интерфейса {@link Comparable}, и использовании
 * в методе {@link #compareTo(SequentialJob)} серийного номера ({@link #seq}).
 * */
class SequentialJob implements Comparable<SequentialJob> {

    private static final AtomicLong seq = new AtomicLong(0);

    private final Callable task;
    private final LocalDateTime time;
    private final long seqNum;

    SequentialJob(Callable task, LocalDateTime time) {
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
    public int compareTo(SequentialJob that) {
        int result = this.time.compareTo(that.time);
        if (result == 0) result = Long.compare(this.seqNum, that.seqNum);
        return result;
    }

}
