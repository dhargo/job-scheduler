package services;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Задача:
 * На вход поступают пары (LocalDateTime, Callable). Нужно реализовать систему, которая будет выполнять Callable
 * для каждого пришедшего события в указанный LocalDateTime, либо как можно скорее в случае если система перегружена
 * и не успевает все выполнять (имеет беклог). Задачи должны выполняться в порядке согласно значению LocalDateTime
 * либо в порядке прихода события для равных LocalDateTime. События могут приходить в произвольном порядке и
 * добавление новых пар (LocalDateTime, Callable) может вызываться из разных потоков.
 *
 * Комментарий:
 * В условии есть важное замечание "Задачи должны выполняться ... в порядке прихода события для равных LocalDateTime",
 * поэтому нельзя так просто взять и использовать ScheduledThreadPoolExecutor.
 */
public class SimpleFutureJobService implements FutureJobService {

    private final static Logger LOGGER = Logger.getLogger(SimpleFutureJobService.class.getName());

    private final PriorityBlockingQueue<Job> queue = new PriorityBlockingQueue<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private final Thread processor = new Thread(() -> {
        while (!Thread.interrupted()) {
            try {
                Job job = queue.take();
                if (job.getTime().isBefore(LocalDateTime.now()) || job.getTime().isEqual(LocalDateTime.now())) {
                    LOGGER.log(Level.FINEST, String.format("execute job for %s", job.getTime()));
                    executor.submit(job.getTask());
                } else {
                    queue.offer(job);
                    synchronized (this) {
                        long delay = ChronoUnit.MILLIS.between(LocalDateTime.now(), job.getTime());
                        LOGGER.log(Level.FINEST, String.format("too early for %s - wait %s ms", job.getTime(), delay));
                        wait(delay);
                    }
                }
            } catch (InterruptedException e) {
                //TODO: сделать сохранение очереди при завершении работы
            }
        }
    }, "Job Processor");

    public SimpleFutureJobService() {
        processor.start();
    }

    @Override
    public void schedule(LocalDateTime executeAt, Callable task) {
        LOGGER.log(Level.FINEST, String.format("schedule job to run at %s", executeAt));
        queue.offer(new Job(task, executeAt));
        synchronized (processor) {
            processor.notify();
        }
    }

}
