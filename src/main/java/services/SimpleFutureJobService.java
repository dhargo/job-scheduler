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
 * поэтому нельзя так просто взять и использовать ScheduledThreadPoolExecutor - очевидно есть строгое требование
 * выполнять задачи в порядке прихода, поэтому в решении используется один поток для выполнения задач.
 */
public class SimpleFutureJobService implements FutureJobService {

    private final static Logger LOGGER = Logger.getLogger(SimpleFutureJobService.class.getName());

    private final PriorityBlockingQueue<Job> queue = new PriorityBlockingQueue<>();
    private final JobProcessor jobProcessor = new JobProcessor(queue, "JobProcessor");

    public SimpleFutureJobService() {
        jobProcessor.start();
    }

    @Override
    public void schedule(LocalDateTime executeAt, Callable task) {
        LOGGER.log(Level.FINEST, String.format("schedule job to run at %s", executeAt));
        queue.offer(new Job(task, executeAt));
        synchronized (jobProcessor) {
            jobProcessor.notify();
        }
    }

}
