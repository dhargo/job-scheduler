package services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.concurrent.*;

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

    private final static Logger LOGGER = LoggerFactory.getLogger(SimpleFutureJobService.class);

    private final PriorityBlockingQueue<SequentialJob> queue = new PriorityBlockingQueue<>();
    private final JobProcessor jobProcessor = new JobProcessor(queue, "JobProcessor");

    public SimpleFutureJobService() {
        jobProcessor.start();
    }

    @Override
    public void schedule(LocalDateTime executeAt, Callable task) {
        LOGGER.debug("schedule job to run at {}", executeAt);
        queue.offer(new SequentialJob(task, executeAt));
        synchronized (jobProcessor) {
            jobProcessor.notify();
        }
    }

}
