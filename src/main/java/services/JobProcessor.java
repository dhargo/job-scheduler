package services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;

public class JobProcessor extends Thread {

    private final static Logger LOGGER = LoggerFactory.getLogger(JobProcessor.class);

    /**
     * Очередь должна упорядочевать задачи как по времени так и по мере поступления элеметнов, поэтому элементы очереди
     * должны реализовать интерфейс {@link Comparable} в соответствии с контрактом {@link SimpleFutureJobService}.
     * */
    private final PriorityBlockingQueue<SequentialJob> queue;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public JobProcessor(PriorityBlockingQueue<SequentialJob> queue, String name) {
        super(name);
        this.queue = queue;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                SequentialJob job = queue.take();
                long delay = ChronoUnit.MILLIS.between(LocalDateTime.now(), job.getTime());
                if (delay <= 0) {
                    LOGGER.debug("execute job for {}", job.getTime());
                    executor.submit(job.getTask());
                } else {
                    queue.offer(job);
                    synchronized (this) {
                        LOGGER.debug("too early for {} - wait {} ms", job.getTime(), delay);
                        wait(delay);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.info("Interrupted");
            } finally {
                //TODO: сделать сохранение очереди при завершении работы
            }
        }
        LOGGER.info("Stopped");
    }

}
