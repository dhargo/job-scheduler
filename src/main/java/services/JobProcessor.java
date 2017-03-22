package services;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JobProcessor extends Thread {

    private final static Logger LOGGER = Logger.getLogger(JobProcessor.class.getName());

    private final PriorityBlockingQueue<Job> queue;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public JobProcessor(PriorityBlockingQueue<Job> queue, String name) {
        super(name);
        this.queue = queue;
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            try {
                Job job = queue.take();
                if (job.getTime().isBefore(LocalDateTime.now()) || job.getTime().isEqual(LocalDateTime.now())) {
                    if (LOGGER.isLoggable(Level.FINEST)) {
                        LOGGER.log(Level.FINEST, String.format("execute job for %s", job.getTime()));
                    }
                    executor.submit(job.getTask());
                } else {
                    queue.offer(job);
                    synchronized (this) {
                        long delay = ChronoUnit.MILLIS.between(LocalDateTime.now(), job.getTime());
                        if (LOGGER.isLoggable(Level.FINEST)) {
                            LOGGER.log(Level.FINEST, String.format("too early for %s - wait %s ms", job.getTime(), delay));
                        }
                        wait(delay);
                    }
                }
            } catch (InterruptedException e) {
                //TODO: сделать сохранение очереди при завершении работы
            }
        }
    }

}
