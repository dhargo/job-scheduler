package services;

import java.time.LocalDateTime;
import java.util.concurrent.Callable;

public interface FutureJobService {
    void schedule(LocalDateTime dateTime, Callable task);
}
