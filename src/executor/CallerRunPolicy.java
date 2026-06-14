package executor;

import java.util.concurrent.RejectedExecutionException;

public class CallerRunsPolicy implements RejectionHandler {

    @Override
    public void reject(Runnable task, CustomThreadPoolExecutor executor) {

        if (!executor.isShutdown()) {

            PoolLogger.log("Отказ",
                    "Пул перегружен. Задача будет выполнена в вызывающем потоке: "
                            + Thread.currentThread().getName());

            task.run();

        } else {
            PoolLogger.log("Отказ",
                    "Пул завершён. Задача отклонена.");
            throw new RejectedExecutionException("Пул остановлен");
        }
    }
}