package executor;

import java.util.concurrent.*;

public class Main {

    public static void main(String[] args) throws Exception {

        CustomThreadPoolExecutor pool =
                new CustomThreadPoolExecutor(
                        2,      // corePoolSize
                        4,      // maxPoolSize
                        5,      // queueSize на каждую очередь
                        5,      // keepAliveTime
                        TimeUnit.SECONDS
                );


        for (int i = 1; i <= 15; i++) {

            int taskId = i;

            pool.execute(() -> {
                PoolLogger.log("Задача",
                        "Задача " + taskId + " началась.");

                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ignored) {}

                PoolLogger.log("Задача",
                        "Задача " + taskId + " завершилась.");
            });
        }

        Thread.sleep(15000);

        pool.shutdown();
    }
}