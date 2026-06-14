package executor;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CopyOnWriteArrayList;

public class CustomThreadPoolExecutor implements CustomExecutor {



    private final int corePoolSize;
    private final int maxPoolSize;
    private final int queueSize;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;

    private final List<Worker> workers = new CopyOnWriteArrayList<>();

    private final AtomicInteger roundRobinIndex = new AtomicInteger(0);

    private volatile boolean isShutdown = false;

    private final CustomThreadFactory threadFactory;

    private final RejectionHandler rejectionHandler;

    public CustomThreadPoolExecutor(int corePoolSize,
                                    int maxPoolSize,
                                    int queueSize,
                                    long keepAliveTime,
                                    TimeUnit timeUnit) {

        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.queueSize = queueSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;

        this.threadFactory = new CustomThreadFactory("МойПул");

        this.rejectionHandler = new CallerRunsPolicy();

        // Создаём базовые (core) потоки сразу
        for (int i = 0; i < corePoolSize; i++) {
            addWorker();
        }
    }

    private void addWorker() {

        if (workers.size() >= maxPoolSize) {
            return;
        }

        BlockingQueue<Runnable> queue =
                new ArrayBlockingQueue<>(queueSize);

        Worker worker = new Worker(queue);

        Thread thread = threadFactory.newThread(worker);
        worker.thread = thread;

        workers.add(worker);

        thread.start();
    }

    @Override
    public void execute(Runnable command) {

        if (command == null)
            throw new NullPointerException("Задача не может быть null");

        if (isShutdown) {
            rejectionHandler.reject(command, this);
            return;
        }

        int size = workers.size();

        if (size == 0) {
            rejectionHandler.reject(command, this);
            return;
        }

        int index = Math.abs(
                roundRobinIndex.getAndIncrement() % size
        );

        Worker worker = workers.get(index);

        if (!worker.queue.offer(command)) {

            PoolLogger.log("Пул",
                    "Очередь #" + index + " переполнена.");

            if (workers.size() < maxPoolSize) {

                PoolLogger.log("Пул",
                        "Создаём дополнительный поток из-за высокой нагрузки.");

                addWorker();
                execute(command);
                return;
            }

            rejectionHandler.reject(command, this);

        } else {
            PoolLogger.log("Пул",
                    "Задача принята в очередь #" + index);
        }
    }


    @Override
    public <T> Future<T> submit(Callable<T> callable) {

        FutureTask<T> futureTask = new FutureTask<>(callable);

        execute(futureTask);

        return futureTask;
    }

    @Override
    public void shutdown() {
        PoolLogger.log("Пул", "Инициирована корректная остановка пула.");
        isShutdown = true;
    }

    @Override
    public void shutdownNow() {

        PoolLogger.log("Пул", "Инициирована немедленная остановка пула.");

        isShutdown = true;

        for (Worker worker : workers) {
            worker.thread.interrupt();
        }
    }

    public boolean isShutdown() {
        return isShutdown;
    }


    private class Worker implements Runnable {

        private final BlockingQueue<Runnable> queue;
        private Thread thread;

        Worker(BlockingQueue<Runnable> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {

            try {

                while (true) {

                    // Если пул завершён и задач нет — выходим
                    if (isShutdown && queue.isEmpty()) {
                        break;
                    }

                    Runnable task =
                            queue.poll(keepAliveTime, timeUnit);

                    // Если задача не поступила за keepAliveTime
                    if (task == null) {

                        if (workers.size() > corePoolSize) {

                            PoolLogger.log("Рабочий поток",
                                    Thread.currentThread().getName()
                                            + " завершает работу по таймауту простоя.");

                            workers.remove(this);
                            break;
                        }

                        continue;
                    }

                    PoolLogger.log("Рабочий поток",
                            Thread.currentThread().getName()
                                    + " выполняет задачу.");

                    try {
                        task.run();
                    } catch (Exception e) {
                        PoolLogger.log("Ошибка",
                                "Во время выполнения задачи возникло исключение: "
                                        + e.getMessage());
                    }
                }

            } catch (InterruptedException ignored) {
                PoolLogger.log("Рабочий поток",
                        Thread.currentThread().getName()
                                + " был прерван.");
            }
        }
    }
}