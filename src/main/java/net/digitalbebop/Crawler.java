package net.digitalbebop;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Crawler {
    private static Logger logger = LogManager.getLogger(Crawler.class);

    private static final Object signal = new Object();

    private static final AtomicInteger active = new AtomicInteger(0);
    private static final ConcurrentHashMap<String, Boolean> lookupCache = new ConcurrentHashMap<>();
    private static final ConcurrentLinkedQueue<String> workq = new ConcurrentLinkedQueue<>();

    private static final int NCORES = Runtime.getRuntime().availableProcessors();
    private static final int CORE_MULTIPLIER = 2;

    private final String origin;
    private final ExecutorService service = Executors.newCachedThreadPool();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final int parallelism;

    private Crawler() {
        throw new IllegalStateException("Use of private constructor");
    }

    public Crawler(final String origin) {
        this(origin, NCORES*CORE_MULTIPLIER);
    }

    public Crawler(final String origin, int parallelism) {
        this.parallelism = parallelism;
        this.origin = origin;
    }

    public void enqueue(String path) {
        workq.add(path);
        synchronized (signal) {
            signal.notifyAll();
        }
    }

    public void start() {
        boolean notStarted = started.compareAndSet(false, true);
        if (!notStarted) {
            throw new RuntimeException("Crawler already started.");
        }

        enqueue(origin);
        for (int idx=0; idx < parallelism; ++idx) {
            service.submit(new CrawlerWorker());
        }
    }

    private class CrawlerWorker implements Runnable {
        @Override
        public void run() {
            String path;

            int added = active.incrementAndGet();
            logger.info("Added: " + added);

            for (;;) {
                while ((path = workq.poll()) == null) {
                    try {
                        synchronized (signal) {
                            int count = active.decrementAndGet();
                            logger.info("Active: " + count);
                            if (count == 0) {
                                signal.notifyAll();
                                return;
                            }

                            if (count < 0) {
                                throw new RuntimeException("Ya Dingus");
                            }

                            signal.wait();
                            if (active.get() == 0) {
                                return;
                            }

                            int activated = active.incrementAndGet();
                            logger.info("Activated: " + activated);
                        }
                    } catch (InterruptedException ignored) {}
                }


                final File rootFile = new File(path);
                if (!rootFile.exists()) {
                    throw new RuntimeException("Root path does not exist: " + path);
                }

                if (!rootFile.canRead()) {
                    throw new RuntimeException("Root path is not readable: " + path);
                }

                logger.debug("Looking at " + rootFile.getAbsolutePath());
                if (rootFile.isDirectory()) {
                    File[] files = rootFile.listFiles();
                    if (files != null) {
                        for (File file : files) {
                            final String absPath = file.getAbsolutePath();
                            Boolean prev = lookupCache.putIfAbsent(absPath, true);
                            if (prev != null) {
                                logger.debug("Already indexed: " + absPath);
                                continue;
                            }

                            enqueue(absPath);
                        }
                    }

                    logger.debug("Finished: " + path);
                }
            }
        }
    }
}
