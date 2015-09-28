package net.digitalbebop;

import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.FiberForkJoinScheduler;
import co.paralleluniverse.fibers.FiberScheduler;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.strands.Strand;
import co.paralleluniverse.strands.channels.Channel;
import co.paralleluniverse.strands.channels.Channels;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class Crawler {
    private static Logger logger = LogManager.getLogger(Crawler.class);

    private static final int FIBER_PARALLELISM = 1000;
    private static final int CHANNEL_SIZE = 100;

    private final ReentrantLock cacheLock = new ReentrantLock();
    private final HashMap<String, Boolean> lookupCache = new HashMap<>();

    private final Channel<String> workChannel = Channels.newChannel(CHANNEL_SIZE, Channels.OverflowPolicy.BLOCK);
    private final FiberScheduler sched = new FiberForkJoinScheduler("CrawlerChildren", FIBER_PARALLELISM);

    private final List<Function> dirFunctions = new ArrayList<>();
    private final List<Function> fileFunctions = new ArrayList<>();

    private final String origin;
    private WatchService watchService;

    /**
     * Construct new Crawler on origin path. This crawler will attempt to initialize a {@link WatchService}
     * on the default filesystem heirarchy, which under an exception will be immediately thrown through the
     * constructor as an {@link IOException}.
     * @param origin Starting point for crawler
     * @throws IOException
     */
    public Crawler(final String origin) throws IOException {
        this.watchService = FileSystems.getDefault().newWatchService();
        this.origin = origin;
        logger.info("Origin: " + origin);
    }

    public void start() throws SuspendExecution, InterruptedException {
        workChannel.send(origin);
        mainFiber.start();
        watchThread.start();
    }

    public <R> void addFileCallback(Function<Path, R> fn) {
        fileFunctions.add(fn);
    }

    public <R> void addDirectoryCallback(Function<Path, R> fn) {
        dirFunctions.add(fn);
    }

    private boolean processFile(String absPath) {
        cacheLock.lock();
        Boolean prev = lookupCache.putIfAbsent(absPath, true);
        cacheLock.unlock();

        if (prev != null) {
            logger.debug("Already indexed: " + absPath);
            return false;
        }

        Path jpath = Paths.get(absPath);
        try {
            File fn = jpath.toFile();
            if (fn.isDirectory()) {
                logger.debug("Registering: " + fn.getAbsolutePath());
                jpath.register(watchService,
                        StandardWatchEventKinds.ENTRY_CREATE,
                        StandardWatchEventKinds.ENTRY_DELETE,
                        StandardWatchEventKinds.ENTRY_MODIFY);
            } else {
                logger.debug("The file: " + jpath.toString() + " was not a directory");
            }
        } catch (IOException e) {
            logger.error("Failed to process file event watch: " + e.getLocalizedMessage(), e);
            return false;
        }

        return true;
    }

    private List<String> handleMessage(String msg) throws SuspendExecution {
        final List<String> pathsOut = new ArrayList<>();
        final File rootFile = new File(msg);

        if (!rootFile.exists()) {
            logger.error("Root path does not exist: " + msg);
            return pathsOut;
        }

        if (!rootFile.canRead()) {
            logger.error("Root path is not readable: " + msg);
            return pathsOut;
        }

        if (rootFile.isDirectory()) {
            logger.debug("Got directory: " + rootFile);
            final File[] files;

            files = rootFile.listFiles();
            if (files != null) {
                for (File file : files) {
                    final String absPath = file.getAbsolutePath();
                    pathsOut.add(absPath);
                }
            }
        }

        return pathsOut;
    }

    /**
     * Because we haven't yet implemented a file watch utility that utilizes fibers, we have to
     * run the watch routine in its own thread. If we don't we'll block the thread that the fiber
     * is running on while we're trying to parse files from an origin.
     *
     * We can still use fibers to do our dirty work, but we must not stop the other thread from
     * working while we wait for stuff.
     */
    private Thread watchThread = new Thread() {
        @Override
        public void run() {
            if (watchService != null) {
                for (;;) {
                    logger.info("Watcher is getting a result");
                    WatchKey key;
                    try {
                        key = watchService.take();
                    } catch (InterruptedException e) {
                        logger.warn("Interrupted while waiting for event");
                        continue;
                    }

                    for (WatchEvent event : key.pollEvents()) {
                        logger.info("Event: " + event.toString());
                    }

                    logger.info("Got: " + key.toString());
                    boolean valid = key.reset();
                    if (!valid) {
                        logger.info("Key no longer valid: " + key.toString());
                    }
                }
            } else {
                throw new RuntimeException("Watch Service Not Initialized");
            }
        }
    };

    private Fiber<Void> mainFiber = new Fiber<Void>(() -> {
        for (;;) {
            final String msg = workChannel.receive();
            processFile(msg);
            for (String result : handleMessage(msg)) {
                boolean queued = workChannel.trySend(result);
                if (!queued) {
                    Fiber<Void> cont = new Fiber<Void>("ChildSubmitter", sched, () -> {
                        logger.debug("Spawned: " + Strand.currentStrand().getName() + " for " + result);
                        if(processFile(result)) {
                            for (String childResult : handleMessage(result)) {
                                workChannel.send(childResult);
                            }
                        }
                    }).start();
                }
            }
        }
    });
}
