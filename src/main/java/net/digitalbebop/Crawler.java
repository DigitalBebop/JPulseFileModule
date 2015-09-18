package net.digitalbebop;

import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.strands.channels.Channel;
import co.paralleluniverse.strands.channels.Channels;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.UserPrincipal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class Crawler {
    private static Logger logger = LogManager.getLogger(Crawler.class);

    private static final Channel<String> workChannel = Channels.newChannel(100, Channels.OverflowPolicy.BLOCK);
    private static final ReentrantLock cacheLock = new ReentrantLock();
    private static final HashMap<String, Boolean> lookupCache = new HashMap<>();
    private final String origin;

    public Crawler(final String origin) {
        this.origin = origin;
        logger.info("Origin: " + origin);
    }

    private void enqueue(@Nonnull String path) throws SuspendExecution {
        logger.debug("Enqueue: " + path);
        try {
            logger.info("Queueing: " + path);
            workChannel.send(path);
            logger.info("Finished queueing: " + path);
        } catch (InterruptedException ignored) {}
    }

    public void start() throws SuspendExecution {
        enqueue(origin);
        mainFiber.start();
    }

    private boolean processFile(String absPath) {
        String owner = "unknown";

        cacheLock.lock();
        Boolean prev = lookupCache.putIfAbsent(absPath, true);
        cacheLock.unlock();

        if (prev != null) {
            logger.debug("Already indexed: " + absPath);
            return false;
        }

        Path jpath = Paths.get(absPath);
        try {
            UserPrincipal princ = Files.getOwner(jpath);
            owner = princ.getName();
        } catch (IOException e) {
            logger.error("Failed to get owner of File: " + e.getLocalizedMessage(), e);
        }

        final net.digitalbebop.ClientRequests.IndexRequest.Builder builder = net.digitalbebop.ClientRequests.IndexRequest.newBuilder();

        final String metaTags = PulseFileUtils.getMetaTags(jpath);
        builder.setUsername(owner)
                .setModuleName(Main.ModuleName)
                .setModuleId(absPath)
                .setMetaTags(metaTags)
                .setIndexData("")
                .setRawData(ByteString.copyFrom(new byte[0]))
                .setTimestamp(new Date().getTime())
                .setLocation("");

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
            final File[] files;

            files = rootFile.listFiles();
            if (files != null) {
                for (File file : files) {
                    final String absPath = file.getAbsolutePath();
                    final boolean processed = processFile(absPath);
                    if (processed) {
                        pathsOut.add(absPath);
                    }
                }
            }
        } else {
            logger.info("Processing: " + msg);
            processFile(msg);
        }

        return pathsOut;
    }

    private Fiber<Void> mainFiber = new Fiber<Void>(() -> {
        for (;;) {
            final String msg = workChannel.receive();
            final List<String> results = handleMessage(msg);

            for (String result : results) {
                boolean queued = workChannel.trySend(result);
                if (!queued) {
                    logger.info("Didn't queue: " + result);
                    processFile(result);
                }
            }
        }
    });
}
