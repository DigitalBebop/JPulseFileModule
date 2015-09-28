package net.digitalbebop;

import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.strands.Strand;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class Main {
    private static Logger logger = LogManager.getLogger(Main.class);

    public static final String ModuleName = "JPulseFileModule";

    public static void main(String[] args) throws SuspendExecution, InterruptedException, IOException {
        final Options options = new Options();

        logger.info("Starting file crawler....");
        Crawler crawler = new Crawler(args[0]);
        crawler.start();


        for(;;) {
            Strand.sleep(1000);
        }
    }
}
