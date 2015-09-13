package net.digitalbebop;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {
    private static Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        logger.info("Starting file crawler....");
        Crawler crawler = new Crawler(args[0]);
        crawler.start();
    }
}
