package client;


import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);

        Client.connect("myKeySpace", "localhost");
        StoreTicksInCassandra.store();
    }
}
