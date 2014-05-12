package client;


import java.net.URL;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoreTicksInCassandra {
    private static Logger log = LoggerFactory.getLogger(StoreTicksInCassandra.class);
    private static Session session = Client.getSession();

    public static void store() {
        new Thread(() -> {
            try {
                new TickReaderService().getTickStream(tick -> {
                    log.info(String.valueOf(tick));
                    String[] names = {"eventtime","ask", "bid"};
                    Object[] values = {DateUtils.getDate(tick.getDateTime()),tick.getAsk(), tick.getBid()};
                    session.execute(QueryBuilder.insertInto("timeseries").values(names, values));
                }, new URL("http://exchange.warm.coffee/api/stream").openStream());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }
}
