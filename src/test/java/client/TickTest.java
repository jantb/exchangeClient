package client;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class TickTest {

    private Session session;

    @Before
    public void setUp() throws Exception {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);
        Client.connect("mykeyspace", "localhost");
        Client.truncTables();
        session = Client.getSession();
    }

    @Test
    public void testInsertSomeData() throws Exception {
        String[] names = {"eventtime", "ask", "bid"};
        Object[] values = {DateUtils.getDate(LocalDateTime.now()), new BigDecimal("1"), new BigDecimal("2")};
        session.execute(QueryBuilder.insertInto("timeseries").values(names, values));
        Row row = session.execute("select * from timeseries").one();

        Assertions.assertThat(row.getDecimal("ask")).isEqualByComparingTo(new BigDecimal("1"));
    }
}