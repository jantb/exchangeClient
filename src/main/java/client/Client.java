package client;

import static com.datastax.driver.core.querybuilder.QueryBuilder.truncate;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {
    private static final Logger log = LoggerFactory.getLogger(Client.class);
    private static Cluster cluster;
    private static Session session;
    private static CountDownLatch countDownLatch = new CountDownLatch(1);
    private static String keySpace;

    public static void connect(String keySpace, String... nodes) throws InterruptedException {
        connect(keySpace, 1, nodes);
    }

    public static void connect(String keySpace, int replicationFactor, String... nodes) throws InterruptedException {
        Client.keySpace = keySpace;
        boolean connected = false;
        while (!connected) {
            for (String node : nodes) {
                cluster = Cluster.builder()
                        .addContactPoint(node)
                        .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                        .build();
                try {
                    session = cluster.connect(keySpace);
                    connected = true;
                    break;
                } catch (Exception e) {
                    try {
                        Session clusterSession = cluster.connect();
                        clusterSession.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH " +
                                "replication = {'class': 'SimpleStrategy', 'replication_factor' : %d};", keySpace, replicationFactor));
                        clusterSession.close();
                    } catch (NoHostAvailableException| RejectedExecutionException e1) {
                       //Ignored try next node
                    } catch (Exception e1) {
                        e1.printStackTrace();
                    }
                }
            }
            if (!connected) {
                log.info("Unable to connect to" + Arrays.toString(nodes));
                Thread.sleep(1000L);
            } else {
                log.info("Connected to cluster: {} {}", cluster.getClusterName(), cluster.getMetadata().getAllHosts());

                try {
                    updateKeyspaceTables();
                    connected = true;
                    break;
                } catch (IOException | NoSuchAlgorithmException e1) {
                    e1.printStackTrace();
                }

            }
        }
        countDownLatch.countDown();
    }

    private static void updateKeyspaceTables() throws IOException, NoSuchAlgorithmException {
        session.execute("create table if not exists migrations(" +
                "table_name text," +
                "md5 blob," +
                "primary key (table_name))");
        ResultSet migrations = session.execute(QueryBuilder.select().from("migrations"));
        HashMap<String, ByteBuffer> migrationsMap = new HashMap<>();
        for (Row migration : migrations) {
            migrationsMap.put(migration.getString("table_name"), migration.getBytes("md5"));
        }

        DirectoryStream<Path> resources = Files.newDirectoryStream(Paths.get("src/main/resources/cql"));
        for (Path resource : resources) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Files.copy(resource, out);
            String filename = resource.getFileName().toString();
            String tableName = filename.substring(0, filename.indexOf(".cql"));
            ByteBuffer byteBuffer = migrationsMap.get(tableName);
            if (byteBuffer == null || byteBuffer.compareTo(ByteBuffer.wrap(MessageDigest.getInstance("MD5").digest(out.toByteArray()))) != 0) {
                log.info("Executing cql from file " + resource.getFileName());
                log.info("drop table if exists " + tableName);
                session.execute("drop table if exists " + tableName);
                log.info(out.toString("utf-8"));
                session.execute(out.toString("utf-8"));
                session.execute(QueryBuilder.insertInto("migrations")
                        .value("table_name", tableName)
                        .value("md5", ByteBuffer.wrap(MessageDigest.getInstance("MD5").digest(out.toByteArray()))));
            }
        }
    }

    public static Session getSession() {
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return session;
    }

    public static void close() {
        cluster.close();
    }

    public static void truncTables() {
        KeyspaceMetadata keyspaceMetadata = session.getCluster().getMetadata().getKeyspace(keySpace);
        ArrayList<String> tablesCql = new ArrayList<>();
        tablesCql.addAll(keyspaceMetadata.getTables().stream().map(TableMetadata::asCQLQuery).collect(Collectors.toList()));
        session.execute(String.format("DROP KEYSPACE IF EXISTS test%s", keySpace));
        session.execute(String.format("CREATE KEYSPACE IF NOT EXISTS test%s WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}", keySpace));
        session = session.getCluster().connect(String.format("test%s", keySpace));
        for (String table : tablesCql) {
            String query = table.replaceFirst(keySpace + ".", "");
            System.out.println(query);
            session.execute(query);
        }

        KeyspaceMetadata testMetadata = session.getCluster().getMetadata().getKeyspace(String.format("test%s", keySpace));
        Collection<TableMetadata> tables = testMetadata.getTables();
        for (TableMetadata table : tables) {
            session.execute(truncate(table.getName()));
        }
    }
}