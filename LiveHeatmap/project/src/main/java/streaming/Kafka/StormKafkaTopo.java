package streaming.Kafka;

import com.google.common.collect.Maps;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Map;
import java.util.UUID;

public class StormKafkaTopo {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        BrokerHosts hosts = new ZkHosts("ubuntu:2181");
        String topic = "project_topic";
        String zkRoot = "/" + topic;
        String id = UUID.randomUUID().toString();

        SpoutConfig spoutconfig = new SpoutConfig(hosts, topic, zkRoot, id);

        KafkaSpout kafkaSpout = new KafkaSpout(spoutconfig);
        spoutconfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();

        String SPOUT_ID = KafkaSpout.class.getSimpleName();
        builder.setSpout(SPOUT_ID, kafkaSpout);

        String BOLT_ID = LogProcessBolt.class.getSimpleName();
        builder.setBolt(BOLT_ID, new LogProcessBolt()).shuffleGrouping(SPOUT_ID);

        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost/storm");
        hikariConfigMap.put("dataSource.user","root");
        hikariConfigMap.put("dataSource.password","zyfei9407");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

        String tableName = "stat";
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withTableName(tableName)
                .withQueryTimeoutSecs(30);

        builder.setBolt("JdbcInsertBolt", userPersistanceBolt).shuffleGrouping(BOLT_ID);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(StormKafkaTopo.class.getSimpleName(), new Config(), builder.createTopology());
    }
}
