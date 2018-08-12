package streaming;




import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class LocalSumTopo {

    /**
     * generate data and emit
     */
    public static class DataSourceSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;
        int num = 0;


        /**
         * initialize
         * @param conf 参数
         * @param context 上下文
         * @param collector 数据发射器
         */
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }


        /**
         * 从消息队列中获取数据
         * 是一个死循环
         */
        public void nextTuple() {
            this.collector.emit(new Values(++num));

            System.out.println("Spout: num = " + num);

            Utils.sleep(1000);
        }


        /**
         * 声明输出字段
         * @param declarer
         */
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("number"));
        }

    }

    public static class SumBolt extends BaseRichBolt {

        int sum = 0;


        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        }

        /**
         * 也是一个死循环，获取Spout发来的数据
         * @param input
         */
        public void execute(Tuple input) {
            Integer value = input.getIntegerByField("number");
            sum += value;

            System.out.println("Bolt: sum = " + sum);
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("SumBolt", new SumBolt()).shuffleGrouping("DataSourceSpout");

        // 创建一个本地模式的Storm集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalSumTopo", new Config(), builder.createTopology());


    }
}
