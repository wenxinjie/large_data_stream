package streaming.Kafka;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class LogProcessBolt extends BaseRichBolt {

    private  OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            byte[] binaryByField = input.getBinaryByField("bytes"); // This field is defined by KafkaSpout
            String value = new String(binaryByField);

            System.out.println("******" + value + "******");


            String[] splits = value.split(" ");
            String[] tmp = splits[0].split(",");

            String lon = tmp[0];
            String lat = tmp[1];
            String timestamp = splits[1];

            System.out.println("lon: "+lon+", lat: "+lat+", timestamp: "+timestamp);

            this.collector.emit(new Values(Long.parseLong(timestamp), Double.parseDouble(lon), Double.parseDouble(lat)));
            this.collector.ack(input);
        } catch (Exception e) {
            System.out.println(e);
            this.collector.fail(input);
        }


    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("time", "lon", "lat"));

    }
}
