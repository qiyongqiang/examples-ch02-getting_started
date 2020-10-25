package spouts;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class KafkaReader extends BaseRichSpout {
    private SpoutOutputCollector collector;
    KafkaConsumer<String, String> consumer;

    public void ack(Object msgId) {
        System.out.println("OK:" + msgId);
    }

    public void close() {
    }

    public void fail(Object msgId) {
        System.out.println("FAIL:" + msgId);
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        initConsumer();
    }

    private void initConsumer() {
        loadProperties();
        //1.加载配置信息
        Properties prop = loadProperties();
        consumer = new KafkaConsumer(prop);
        //1.订阅消息
        consumer.subscribe(Collections.singletonList("test1040"));
    }

    @Override
    public void nextTuple() {
        if (consumer == null) {
            initConsumer();
        }
        //2.读取消息
        ConsumerRecords<String, String> records = consumer.poll(1000);
        if (records != null && !records.isEmpty()) {
            records.forEach(x -> {
                String value = x.value();
                System.out.println("kafka-reader " + value);
                this.collector.emit(new Values(value), value);
            });
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    private Properties loadProperties() {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092,localhost:9093");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("group.id", "topologyGroup");
        prop.put("client.id", "demo-consumer-client");
        prop.put("auto.offset.reset", "earliest");        // earliest(最早) latest(最晚)
        return prop;
    }
}
