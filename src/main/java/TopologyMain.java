import bolts.WordCounter;
import bolts.WordNormalizer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import spouts.KafkaReader;


public class TopologyMain {
    public static void main(String[] args) throws InterruptedException {

        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new KafkaReader());
        builder.setBolt("word-normalizer", new WordNormalizer())
                .shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter(), 1)
                .fieldsGrouping("word-normalizer", new Fields("word"));

        //Configuration
        Config conf = new Config();
//        conf.put("wordsFile", args[0]);
//        conf.put("wordsFile", "/Users/zhujian/project/stormDemo/examples-ch02-getting_started/src/main/resources/words.txt");
//        conf.put("wordsFile", "words.txt");//集群路径,傻逼了,这个不能在集群上跑,或者改成一个文件或者其他topic形式,尼玛 ！！！！
        conf.setDebug(true);
        //Topology run
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        //topology本地运行
        String topologyName = "topology-03";

        LocalCluster cluster = null;
        try {
            cluster = new LocalCluster();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (cluster != null) {
                cluster.submitTopology(topologyName, conf, builder.createTopology());
            }
        } catch (TException e) {
            e.printStackTrace();
        }
        Thread.sleep(5000000);
        assert cluster != null;
        cluster.shutdown();

        //远程提交
//        try {
//            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
//        } catch (AlreadyAliveException e) {
//            e.printStackTrace();
//        } catch (InvalidTopologyException e) {
//            e.printStackTrace();
//        } catch (AuthorizationException e) {
//            e.printStackTrace();
//        }
    }
}
