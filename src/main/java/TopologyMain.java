import spouts.WordReader;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.WordCounter;
import bolts.WordNormalizer;


public class TopologyMain {
    public static void main(String[] args) throws InterruptedException {

        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("word-normalizer", new WordNormalizer())
                .shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter(), 1)
                .fieldsGrouping("word-normalizer", new Fields("word"));

        //Configuration
        Config conf = new Config();
//        conf.put("wordsFile", args[0]);
        conf.put("wordsFile","/Users/zhujian/project/stormDemo/examples-ch02-getting_started/src/main/resources/words.txt");
        conf.setDebug(true);
        //Topology run
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        //topology提交
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
        Thread.sleep(20000);
        cluster.shutdown();
    }
}
