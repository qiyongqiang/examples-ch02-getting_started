package bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordNormalizer extends BaseBasicBolt {

    public void cleanup() {
        System.out.println("madouTest#WordNormalizer#cleanup ," + Thread.currentThread().getName());
    }

    /**
     * The bolt will receive the line from the
     * words file and process it to Normalize this line
     * <p>
     * The normalize will be put the words in lower case
     * and split the line to get all words in this
     */
    public void execute(Tuple input, BasicOutputCollector collector) {
        String sentence = input.getString(0);
        System.out.println("madouTest#WordNormalizer#sentence " + sentence + ", " + Thread.currentThread().getName());
        String[] words = sentence.split(" ");
        for (String word : words) {
            word = word.trim();
            if (!word.isEmpty()) {
                word = word.toLowerCase();
                collector.emit(new Values(word));
            }
        }
    }


    /**
     * The bolt will only emit the field "word"
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
