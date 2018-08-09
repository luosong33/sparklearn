package cn.jstorm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 2、WordNormalizer继承BaseBasicBolt来处理拉取的数据，将数据规范化。并发送给下一个Bolt。
 */
public class WordNormalizer extends BaseBasicBolt {

    private static final long serialVersionUID = 1L;
    private Logger log = LoggerFactory.getLogger(WordNormalizer.class);
    public void cleanup() {}

    /**
     * The bolt will receive the line from the
     * words file and process it to Normalize this line
     * 
     * The normalize will be put the words in lower case
     * and split the line to get all words in this 
     * 
     * 元组(tuple)是一个具名值列表，它可以是任意java对象（只要它是可序列化的）。
     * 默认情况，Storm会序列化字符串、字节数组、ArrayList、HashMap和HashSet等类型。
     * 
     * 这是bolt中最重要的方法，每当接收到一个tuple时，此方法便被调用 
     * 这个方法的作用就是把文本文件中的每一行切分成一个个单词，并把这些单词发射出去（给下一个bolt处理） 
     */
    public void execute(Tuple input, BasicOutputCollector collector) {
        String sentence = input.getString(0);
        /*String[] words = sentence.split(" ");
        for(String word : words){
            word = word.trim();
            if(!word.isEmpty()){
                word = word.toLowerCase();
                log.info(">>>>>>>>>>>处理结果："+word+"<<<<<<<<<<<<<<<<<<");
                collector.emit(new Values(word));
            }
        }*/
        collector.emit(new Values(sentence));
    }


    /**
     * The bolt will only emit the field "word" 
     * 这个*bolt*只会发布“word”域
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}