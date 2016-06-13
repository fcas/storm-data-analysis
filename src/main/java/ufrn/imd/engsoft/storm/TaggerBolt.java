package ufrn.imd.engsoft.storm;

import opennlp.tools.cmdline.postag.POSModelLoader;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSSample;
import opennlp.tools.postag.POSTaggerME;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import ufrn.imd.engsoft.helpers.TemporaryFilesMaker;
import ufrn.imd.engsoft.model.TweetStream;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Felipe on 10/15/15.
 */

public class TaggerBolt extends BaseRichBolt
{
    private OutputCollector _outputCollector;
    private POSTaggerME _tagger;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector)
    {
        File file = TemporaryFilesMaker.getFile("pt-pos-maxent.bin", ".bin");
        POSModel _model = new POSModelLoader().load(file);
        _tagger = new POSTaggerME(_model);
        _outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple)
    {
        TweetStream tweetStream = (TweetStream) tuple.getValue(0);
        List<String> tokensList = tweetStream.getTokenList();
        String[] tokensArray = new String[tokensList.size()];
        tokensArray = tokensList.toArray(tokensArray);
        String[] tagsArray = _tagger.tag(tokensArray);
        List<String> tagsList = new ArrayList<>(Arrays.asList(tagsArray));
        POSSample sample = new POSSample(tokensList, tagsList);
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
        for (int i = 0; i < sample.getSentence().length; i++)
        {
            map.put(sample.getSentence()[i], sample.getTags()[i]);
        }
        tweetStream.setTagsMap(map);
        _outputCollector.emit(new Values(tweetStream));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {
        outputFieldsDeclarer.declare(new Fields("tags"));
    }
}