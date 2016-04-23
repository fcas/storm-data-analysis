package ufrn.imd.engsoft.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import ufrn.imd.engsoft.helpers.TemporaryFilesMaker;
import ufrn.imd.engsoft.model.TweetStream;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

/**
 * Created by Felipe on 10/15/15.
 */

public class SymbolsCleanerBolt extends BaseRichBolt
{
    private List<String> _symbols;
    private OutputCollector _outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector)
    {
        String symbolsFilename = "symbols.txt";
        File symbolsFile = TemporaryFilesMaker.getFile(symbolsFilename, ".txt");
        try
        {
            _symbols = Files.readAllLines(symbolsFile.toPath());
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        _outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple)
    {
        TweetStream tweetStream = (TweetStream) tuple.getValue(0);
        String tweet = tweetStream.getPlainTweetText();

        for (String symbol : _symbols)
        {
            tweet = tweet.replace(symbol, "");
        }

        tweetStream.setSymbolLessTweetText(tweet);
        _outputCollector.emit(new Values(tweetStream));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("tweetSymbolLess"));
    }
}