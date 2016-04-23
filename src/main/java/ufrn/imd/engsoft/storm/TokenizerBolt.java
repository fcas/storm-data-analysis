package ufrn.imd.engsoft.storm;

import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import ufrn.imd.engsoft.helpers.TemporaryFilesMaker;
import ufrn.imd.engsoft.model.TweetStream;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by Felipe on 10/24/15.
 */

public class TokenizerBolt extends BaseRichBolt
{
    private InputStream _inputStream = null;
    private OutputCollector _outputCollector;
    private Tokenizer _tokenizer;
    private List<String> _tokenList;
    private List<String> _stopwords;
    private HashMap<String, String> _abbreviationsMap;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector)
    {
        String abbreviationsFileName = "abbreviations.txt";
        String stopwordsFilename = "stopwords.txt";
        String _tokenModelFileName = "pt-token.bin";

        List<String> abbreviationsFileLines;
        _inputStream = this.getClass().getClassLoader().getResourceAsStream(_tokenModelFileName);

        if (_inputStream != null)
        {
            try
            {
                TokenizerModel _model = new TokenizerModel(_inputStream);
                _tokenizer = new TokenizerME(_model);
                abbreviationsFileLines = Files.readAllLines(TemporaryFilesMaker.getFile(abbreviationsFileName, ".txt").toPath());
                _abbreviationsMap = linesToMap(abbreviationsFileLines);
                _stopwords = Files.readAllLines(TemporaryFilesMaker.getFile(stopwordsFilename, ".txt").toPath());
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }

        _outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple)
    {
        TweetStream tweetStream = (TweetStream) tuple.getValue(0);
        String tweet = tweetStream.getSymbolLessTweetText();
        String tokens[] = _tokenizer.tokenize(tweet);
        _tokenList = new ArrayList<>(Arrays.asList(tokens));

        replaceAbbreviations();
        removeStopWords();

        tweetStream.setTokenList(_tokenList);
        _outputCollector.emit(new Values(tweetStream));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("tokens"));
    }

    @Override
    public void cleanup()
    {
        try
        {
            _inputStream.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private void replaceAbbreviations()
    {
        for (int i = 0; i < _tokenList.size(); i++)
        {
            String token = _tokenList.get(i);
            if (_abbreviationsMap.containsKey(token))
            {
               _tokenList.set(i, _abbreviationsMap.get(token));
            }
        }
    }

    private void removeStopWords()
    {
        _stopwords.stream()
                .filter(string -> _tokenList.contains(string))
                .forEach(string -> _tokenList.remove(string));
    }

    private HashMap<String, String> linesToMap(List<String> lines)
    {
        HashMap<String, String> map = new HashMap<>();
        for(String line : lines)
        {
            map.put(line.split(",")[0], line.split(",")[1]);
        }
        return map;
    }
}