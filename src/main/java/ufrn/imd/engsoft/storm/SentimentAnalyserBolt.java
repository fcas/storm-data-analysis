package ufrn.imd.engsoft.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import ufrn.imd.engsoft.helpers.TemporaryFilesMaker;
import ufrn.imd.engsoft.model.Sentiments;
import ufrn.imd.engsoft.model.TweetStream;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Created by Felipe on 4/14/16.
 */
public class SentimentAnalyserBolt extends BaseRichBolt
{
    private ConcurrentHashMap<String, Integer> _sentimentMap;
    private OutputCollector _outputCollector;
    private List<String> _positiveEmoticons;
    private List<String> _negativeEmoticons;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector)
    {
        String sentiLexFilename = "sentilex.txt";
        String positiveEmoticonsFilename = "positive_emoticons.txt";
        String negativeEmoticonsFilename = "negative_emoticons.txt";
        File sentilexFile = TemporaryFilesMaker.getFile(sentiLexFilename, ".text");
        File positiveEmoticonsFile = TemporaryFilesMaker.getFile(positiveEmoticonsFilename, ".text");
        File negativeEmoticonsFile = TemporaryFilesMaker.getFile(negativeEmoticonsFilename, ".text");

        List<String> sentiLexFileLines;
        _sentimentMap = new ConcurrentHashMap<>();
        try
        {
            sentiLexFileLines = Files.readAllLines(sentilexFile.toPath());
            linesToMap(sentiLexFileLines);
            _positiveEmoticons = Files.readAllLines(positiveEmoticonsFile.toPath());
            _negativeEmoticons = Files.readAllLines(negativeEmoticonsFile.toPath());
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
        ConcurrentHashMap hashMap = tweetStream.getTagsMap();
        Iterator words = hashMap.keySet().iterator();
        boolean hasNegativeAdverbs = hashMap.keySet().contains("não");
        int negativeSentimentsCounter = 0;
        int positiveSentimentsCounter = 0;
        while (words.hasNext())
        {
            String word = words.next().toString();
            String tag = hashMap.get(word).toString();
            if (tag.equals("adj"))
            {
                int sentiment = 0;
                if (_sentimentMap.containsKey(word))
                {
                    sentiment = _sentimentMap.get(word);
                }
                if (sentiment == 1)
                {
                    if (hasNegativeAdverbs)
                    {
                        negativeSentimentsCounter++;
                    }
                    else
                    {
                        positiveSentimentsCounter++;
                    }
                }
                else if (sentiment == -1)
                {
                    if (hasNegativeAdverbs)
                    {
                        positiveSentimentsCounter++;
                    }
                    else
                    {
                        negativeSentimentsCounter++;
                    }
                }
            }
        }

        String tweetText = tweetStream.getTweetText();
        try
        {
            positiveSentimentsCounter += emoticonCounter(Sentiments.positive, tweetText);
            negativeSentimentsCounter += emoticonCounter(Sentiments.negative, tweetText);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        double sentimentsScore = countSentimentsScore(positiveSentimentsCounter, negativeSentimentsCounter);

        if (sentimentsScore > 0 && ((1 - sentimentsScore) > 0.5) || sentimentsScore == 1)
        {
            tweetStream.setSentiment(Sentiments.positive.name());
        }
        else if (sentimentsScore < 0)
        {
            tweetStream.setSentiment(Sentiments.negative.name());
        }
        else
        {
            tweetStream.setSentiment(Sentiments.neutral.name());
        }

        _outputCollector.emit(new Values(tweetStream));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {
        outputFieldsDeclarer.declare(new Fields("sentiments"));
    }

    private void linesToMap(List<String> lines)
    {
        for(String line : lines)
        {
            String[] strings = line.split(",");
            String adjective = strings[0];
            int sentiment = Integer.parseInt(strings[1].split(";")[3].split("=")[1]);
            _sentimentMap.put(adjective, sentiment);
        }
    }

    private double countSentimentsScore(int positiveSentimentsCounter, int negativeSentimentsCounter)
    {
        int sum = positiveSentimentsCounter + negativeSentimentsCounter;
        int dif = positiveSentimentsCounter - negativeSentimentsCounter;
        if (sum > 0)
        {
            return dif / sum;
        }
        else
        {
            return 0;
        }
    }

    private int emoticonCounter(Sentiments sentiment, String text) throws IOException
    {
        Pattern pattern = Pattern.compile("([0-9A-Za-z'&\\-\\./\\(\\)\\]\\[<>\\}\\*\\^\"\\D=:;]+)|((?::|;|=)(?:-)?(?:\\)|D|P))");
        Matcher matcher = pattern.matcher(text);
        int emoticonCounter = 0;
        while(matcher.find())
        {
            String emoticon = matcher.group();
            Stream<String> filteredLines = null;
            if (sentiment == Sentiments.positive)
            {
                filteredLines = _positiveEmoticons.stream().filter(s -> s.contains(emoticon));
            }
            else if (sentiment == Sentiments.negative)
            {
                filteredLines = _negativeEmoticons.stream().filter(s -> s.contains(emoticon));
            }
            if (filteredLines != null)
            {
                Optional<String> hasString = filteredLines.findFirst();
                if(hasString.isPresent())
                {
                    emoticonCounter++;
                }
            }
        }

        return emoticonCounter;
    }
}