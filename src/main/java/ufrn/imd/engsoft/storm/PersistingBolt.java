package ufrn.imd.engsoft.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.bson.Document;
import twitter4j.UserMentionEntity;
import ufrn.imd.engsoft.dao.ITweetsDAO;
import ufrn.imd.engsoft.dao.TweetsDAO;
import ufrn.imd.engsoft.model.TweetStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Felipe on 4/18/16.
 */

public class PersistingBolt extends BaseRichBolt {

    private ITweetsDAO _tweetsDAO;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector)
    {
        String collectionName = "storm_twitter_stream";
        _tweetsDAO = TweetsDAO.getInstance(collectionName, false);
    }

    @Override
    public void execute(Tuple tuple)
    {
        TweetStream tweetStream = (TweetStream) tuple.getValue(0);
        Document document = new Document();
        document.put("_id", tweetStream.getId());
        document.put("_sentiment", tweetStream.getSentiment());

        List<String> _usersMention = new ArrayList<>();

        for (UserMentionEntity userMention : tweetStream.getUsersMention())
        {
            _usersMention.add(userMention.getScreenName());
        }

        document.put("_tweetText", tweetStream.getTweetText());
        document.put("_tags", tweetStream.getTagsMap());

        document.put("_userMentions", _usersMention);
        _tweetsDAO.saveTweetStreams(document);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {
    }

    @Override
    public void cleanup()
    {
        _tweetsDAO.closeMongo();
    }
}