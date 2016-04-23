package ufrn.imd.engsoft.storm;

/**
 * Created by Felipe on 10/8/15.
 */

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.Status;
import twitter4j.UserMentionEntity;
import ufrn.imd.engsoft.model.TweetStream;

public class TweetCleanerBolt extends BaseBasicBolt
{
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector)
    {
        String plainTweetText;
        Status tweet = ((Status)tuple.getValue(0));
        String tweetText = tweet.getText().toLowerCase();
        plainTweetText = replaceMentions(tweet.getUserMentionEntities(), tweetText);
        plainTweetText = replaceUrls(plainTweetText);

        if (tweet.isRetweet())
        {
            plainTweetText = plainTweetText.split(":")[1];
        }

        TweetStream tweetStream = new TweetStream();
        tweetStream.setTweetText(tweetText);
        tweetStream.setId(tweet.getId());
        tweetStream.setPlainTweetText(plainTweetText.trim());
        tweetStream.setUsersMention(tweet.getUserMentionEntities());

        basicOutputCollector.emit(new Values(tweetStream));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("tweetCleaned"));
    }

    private String replaceUrls(String tweet)
    {
        return tweet.replaceAll("https?://\\S+\\s?", "");
    }

    private String replaceMentions(UserMentionEntity[] userMentionEntities, String tweet)
    {
        for(UserMentionEntity userMentionEntity : userMentionEntities)
        {
            String userMention = userMentionEntity.getScreenName();
            tweet = tweet.replace("@" + userMention, "");
        }
        return tweet;
    }
}
