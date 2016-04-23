package ufrn.imd.engsoft.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import ufrn.imd.engsoft.helpers.TwitterKeysReader;

import java.util.Properties;

/**
 * Created by Felipe on 10/8/15.
 */

public class StormTopology
{
    private static Config _config;

    public static void main(String[] args)
    {
        TopologyBuilder builder = new TopologyBuilder();

        setConfig();

        Properties properties = TwitterKeysReader.getTwitterKeys();

        if (properties != null)
        {
            builder.setSpout("twitter", new TwitterStreamSpout
                    (
                            properties.getProperty("consumerKey"),
                            properties.getProperty("consumerSecret"),
                            properties.getProperty("accessToken"),
                            properties.getProperty("accessTokenSecret")), 8
            );
        }

        builder.setBolt("tweetCleaner", new TweetCleanerBolt()).shuffleGrouping("twitter");
        builder.setBolt("symbolsCleaner", new SymbolsCleanerBolt()).shuffleGrouping("tweetCleaner");
        builder.setBolt("tokenizer", new TokenizerBolt()).shuffleGrouping("symbolsCleaner");
        builder.setBolt("tags", new TaggerBolt()).shuffleGrouping("tokenizer");
        builder.setBolt("sentimentAnalyser", new SentimentAnalyserBolt()).shuffleGrouping("tags");
        builder.setBolt("persisting", new PersistingBolt()).shuffleGrouping("sentimentAnalyser");

//        //Submit the topology on local cluster
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("twitterTopology", _config, builder.createTopology());
//
//        try
//        {
//            Thread.sleep(60000);
//        }
//        catch (InterruptedException e)
//        {
//            e.printStackTrace();
//        }
//
//        cluster.killTopology("twitterTopology");
//        cluster.shutdown();
//        System.exit(0);

        try
        {
            StormSubmitter.submitTopology(args[0], _config, builder.createTopology());
        }
        catch (AuthorizationException e)
        {
            e.printStackTrace();
        }
        catch (AlreadyAliveException alreadyAliveException)
        {
            System.out.println(alreadyAliveException);
        }
        catch (InvalidTopologyException invalidTopologyException)
        {
            System.out.println(invalidTopologyException);
        }
    }

    private static void setConfig()
    {
        _config = new Config();
        _config.setDebug(true);
        _config.setNumWorkers(4);
    }
}