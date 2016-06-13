package ufrn.imd.engsoft.dao;

import org.bson.Document;

/**
 * Created by Felipe on 22/05/16.
 */
public interface ITweetsDAO
{
    void saveTweetStreams(Document tweetStream);

    void dropCollection();

    void closeMongo();
}
