package ufrn.imd.engsoft.dao;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import org.bson.Document;
import org.jongo.Jongo;
import org.jongo.marshall.MarshallingException;

/**
 * Created by Felipe on 10/16/15.
 */
public class TweetsDAO implements ITweetsDAO
{
    private static ITweetsDAO _instance;
    private static boolean _dropCollection;
    private static String _collectionName;
    private DB _database;
    private Jongo _jongo;
    private MongoClient _mongoClient;

    private TweetsDAO(String collectionName)
    {
        _mongoClient = new MongoClient();
        _database = _mongoClient.getDB("tweets_db");
        _jongo = new Jongo(_database);
        _collectionName = collectionName;

        if(_dropCollection)
        {
            dropCollection();
        }
    }

    private static synchronized void createInstance (String collectionName)
    {
        if (_instance == null)
        {
            _instance = new TweetsDAO(collectionName);
        }
    }

    public static ITweetsDAO getInstance(String collectionName, boolean dropCollection)
    {
        if(_instance == null)
        {
            _dropCollection = dropCollection;
            createInstance (collectionName);
        }
        return _instance;
    }

    public void saveTweetStreams(Document tweetStream)
    {
        try
        {
            _jongo.getCollection(_collectionName).insert(tweetStream);
        }
        catch (MarshallingException e)
        {

        }
    }

    public void dropCollection()
    {
        _jongo.getCollection(_collectionName).drop();
    }

    public void closeMongo()
    {
        _mongoClient.close();
    }
}
