package edu.csula.datascience.acquisition;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;

import com.google.gson.Gson;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;


public class StreamSourceTwitter{
	
	private final static String indexName = "twitter-data";
    private final static String typeName = "twitter-tags";
    
    Node node = nodeBuilder().settings(Settings.builder()
            .put("cluster.name", "rahul-rapatwar")
            .put("path.home", "elasticsearch-data")).node();
    Client client = node.client();
	
    
 // create bulk processor
    BulkProcessor bulkProcessor = BulkProcessor.builder(
        client,
        new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId,
                                   BulkRequest request) {
            }

            @Override
            public void afterBulk(long executionId,
                                  BulkRequest request,
                                  BulkResponse response) {
            }

            @Override
            public void afterBulk(long executionId,
                                  BulkRequest request,
                                  Throwable failure) {
                System.out.println("Facing error while importing data to elastic search");
                failure.printStackTrace();
            }
        })
        .setBulkActions(10000)
        .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
        .setFlushInterval(TimeValue.timeValueSeconds(5))
        .setConcurrentRequests(1)
        .setBackoffPolicy(
            BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
        .build();

    // Gson library for sending json to elastic search
    Gson gson = new Gson();
    
	
	TwitterCollector tc=new TwitterCollector();
	
	public void getStream()
	{
		ConfigurationBuilder cb = new ConfigurationBuilder();
	    cb.setDebugEnabled(true);
	    cb.setOAuthConsumerKey("dm0LGPgB7j8pVz5XEW8szqOoP");
	    cb.setOAuthConsumerSecret("yyvRhKOLDJBhlT6WKOJ76IgLxeKStsDhnHJPfgj1yMtoRNX3Iq");
	    cb.setOAuthAccessToken("724318226318954497-VNfzNv2xMIQv13pPrLBTrjVPJiMD7Lq");
	    cb.setOAuthAccessTokenSecret("3orChIaPZFPZZkR08QDxlli6XdQxCrlvn9AbiXOH5Ki8P");

	    TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

	    StatusListener listener = new StatusListener() {

	        @Override
	        public void onException(Exception arg0) {
	            // TODO Auto-generated method stub

	        }

	        @Override
	        public void onDeletionNotice(StatusDeletionNotice arg0) {
	            // TODO Auto-generated method stub

	        }

	        @Override
	        public void onScrubGeo(long arg0, long arg1) {
	            // TODO Auto-generated method stub

	        }

	        @Override
	        public void onStatus(Status status) {
	            User user = status.getUser();
	            
	            // gets User Name
	            String username = status.getUser().getScreenName();
	            System.out.println(username);
	            // gets Location
	            String profileLocation = user.getLocation();
	            System.out.println(profileLocation);
	            // gets TweetID
	            long tweetId = status.getId(); 
	            System.out.println(tweetId);
	            // gets Posts
	            String content = status.getText();
	            System.out.println(content +"\n");
	            
	         // Calling MUNGEE
	            Boolean check=tc.mungee(profileLocation);
	         //check if it have valid location	            
	            if(!check)
	            {
	            	tc.save(username, profileLocation, tweetId, content);
	            	System.out.println("SAVING!!!!!!!!");
	            	{
	                    TwitterData temp = new TwitterData(
	                        username,
	                        profileLocation,
	                        tweetId,
	                        content
	                    );

	                    bulkProcessor.add(new IndexRequest(indexName, typeName)
	                        .source(gson.toJson(temp))
	                    );
	                }
	            }
	        }

	        @Override
	        public void onTrackLimitationNotice(int arg0) {
	            // TODO Auto-generated method stub

	        }

			@Override
			public void onStallWarning(StallWarning warning) {
				// TODO Auto-generated method stub
				
			}

	    };
	    FilterQuery fq = new FilterQuery();

	    String keywords[] = {"#python","#swift","#SQL","#mongodb","#android","#ios","#NoSQL","#BgData","#Ruby","#PSP","#Xbox360","#JS"};

	    fq.track(keywords);

	    twitterStream.addListener(listener);
	    twitterStream.filter(fq);  	
	    
	    
	    /**
         * AGGREGATION
         */
        SearchResponse sr = node.client().prepareSearch(indexName)
            .setTypes(typeName)
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                AggregationBuilders.terms("stateAgg").field("state")
                    .size(Integer.MAX_VALUE)
            )
            .execute().actionGet();

        // Get your facet results
        Terms agg1 = sr.getAggregations().get("stateAgg");

        for (Terms.Bucket bucket: agg1.getBuckets()) {
            System.out.println(bucket.getKey() + ": " + bucket.getDocCount());
        }
	    
	}
	
	
	
	static class TwitterData {
        final String username;
        final String profileLocation;
        final long tweetId;
        final String content;
        final String date;

        public TwitterData(String username, String profileLocation, long tweetId, String content) {
        	//Date date = new Date();
        	//SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        	
        	Date myDate = new Date();        	
        	
        	this.username = username;
            this.profileLocation = profileLocation;
            this.tweetId = tweetId;
            this.content = content;
            this.date = new SimpleDateFormat("yyyy-MM-dd").format(myDate);
            
            System.out.println("-------------");
            System.out.println(date);
            
        }
    }
	
}
