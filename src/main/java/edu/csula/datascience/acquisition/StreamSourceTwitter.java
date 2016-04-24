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

public class StreamSourceTwitter 
{
	TwitterCollector tc=new TwitterCollector();
	
	public void getStream()
	{
		ConfigurationBuilder cb = new ConfigurationBuilder();
	    cb.setDebugEnabled(true);
	    cb.setOAuthConsumerKey("ConsumerKey");
	    cb.setOAuthConsumerSecret("ConsumerSecret");
	    cb.setOAuthAccessToken("AccessToken");
	    cb.setOAuthAccessTokenSecret("AccessTokenSecret");

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
	}
	
}
