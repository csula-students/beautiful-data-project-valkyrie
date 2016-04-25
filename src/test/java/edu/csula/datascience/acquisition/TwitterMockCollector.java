package edu.csula.datascience.acquisition;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * A mock implementation of collector for testing
 */
public class TwitterMockCollector implements  Collector<TwitterSimpleModel, TwitterMockData> {
    @Override
    public Boolean mungee(String src) 
    {
    	if(src.equals(null))
    	{
    		return true;
    	}
    	else        return false;
        
    }

	@Override
	public void save(String username, String profilelocation, long tweetId, String post) {
		// TODO Auto-generated method stub
		
	}

   
}
