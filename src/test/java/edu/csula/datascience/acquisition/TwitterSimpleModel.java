package edu.csula.datascience.acquisition;

/**
 * A simple model for testing
 */
public class TwitterSimpleModel {
    private final String id;
    private final String post;

    public TwitterSimpleModel(String id, String post) {
        this.id = id;
        this.post = post;
    }

    public String getId() {
        return id;
    }

    

    public String getPost() {
		return post;
	}

	public static TwitterSimpleModel build(TwitterMockData data) {
        return new TwitterSimpleModel(data.getId(), data.getPost());
    }
}
