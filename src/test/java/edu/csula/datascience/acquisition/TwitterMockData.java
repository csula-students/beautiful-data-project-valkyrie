package edu.csula.datascience.acquisition;

/**
 * Mock raw data
 */
public class TwitterMockData {
    private final String id;
    private final String post;

    public TwitterMockData(String id, String post) {
        this.id = id;
        this.post = post;
    }

    public String getId() {
        return id;
    }

    public String getPost() {
		return post;
	}

	
}
