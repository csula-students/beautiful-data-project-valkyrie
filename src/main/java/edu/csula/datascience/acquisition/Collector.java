package edu.csula.datascience.acquisition;

/**
 * Interface to define collector behavior
 *
 * It should be able to download data from source and save data.
 */
public interface Collector<T, R> {
    Boolean mungee(String src);

    void save(String username,String profilelocation,long tweetId,String post);
}
