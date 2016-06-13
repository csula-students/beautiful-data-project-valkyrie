
package edu.csula.datascience.examples;

import com.google.common.collect.Lists;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

/**
 * A quick example app to send data to elastic search on AWS
 */
public class JestExampleApp {
    public static void main(String[] args) throws URISyntaxException, IOException {
        String indexName = "twitter-data";
        String typeName = "twitter-tags";
        String awsAddress = "https://search-valkyrie-curthmhxcjun4y43tn2foxfinu.us-west-2.es.amazonaws.com/";
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
            .Builder(awsAddress)
            .multiThreaded(true)
            .build());
        JestClient client = factory.getObject();

        // as usual process to connect to data source, we will need to set up
        // node and client// to read CSV file from the resource folder
        File csv = new File(
            ClassLoader.getSystemResource("TwitterData.csv")
                .toURI()
        );

        try {
            // after reading the csv file, we will use CSVParser to parse through
            // the csv files
            CSVParser parser = CSVParser.parse(
                csv,
                Charset.defaultCharset(),
                CSVFormat.EXCEL.withHeader()
            );
            Collection<TwitterData> TwitterData = Lists.newArrayList();

            int count = 0;

            // for each record, we will insert data into Elastic Search
//            parser.forEach(record -> {
            for (CSVRecord record: parser) {
                // cleaning up dirty data which doesn't have time or temperature
                if (
                    !record.get("_id").isEmpty() &&
                    !record.get("TweetId").isEmpty() &&
                    !record.get("Username").isEmpty() &&
                    !record.get("ProfileLocation").isEmpty() &&
                    !record.get("Content").isEmpty()
                ) {
                	TwitterData temp = new TwitterData(
                        record.get("Username"),
                        record.get("ProfileLocation"),
                        Long.valueOf(record.get("TweetId")),
                        record.get("Content")
                    );

                    if (count < 500) {
                    	TwitterData.add(temp);
                        count ++;
                    } else {
                        try {
                            Collection<BulkableAction> actions = Lists.newArrayList();
                            TwitterData.stream()
                                .forEach(tmp -> {
                                    actions.add(new Index.Builder(tmp).build());
                                });
                            Bulk.Builder bulk = new Bulk.Builder()
                                .defaultIndex(indexName)
                                .defaultType(typeName)
                                .addAction(actions);
                            client.execute(bulk.build());
                            count = 0;
                            TwitterData = Lists.newArrayList();
                            System.out.println("Inserted 500 documents to cloud");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }

            Collection<BulkableAction> actions = Lists.newArrayList();
            TwitterData.stream()
                .forEach(tmp -> {
                    actions.add(new Index.Builder(tmp).build());
                });
            Bulk.Builder bulk = new Bulk.Builder()
                .defaultIndex(indexName)
                .defaultType(typeName)
                .addAction(actions);
            client.execute(bulk.build());
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("We are done! Yay!");
    }

    static class Temperature {
        final String date;
        final double averageTemperature;
        final String state;
        final String country;

        public Temperature(String date, double averageTemperature, String state, String country) {
            this.date = date;
            this.averageTemperature = averageTemperature;
            this.state = state;
            this.country = country;
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
            
//            System.out.println("-------------");
//            System.out.println(date);
//            
        }
    }
}