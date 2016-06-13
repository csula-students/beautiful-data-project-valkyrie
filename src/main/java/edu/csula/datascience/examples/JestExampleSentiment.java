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
public class JestExampleSentiment {
    public static void main(String[] args) throws URISyntaxException, IOException {
        String indexName = "twitter-data-sentiment";
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
            ClassLoader.getSystemResource("Tweetsentiment.csv")
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
            Collection<TwitterDatasentiment> TwitterDatasentiment = Lists.newArrayList();

            int count = 0;

            // for each record, we will insert data into Elastic Search
//            parser.forEach(record -> {
            for (CSVRecord record: parser) {
                // cleaning up dirty data which doesn't have time or temperature
                if (
                    !record.get("_id").isEmpty() &&
                    !record.get("polarity").isEmpty() &&
                    !record.get("sentiment").isEmpty() &&
                    !record.get("author").isEmpty() &&
                    !record.get("subjectivity").isEmpty() &&
                    !record.get("date").isEmpty() &&
                    !record.get("message").isEmpty()
                ) {
                	TwitterDatasentiment temp = new TwitterDatasentiment(
                			record.get("_id"),
                			record.get("polarity"),
                			record.get("sentiment"),
                			record.get("author"),
                			record.get("subjectivity"),
                			//record.get("date"),
                			record.get("message")
                			
                    );

                    if (count < 500) {
                    	TwitterDatasentiment.add(temp);
                        count ++;
                    } else {
                        try {
                            Collection<BulkableAction> actions = Lists.newArrayList();
                            TwitterDatasentiment.stream()
                                .forEach(tmp -> {
                                    actions.add(new Index.Builder(tmp).build());
                                });
                            Bulk.Builder bulk = new Bulk.Builder()
                                .defaultIndex(indexName)
                                .defaultType(typeName)
                                .addAction(actions);
                            client.execute(bulk.build());
                            count = 0;
                            TwitterDatasentiment = Lists.newArrayList();
                            System.out.println("Inserted 500 documents to cloud");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }

            Collection<BulkableAction> actions = Lists.newArrayList();
            TwitterDatasentiment.stream()
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

    
    static class TwitterDatasentiment {
        final String id;
        final String polarity;
        final String sentiment;
        final String author;
        final String subjectivity;
        final String date;
        final String message;

        public TwitterDatasentiment(String id, String polarity, String sentiment, String author, String subjectivity,String message) {
        	
			super();
			Date myDate = new Date();
			this.id = id;
			this.polarity = polarity;
			this.sentiment = sentiment;
			this.author = author;
			this.subjectivity = subjectivity;
			this.date = new SimpleDateFormat("yyyy-MM-dd").format(myDate);
			this.message = message;
		}

		

		
    }
}
