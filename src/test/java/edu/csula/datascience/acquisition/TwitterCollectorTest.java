package edu.csula.datascience.acquisition;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;


// * A test case to show how to use Collector and Source
public class TwitterCollectorTest {
    private Collector<TwitterSimpleModel, TwitterMockData> collector;
    private Source<TwitterMockData> source;

//    @Before
    public void setup() {
        collector = new TwitterMockCollector();
        source = new TwitterMockSource();
    }

    @Test
    public void mungee() throws Exception {
      //  List<SimpleModel> list = (List<SimpleModel>) collector.mungee(source.next());
        List<TwitterSimpleModel> expectedList = Lists.newArrayList(
            new TwitterSimpleModel("2", "content2"),
            new TwitterSimpleModel("3", "content3")
        );

    //    Assert.assertEquals(list.size(), 2);

        for (int i = 0; i < 2; i ++) {
   //         Assert.assertEquals(list.get(i).getId(), expectedList.get(i).getId());
   //         Assert.assertEquals(list.get(i).getContent(), expectedList.get(i).getContent());
        }
    }
}