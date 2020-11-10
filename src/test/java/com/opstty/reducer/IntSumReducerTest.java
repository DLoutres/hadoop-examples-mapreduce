package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class IntSumReducerTest {
    @Mock
    private Reducer.Context context;
    private IntSumReducer intSumReducer;

    @Before
    public void setup() {
        this.intSumReducer = new IntSumReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String key = "18;12;6";
        String[] expectedArray = {"18"};
        Text value = new Text("18;18;6");
        Iterable<Text> values = Arrays.asList(value, value,value);
        this.intSumReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(key), new Text("{18=18.0}"));
    }
}
