package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TokenizerMapperTest {
    @Mock
    private Mapper<org.apache.hadoop.io.LongWritable, org.apache.hadoop.io.Text, org.apache.hadoop.io.Text, org.apache.hadoop.io.IntWritable>.Context context;
    private TokenizerMapper tokenizerMapper;

    @Before
    public void setup() {
        this.tokenizerMapper = new TokenizerMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String value = "Paris;7;beau;maclura";
        this.tokenizerMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
                .write(new Text("maclura"), new IntWritable(1));
    }
}
