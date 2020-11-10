package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

public class IntSumReducer extends Reducer<Text, Text, Text, Text> {
        TreeMap<String,Float> tm = new TreeMap<String,Float>();
        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
            int max=0;
            for (Text l : values) {
                String arr[] = l.toString().split(";");
                tm.put(arr[0],Float.parseFloat(arr[1]));
                if(tm.size() > 1){
                    tm.remove(tm.firstKey());
                }
            }
            context.write(key, new Text(tm.toString()));
        }
}
