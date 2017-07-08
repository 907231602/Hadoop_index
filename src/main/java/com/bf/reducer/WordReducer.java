package com.bf.reducer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordReducer extends Reducer< Text, LongWritable,  Text, LongWritable> {
	@Override
	protected void reduce(Text arg0, Iterable<LongWritable> arg1,
			Reducer<Text, LongWritable, Text, LongWritable>.Context arg2) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.reduce(arg0, arg1, arg2);
		int count=0;
		for (LongWritable longWritable : arg1) {
			count++;
		}
		arg2.write(arg0, new LongWritable(count));
		
		
	}
}
