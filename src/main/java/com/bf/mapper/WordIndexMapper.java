package com.bf.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	//	super.map(key, value, context);
		String[] words=value.toString().split("\t");
		context.write(new Text(words[0]), new Text(words[1]+"\t"+words[2]));
		
		
	}

}
