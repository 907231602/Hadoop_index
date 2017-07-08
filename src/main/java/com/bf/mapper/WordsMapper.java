package com.bf.mapper;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WordsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	//	super.map(key, value, context);
		String[] words=value.toString().split("\t");
		FileSplit file= (FileSplit) context.getInputSplit();
		String fileName=file.getPath().getName();
		for (String word : words) {
			context.write(new Text(word+"\t"+fileName), new LongWritable(1));
		}
	}

}
