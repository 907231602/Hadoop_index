package com.bf.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordsIndexReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	//	super.reduce(arg0, arg1, arg2);
		String str="";
		for (Text text : arg1) {
			str+=text.toString()+"=>";
		}
		arg2.write(arg0, new Text(str));
	}
}
