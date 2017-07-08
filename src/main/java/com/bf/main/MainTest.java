package com.bf.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bf.mapper.WordIndexMapper;
import com.bf.mapper.WordsMapper;
import com.bf.reducer.WordReducer;
import com.bf.reducer.WordsIndexReducer;

public class MainTest implements Tool {
	private Configuration conf;
	

	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		this.conf=conf;
	}

	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(MainTest.class);
		
		job.setMapperClass(WordsMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setReducerClass(WordReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		//文件夹下放多个文件文本，作为输入，输出结果作为下次输入
		FileOutputFormat.setOutputPath(job, new Path("hdfs://yanjijun1:9000/wwwout"));
		FileInputFormat.setInputPaths(job, new Path("hdfs://yanjijun1:9000/www"));
		//===================================================================
		Job job1=Job.getInstance(conf);
		
		job1.setJarByClass(MainTest.class);
		
		job1.setMapperClass(WordIndexMapper.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setReducerClass(WordsIndexReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job1, new Path("hdfs://yanjijun1:9000/wwwoutOut"));
		FileInputFormat.setInputPaths(job1, new Path("hdfs://yanjijun1:9000/wwwout"));
		//=================================================================
		
		
		
		
		ControlledJob cjob1=new ControlledJob(conf);
		cjob1.setJob(job);
		
		ControlledJob cjob2=new ControlledJob(conf);
		cjob2.setJob(job1);
		cjob2.addDependingJob(cjob1);
		
		JobControl jc=new JobControl("wordsIndex1");
		jc.addJob(cjob1);
		jc.addJob(cjob2);
		
		
		//========================================================================
	
		
	
		//=============================================
		
		
		
		
		Thread t1=new Thread(jc);
		t1.start();
		
		while(true){
			if(jc.allFinished()){
				System.out.println(jc.getSuccessfulJobList());
				jc.stop();
				return 1;
			}
			if(jc.getFailedJobList().size()>0){
				System.out.println(jc.getFailedJobList());
				jc.stop();
				return 0;
			}
		}
		
		//return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int t= ToolRunner.run(new MainTest(), args);
		System.out.println(t);
	}

}
