package com.agri.forcast;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextFileReducer extends Reducer<Text, Text, Text, Text> {

	private static final Logger logger = LoggerFactory
			.getLogger(TextFileReducer.class);

	public void reducer(Text key, Text value, Context context)
			throws IOException, InterruptedException {

		context.write(key, value);

		logger.debug("Reduced " + key.toString());
	}

}
