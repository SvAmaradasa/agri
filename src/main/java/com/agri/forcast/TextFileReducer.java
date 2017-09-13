package com.agri.forcast;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YearByPraductionReducer extends Reducer<Text, FloatWritable, Text, Text> {

	private static final Logger logger = LoggerFactory
			.getLogger(YearByPraductionReducer.class);

	public void reducer(Text key, Iterable<FloatWritable> valueList, Context context)
			throws IOException, InterruptedException {
			float YearlyProductionSum = 0;
			int count = 0;
 for (FloatWritable var : valueList) {
    YearlyProductionSum += var.get();
    //System.out.println("reducer " + var.get());
    count++;
		context.write(key, value);

		logger.debug("Reduced " + key.toString());
	}

}
}