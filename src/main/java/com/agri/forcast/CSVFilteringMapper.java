package com.agri.forcast;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CSVFilteringMapper extends Mapper<Text, Text, Text, Text> {
    private static final Logger logger = LoggerFactory.getLogger(CSVFilteringMapper.class);
   // private ObjectMapper jsonMapper = new ObjectMapper();
   // private String targetAppId = "carbon";

		private Text year = new Text();
    private FloatWritable val = new FloatWritable();
    Logger logger = Logger.getLogger(CSVFilteringMapper.class);

	 String[] years = {"1952","1953","1954","1955","1956","1957","1958","1959",
            "1960","1961","1962","1963","1964","1965","1966","1967",
            "1968","1969","1970","1971","1972","1973","1974","1975",
            "1977","1978","1979","1980","1991","1992","1993","1994","1995","1996",
            "1997","1998","1999","2000","2001","2002","2003","2004","2005","2006",
            "2007"};
			
    //public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String tupleAppId = getAppId(value);
        String stored = getPublishedDate(value);
		
		 public void map(LongWritable key, Text year, Context con)throws IOException, InterruptedException {
		 
		  String[] years = year.toString().split("\\t");
		String yearKey = year[3];
		Float AvarageProduction = Float.parseFloat(year[8]);
		  for (int i = 0;i< years.length;i++) {
				
			context.write(new Text(yearKey), new FloatWritable(AvarageProduction));
           //context.write(key, value);
            logger.debug("Mapped: " + key.toString());
			}
        }

           }

}
