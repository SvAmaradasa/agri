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
    private ObjectMapper jsonMapper = new ObjectMapper();
    private String targetAppId = "carbon";

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String tupleAppId = getAppId(value);
        String stored = getPublishedDate(value);
        if (mapFunction()) {
            // Write event to context
            context.write(key, value);
            logger.debug("Mapped: " + key.toString());
        }

        // else - don't map
    }

    public boolean mapFunction() {
        return true;
    }
 
    private String getAppId(Text value) throws JsonProcessingException, IOException {
        String appId = jsonMapper.readTree(value.toString()).path("generator").path("appId").asText();

        return appId;
    }
 
    private String getPublishedDate(Text value) throws JsonProcessingException, IOException {
        String publishedDate = "";

        // Try to read the serverDate value
        publishedDate = jsonMapper.readTree(value.toString()).path("serverDate").asText();
        if (StringUtils.isBlank(publishedDate)) {
            // some old clients might not have serverDate populated.
            // They might have the value for "published" set.
            publishedDate = jsonMapper.readTree(value.toString()).path("published").asText();
        }

        return publishedDate;
    }

}
