package agri.forcast;

 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.Tag;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

public class ClusterTrigger extends Configured implements Tool {

    private static final Logger logger = LoggerFactory.getLogger(ClusterTrigger.class);

    public static void main(String[] args) throws Exception {
        try {
            if (args[0].equals("deploy")) {
                deploy(args);
                System.exit(0);
            } else if (args[0].equals("run")) {
                int res = ToolRunner.run(new Configuration(), new ClusterTrigger(), args);
                System.exit(res);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

    }

    private static void deploy(String[] args) {
        String jarLocation = args[1];
        final String inputPath = args[2];
        final String outputPath = args[3];
        final String key = args[4];
        final String password = args[5];
        String s3_bucketname = args[6];
        String s3_logs = args[7];

        String instanceType = "m1.medium";

        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(key, password);

        AmazonElasticMapReduceClient client = new AmazonElasticMapReduceClient(awsCredentials);
        client.setEndpoint("https://elasticmapreduce.us-east-1.amazonaws.com");

        StepFactory stepFactory = new StepFactory();

        StepConfig enabledebugging = new StepConfig().withName("Enable debugging")
                .withActionOnFailure("TERMINATE_CLUSTER").withHadoopJarStep(stepFactory.newEnableDebuggingStep());
        HadoopJarStepConfig customJarStep = new HadoopJarStepConfig(jarLocation);

        customJarStep.withArgs("run", inputPath, outputPath, s3_bucketname);

        StepConfig customJar = new StepConfig()
                .withName("Custom Jar")
                .withActionOnFailure("TERMINATE_CLUSTER")
                .withHadoopJarStep(customJarStep);

        JobFlowInstancesConfig jobFlowInstancesConfig = new JobFlowInstancesConfig()
                .withHadoopVersion("1.0.3")
                .withInstanceCount(1)
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withTerminationProtected(false)
                .withMasterInstanceType(instanceType)
                .withSlaveInstanceType(instanceType);

        Tag[] tags = { new Tag("app_name", "msc")};

        RunJobFlowRequest request = new RunJobFlowRequest().withName("msc-emr" + org.apache.commons.lang.RandomStringUtils.randomAlphanumeric(16))
                .withVisibleToAllUsers(true)
                .withSteps(enabledebugging, customJar)
                .withLogUri(s3_logs)
                .withTags(tags)
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withServiceRole("EMR_DefaultRole")
                .withInstances(jobFlowInstancesConfig);

        client.runJobFlow(request);

    }

    @Override
    public int run(String[] args) throws Exception {

        logger.info("Begin job setup");
        // // s3n://KEY:password@bucket/folders/
        final String inputPath = args[1];
        final String outputPath = args[2]+org.apache.commons.lang.RandomStringUtils.randomAlphanumeric(16); // s3n://KEY:password@bucket/folders/
        String s3_bucketname = args[3];

        logger.info("input path: " + inputPath);
        logger.info("output  path: " + outputPath);
        
        
        Configuration conf = new Configuration(getConf());
        conf.set("s3.bucketname", s3_bucketname);
        conf.set("outputPath", outputPath);
        
        Job job = new Job(conf, "EmrMsc");
        
        job.setJarByClass(getClass());
        job.setMapperClass(CSVFilteringMapper.class);
        job.setReducerClass(TextFileReducer.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        SequenceFileInputFormat.addInputPath(job, new Path(inputPath));

        TextOutputFormat.setCompressOutput(job, true);
        TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        TextOutputFormat.setOutputPath(job, new Path(outputPath));
        // No of output files
        job.setNumReduceTasks(1);

        logger.info("Job setup complete");

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }
}
