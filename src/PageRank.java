import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.util.UUID;

public class PageRank {

    public static void main(String[] args) throws Exception {

        //preprocess job
        Configuration preProcessConf = new Configuration();
        Job preProcessJob = Job.getInstance(preProcessConf, "preprocess");
        preProcessJob.setJarByClass(PageRank.class);
        preProcessJob.setMapperClass(PRPreProcess.PreProcessMapper.class);
        //implement combiner later
        //job.setCombinerClass();
        preProcessJob.setReducerClass(PRPreProcess.PreProcessReducer.class);

        preProcessJob.setMapOutputKeyClass(LongWritable.class);
        preProcessJob.setMapOutputValueClass(LongWritable.class);

        preProcessJob.setOutputKeyClass(LongWritable.class);
        preProcessJob.setOutputValueClass(PRNodeWritable.class);

//        preProcessJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        Path inputPath = new Path(args[0]);
        FileInputFormat.addInputPath(preProcessJob, inputPath);

        Path preProcessedOutputPath = new Path("preProcessedOutput-" + UUID.randomUUID());
        FileOutputFormat.setOutputPath(preProcessJob, preProcessedOutputPath);

        if (!preProcessJob.waitForCompletion(true)) {
            System.exit(1);
        }

        Path pageRankInputPath = preProcessedOutputPath;
        Path pageRankOutputPath = new Path("pageRankOutput-" + UUID.randomUUID());

        int iteration = Integer.parseInt(args[2]);
        //pagerank job
        for (int i = 0; i < iteration; i++) {
            Configuration pageRankConf = new Configuration();
            Job pageRankJob = Job.getInstance(pageRankConf, "pagerank");
            pageRankJob.setJarByClass(PageRank.class);
            pageRankJob.setMapperClass(PRPreProcess.PreProcessMapper.class);
            //implement combiner later
            //pageRankJob.setCombinerClass();
            pageRankJob.setReducerClass(PRPreProcess.PreProcessReducer.class);

            pageRankJob.setMapOutputKeyClass(LongWritable.class);
            pageRankJob.setMapOutputValueClass(LongWritable.class);

            pageRankJob.setOutputKeyClass(LongWritable.class);
            pageRankJob.setOutputValueClass(PRNodeWritable.class);

            pageRankJob.setOutputFormatClass(SequenceFileOutputFormat.class);

            pageRankOutputPath = new Path("pageRankOutput-" + UUID.randomUUID());

            FileInputFormat.addInputPath(pageRankJob, pageRankInputPath);
            FileOutputFormat.setOutputPath(pageRankJob, pageRankOutputPath);

            if (!preProcessJob.waitForCompletion(true)) {
                System.exit(1);
            }

            //run pagerank adjust
            Configuration pageRankAdjustConf = new Configuration();
            Job pageRankAdjustJob = Job.getInstance(pageRankAdjustConf, "pagerank-adjust");
            pageRankAdjustJob.setJarByClass(PageRank.class);
            pageRankAdjustJob.setMapperClass(PRPreProcess.PreProcessMapper.class);
            //implement combiner later
            //pageRankJob.setCombinerClass();
            pageRankAdjustJob.setReducerClass(PRPreProcess.PreProcessReducer.class);

            pageRankAdjustJob.setMapOutputKeyClass(LongWritable.class);
            pageRankAdjustJob.setMapOutputValueClass(LongWritable.class);

            pageRankAdjustJob.setOutputKeyClass(LongWritable.class);
            pageRankAdjustJob.setOutputValueClass(PRNodeWritable.class);

            pageRankAdjustJob.setOutputFormatClass(SequenceFileOutputFormat.class);

            Path pageRankAdjustInputPath = pageRankOutputPath;
            Path pageRankAdjustOutputPath = new Path("pageRankAdjustOutput-" + UUID.randomUUID());

            FileInputFormat.addInputPath(pageRankAdjustJob, pageRankAdjustInputPath);
            FileOutputFormat.setOutputPath(pageRankAdjustJob, pageRankAdjustOutputPath);

            if (!preProcessJob.waitForCompletion(true)) {
                System.exit(1);
            }

            pageRankInputPath = pageRankAdjustOutputPath;

        }

        System.exit(0);


    }
}
