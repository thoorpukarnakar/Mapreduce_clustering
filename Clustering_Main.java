package com.mr.research.unsupervised.k_means;

import com.mr.research.unsupervised.k_means.Utils.Configuration_settings;
import com.mr.research.unsupervised.k_means.Utils.Hdfs_Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public class Clustering_Main {
    public static void main(String[] args) throws Exception {

        String[] clustering_args = new GenericOptionsParser(new Configuration(),args).getRemainingArgs();
        String config_file = clustering_args[0];
        Hdfs_Utils fs_utils = new Hdfs_Utils();

        FileSystem hdfs_fs = FileSystem.get(new Configuration());
        Configuration_settings fs_config = fs_utils.load_config(hdfs_fs.open(new Path(config_file)));


        Configuration conf = new Configuration();
        conf.set("fs_config_set",config_file);

        List<Double[]> centroids = fs_utils.createRandomCentroids(Integer.parseInt(fs_config.getHdfs().get("centroids_number")));

        String centroidsFile = fs_utils.getFormattedCentroids(centroids);
        fs_utils.writeCentroids(conf, centroidsFile);

        boolean converged = false;
        int iteration = 0;
        do {

            //conf.set(Constants.OUTPUT_FILE_ARG, otherArgs[1] + "-" + iteration);

            // executes hadoop job
            if (!launchJob(conf,config_file,fs_config)) {
                System.exit(1);
            }

            // reads reducer output file
            String newCentroids = fs_utils.readReducerOutput(conf);

            // if the output of the reducer equals the old one
            if (centroidsFile.equals(newCentroids)) {

                // it means that the iteration is finished
                converged = true;
            }
            else {

                // writes the reducers output to distributed cache
                fs_utils.writeCentroids(conf, newCentroids);
            }

            centroidsFile = newCentroids;
            iteration ++;

        } while (!converged);


    }
    private static boolean launchJob(Configuration configuration, String config_file,Configuration_settings fs_config) throws Exception {

        Job job = Job.getInstance(configuration);
        job.setJobName("KMeans");
        job.setJarByClass(Clustering_Main.class);

        job.setMapperClass(ClusteringMapper.class);
        job.setReducerClass(ClusteringReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        job.addCacheFile(new Path(config_file).toUri());

        FileInputFormat.addInputPath(job,new Path(fs_config.getHdfs().get("data_set") ));
        FileOutputFormat.setOutputPath(job, new Path(fs_config.getHdfs().get("output") ));

        return job.waitForCompletion(true);
    }
    public static class ClusteringMapper extends Mapper<Object, Text, IntWritable, Text>
    {
        public static List<Double[]> centroids;
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            Hdfs_Utils fs_utils = new Hdfs_Utils();
            centroids = fs_utils.readCentroids(cacheFiles[0].toString());
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] xy = value.toString().split(" ");
            double x = Double.parseDouble(xy[0]);
            double y = Double.parseDouble(xy[1]);
            int index = 0;
            double minDistance = Double.MAX_VALUE;
            Hdfs_Utils fs_utils = new Hdfs_Utils();
            for (int j = 0; j < centroids.size(); j++) {
                double distance = fs_utils.euclideanDistance(centroids.get(j)[0], centroids.get(j)[1], x, y);
                if (distance < minDistance) {
                    index = j;
                    minDistance = distance;
                }
            }

            context.write(new IntWritable(index), value);
        }

    }
    public static class ClusteringReducer extends Reducer<IntWritable, Text, Text, IntWritable>{
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Double mx = 0d;
            Double my = 0d;
            int counter = 0;

            for (Text value: values) {
                String[] temp = value.toString().split(" ");
                mx += Double.parseDouble(temp[0]);
                my += Double.parseDouble(temp[1]);
                counter ++;
            }

            mx = mx / counter;
            my = my / counter;
            String centroid = mx + " " + my;

            context.write(new Text(centroid), key);
        }
    }

}
