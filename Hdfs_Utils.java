package com.mr.research.unsupervised.k_means.Utils;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.mr.research.unsupervised.k_means.Utils.Configuration_settings;
import com.mr.research.unsupervised.k_means_cluster.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.htrace.fasterxml.jackson.core.JsonFactory;
import org.apache.htrace.fasterxml.jackson.core.JsonGenerator;
import org.apache.htrace.fasterxml.jackson.databind.MappingJsonFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Hdfs_Utils {


    public Configuration_settings load_config(DataInputStream config_set) throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        JsonFactory f = new MappingJsonFactory();
        f.configure(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT,false);

        return mapper.readValue(config_set,Configuration_settings.class);
    }
    public List<Double[]> createRandomCentroids(int centroidsNumber) {

        List<Double[]> centroids = new ArrayList<>();
        for (int j = 0; j < centroidsNumber; j++) {
            Double[] centroid = new Double[2];
            centroid[0] = Math.random() * 2;
            centroid[1] = Math.random() * 2;
            centroids.add(centroid);
        }
        return centroids;
    }

    public String getFormattedCentroids(List<Double[]> centroids) {

        int counter = 0;
        StringBuilder centroidsBuilder = new StringBuilder();
        for (Double[] centroid : centroids) {
            centroidsBuilder.append(centroid[0].toString());
            centroidsBuilder.append(" ");
            centroidsBuilder.append(centroid[1].toString());
            centroidsBuilder.append("\t");
            centroidsBuilder.append("" + counter++);
            centroidsBuilder.append("\n");
        }

        return centroidsBuilder.toString();
    }
    public void writeCentroids(Configuration configuration, String formattedCentroids) throws IOException {

        FileSystem fs = FileSystem.get(configuration);
        Configuration_settings fs_set = load_config(fs.open(new Path(configuration.get("fs_config_set"))));

        FSDataOutputStream fin = fs.create(new Path(fs_set.getHdfs().get("Centroids")));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fin));
        bw.append(formattedCentroids);
        bw.close();
    }
    public  double euclideanDistance(double x1, double y1, double x2, double y2) {
        return Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2));
    }

    public  String readReducerOutput(Configuration configuration) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        Configuration_settings fs_set = load_config(fs.open(new Path(configuration.get("fs_config_set"))));
        FSDataInputStream dataInputStream = new FSDataInputStream(fs.open(new Path(fs_set.getHdfs().get("output") + "/part-r-00000")));
        BufferedReader reader = new BufferedReader(new InputStreamReader(dataInputStream));
        StringBuilder content = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            content.append(line).append("\n");
        }

        return content.toString();
    }

    public static List<Double[]> readCentroids(String filename) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));
        return  readData(reader);
    }

    private static List<Double[]> readData(BufferedReader reader) throws IOException {
        List<Double[]> centroids = new ArrayList<>();
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                String[] values = line.split("\t");
                String[] temp = values[0].split(" ");
                Double[] centroid = new Double[2];
                centroid[0] = Double.parseDouble(temp[0]);
                centroid[1] = Double.parseDouble(temp[1]);
                centroids.add(centroid);
            }
        }
        finally {
            reader.close();
        }
        return centroids;
    }
}

