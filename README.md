# Mapreduce_clustering

It's an example to use the Clustering alogorithm with Mapreduce technique

Mapper <Centeroid_index, data_point> 
    1) finds the minimum Euclidian distance between the centroid and the data point.
    2) writes the context with key = centroid and value = data point.

Reducer<index,datapoints>
    1) Maps all the same centroids indexes and find the mean(average) distance
    2) writes the new centroids to output file

Iterates the above Mapper and Reducer till the centroids get converged.
