package org.dinozhang.storm.topology;

import org.dinozhang.storm.bolt.MovieSpliter;
import org.dinozhang.storm.bolt.MovieToMysql;
import org.dinozhang.storm.bolt.RatingsSpliter;
import org.dinozhang.storm.bolt.RatingsToMysql;
import org.dinozhang.storm.spout.MovieReader;
import org.dinozhang.storm.spout.RatingsReader;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class RecommendDataTopo {

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("input: inputPah inputPah timeOffset");
            System.err.println("such as : java -jar  RecommendDataToPo.jar D://input/ D://input/ 2");
            System.exit(2);
        }
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("movie-reader", new MovieReader());
        builder.setBolt("movie-spilter", new MovieSpliter()).shuffleGrouping("movie-reader");
        builder.setBolt("MovieToMysql", new MovieToMysql()).shuffleGrouping("movie-spilter");
        //rating topo
        builder.setSpout("ratings-reader", new RatingsReader());
        builder.setBolt("ratings-spilter", new RatingsSpliter()).shuffleGrouping("ratings-reader");
        builder.setBolt("ratingsToMysql", new RatingsToMysql()).shuffleGrouping("ratings-spilter");
        String inputPahtMovie = args[0];
        String inputPahtRating = args[1];
        String timeOffset = args[1];
        Config conf = new Config();
        conf.put("INPUT_PATH_MOVIE", inputPahtMovie);
        conf.put("INPUT_PATH_RATTING", inputPahtRating);
        conf.put("TIME_OFFSET", timeOffset);
        conf.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("RecommendDataToPo", conf, builder.createTopology());

    }
}
