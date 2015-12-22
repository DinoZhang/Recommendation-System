package org.dinozhang.storm.spout;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class RatingsReader extends BaseRichSpout {

    private static final long    serialVersionUID = 2197521792014017918L;
    private String               inputPath;

    private SpoutOutputCollector collector;

    @Override
    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        inputPath = (String) conf.get("INPUT_PATH_RATTING");
    }

    @Override
    public void nextTuple() {
        //		Collection<File> files = FileUtils.listFiles(new File(inputPath),FileFilterUtils.notFileFilter(FileFilterUtils
        //            .suffixFileFilter(".DS_Store")), null);
        Collection<File> files = FileUtils.listFiles(new File(inputPath),
            FileFilterUtils.and(FileFilterUtils.suffixFileFilter(".dat")), null);

        //		Collection<File> files = FileUtils.listFiles(new File("D://test/"),
        //				FileFilterUtils.notFileFilter(FileFilterUtils
        //						.suffixFileFilter(".bak")), null);
        if (files != null) {
            for (File f : files) {
                try {

                    List<String> lines = FileUtils.readLines(f, "UTF-8");
                    for (String line : lines) {
                        String[] mo = line.split("::");
                        String user_id = mo[0];
                        String movie_id = mo[1];
                        String ratting = mo[2];
                        String timestamp = mo[3];
                        collector.emit(new Values(user_id, movie_id, ratting, timestamp));
                    }
                    FileUtils.moveFile(f, new File(f.getPath() + System.currentTimeMillis()
                                                   + ".bak"));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("user_id", "movie_id", "ratting", "timestamp"));
    }

    //    public static void main(String[] args) {
    //
    //        RatingsReader mr = new RatingsReader();
    //        mr.nextTuple();
    //
    //    }
}
