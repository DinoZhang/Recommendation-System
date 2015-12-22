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

public class MovieReader extends BaseRichSpout {

    private static final long    serialVersionUID = 2197521792014017918L;
    private String               inputPath;

    private SpoutOutputCollector collector;

    @Override
    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        inputPath = (String) conf.get("INPUT_PATH");
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
                        String id = mo[0];
                        String name = mo[1].substring(0, mo[1].lastIndexOf("(") - 1);
                        String year = mo[1].substring(mo[1].lastIndexOf("(") + 1,
                            mo[1].lastIndexOf(")"));
                        /*List<String> type = new ArrayList<String>();
                        String[] t = mo[2].split("\\|");
                        for (int i = 0; i < t.length; i++) {
                        	type.add(t[i]);
                        }*/
                        StringBuffer type = new StringBuffer();
                        String[] t = mo[2].split("\\|");
                        for (int i = 0; i < t.length; i++) {
                            type.append(t[i] + ",");
                        }
                        String hobby = type.substring(0, type.length() - 1);
                        //System.out.print("123\n");
                        System.out.print(id + name + year + hobby);
                        collector.emit(new Values(id, name, year, hobby));
                    }

                    //String lines = "";
                    /*String[] mo = f.split("::");
                    String id=mo[0];
                    String name=mo[1].substring(0, mo[1].lastIndexOf("(") - 1);
                    String year=mo[1].substring(mo[1].lastIndexOf("(") + 1,
                    		mo[1].lastIndexOf(")"));
                    List<String> type = new ArrayList<String>();
                    String[] t = mo[2].split("\\|");
                    for (int i = 0; i < t.length; i++) {
                    	type.add(t[i]);
                    }
                    System.out.print("123");
                    System.out.print(id+name+year+type);*/

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
        //declarer.declare(new Fields("line"));
        declarer.declare(new Fields("id", "name", "year", "hobby"));
    }

    public static void main(String[] args) {

        MovieReader mr = new MovieReader();
        mr.nextTuple();

    }
}
