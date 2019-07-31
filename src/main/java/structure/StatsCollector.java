/*
 * Copyright by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package structure;

import queryGenerator.RandomQueryGenerator;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.DoubleBinaryOperator;

/**
 * @author pouria
 */
public class StatsCollector {

    int ignore; //number of beginning iteration to ignore in average report
    HashMap<Pair, QueryStat> qvToStat;
    String statsFile; //the file to eventually dump final results into
    int counter; //for tracing purpose only
    HashMap<Pair,Double> qvToAvgtimeX;

    public StatsCollector(String statsFile, int ignore) {
        this.qvToStat = new HashMap<Pair, QueryStat>();
        this.qvToAvgtimeX = new HashMap<Pair, Double>();
        this.statsFile = statsFile;
        this.ignore = ignore;
        this.counter = 0;
    }
    public void setqvToAvgTimeX(){
        double i=0;
        Set<Pair> keys = qvToStat.keySet();
        Pair[] qvs = keys.toArray(new Pair[keys.size()]);
        Arrays.sort(qvs);
       for(Pair qv: qvs){
           qvToAvgtimeX.put(qv,i);
           i++;
       }
    }
    public void reset() {
        qvToStat.clear();
    }

    public void updateStat(int qid, int vid, long time) {
        Pair p = new Pair(qid, vid);
        if (!qvToStat.containsKey(p)) {
            qvToStat.put(p, new QueryStat(qid));
        }
        qvToStat.get(p).addStat(time);
    }

    public void report() {
//        partialReport(0, counter, statsFile);
//        increasePartitionChartReport(0,statsFile);
        LRreport(0,statsFile); //--commented this one out to get Stats in server
    }
    public void increasePartitionChartReport(int startRound, String fileName){
        try {
            setqvToAvgTimeX();
            PrintWriter pw = new PrintWriter(new File("./files/output/user_"+fileName));
            PrintWriter avgpw = new PrintWriter(new File("./files/output/avg/avg_"+fileName));
            if (startRound != 0) {
                ignore = -1;
            }
            StringBuffer tsb = new StringBuffer();
            StringBuffer avgsb = new StringBuffer();
            Set<Pair> keys = qvToStat.keySet();
            Pair[] qvs = keys.toArray(new Pair[keys.size()]);
            Arrays.sort(qvs);
            for (Pair p : qvs) {
                int partition = (Integer)RandomQueryGenerator.qps.getParam(p.qid,p.vid).get(2);
                QueryStat qs = qvToStat.get(p);
                tsb.append(partition).append("\t").append(qs.getIterations()).append("\t").append(qs.getTimesForChart()).append("\n");
                double partialAvg = qs.getAverageRT(ignore);
                double partialSTD = qs.getSTD(ignore,partialAvg);
                avgsb.append("Q").append(p.toString()).append("\t").append("avgRT: ").append(partialAvg).append(" STD:" ).append(partialSTD).append("\n");
                //avgsb.append(partition).append("\t").append(partialAvg).append("\n");
            }
            if (avgsb != null) {
                avgpw.println(avgsb.toString());
            }
            pw.println(tsb.toString());
            pw.close();
            avgpw.close();

        } catch (IOException e) {
            System.err.println("Problem in creating report in StatsCollector !");
            e.printStackTrace();
        }
    }
    public void chartReport(int startRound, String fileName){
        try {
            setqvToAvgTimeX();
            PrintWriter pw = new PrintWriter(new File("./files/output/user_"+fileName+Thread.currentThread().getName()));
            PrintWriter avgpw = new PrintWriter(new File("./files/output/avg/avg_"+fileName+Thread.currentThread().getName()));
            if (startRound != 0) {
                ignore = -1;
            }
            StringBuffer tsb = new StringBuffer();
            StringBuffer avgsb = new StringBuffer();
            Set<Pair> keys = qvToStat.keySet();
            Pair[] qvs = keys.toArray(new Pair[keys.size()]);
            Arrays.sort(qvs);
            for (Pair p : qvs) {
                QueryStat qs = qvToStat.get(p);
                tsb.append("Q").append(p.toString()).append("\t").append(qs.getIterations()).append("\t").append(qs.getTimesForChart()).append("\n");
                double partialAvg = qs.getAverageRT(ignore);
                double partialSTD = qs.getSTD(ignore,partialAvg);
                avgsb.append("Q").append(p.toString()).append("\t").append("avgRT: ").append(partialAvg).append(" STD:" ).append(partialSTD).append("\n");
                //avgsb.append("Q").append(p.toString()).append("\t").append(partialAvg).append("\n");
            }
            if (avgsb != null) {
                avgpw.println(avgsb.toString());
            }
            pw.println(tsb.toString());
            pw.close();
            avgpw.close();

        } catch (IOException e) {
            System.err.println("Problem in creating report in StatsCollector !");
            e.printStackTrace();
        }
    }
    public void increaseMemoryChartReport(int startRound, String fileName){
        try {
            setqvToAvgTimeX();
            PrintWriter pw = new PrintWriter(new File("./files/output/user_"+Thread.currentThread().getName()+fileName));
            PrintWriter avgpw = new PrintWriter(new File("./files/output/avg/avg_"+Thread.currentThread().getName()+fileName));
            if (startRound != 0) {
                ignore = -1;
            }
            StringBuffer tsb = new StringBuffer();
            StringBuffer avgsb = new StringBuffer();
            Set<Pair> keys = qvToStat.keySet();
            Pair[] qvs = keys.toArray(new Pair[keys.size()]);
            Arrays.sort(qvs);
            for (Pair p : qvs) {
                double memory = (Double)RandomQueryGenerator.qps.getParam(p.qid,p.vid).get(0);
                QueryStat qs = qvToStat.get(p);
                tsb.append(memory).append("\t").append(qs.getIterations()).append("\t").append(qs.getTimesForChart()).append("\n");
                double partialAvg = qs.getAverageRT(ignore);
                avgsb.append(memory).append("\t").append(partialAvg).append("\n");
            }
            if (avgsb != null) {
                avgpw.println(avgsb.toString());
            }
            pw.println(tsb.toString());
            pw.close();
            avgpw.close();

        } catch (IOException e) {
            System.err.println("Problem in creating report in StatsCollector !");
            e.printStackTrace();
        }
    }


    private void LRreport(int startRound, String fileName) {
        try {
            setqvToAvgTimeX();
            PrintWriter pw = new PrintWriter(new File("./files/output/user_"+Thread.currentThread().getName()+fileName));
            PrintWriter avgpw = new PrintWriter(new File("./files/output/avg/avg_"+Thread.currentThread().getName()+fileName));
            if (startRound != 0) {
                ignore = -1;
            }
            StringBuffer tsb = new StringBuffer();
            StringBuffer avgsb = new StringBuffer();
            Set<Pair> keys = qvToStat.keySet();
            Pair[] qvs = keys.toArray(new Pair[keys.size()]);
            Arrays.sort(qvs);
            for (Pair p : qvs) {
                    QueryStat qs = qvToStat.get(p);
                    tsb.append("Q-").append(p.getQId()).append("-").append(p.getVId()).append("\t").append("\t").append(qs.getIterations()).append(
                            "\t").append(qs.getTimesForChart()).append("\n");
                    double partialAvg = qs.getAverageRT(ignore);
                    double partialSTD = qs.getSTD(ignore,partialAvg);
                    avgsb.append("Q").append(p.toString()).append("\t").append("avgRT: ").append(partialAvg).append(
                            " STD:" ).append(partialSTD).append("\n");
            }
            if (avgsb != null) {
                avgpw.println(avgsb.toString());
            }
            pw.println(tsb.toString());
            pw.close();
            avgpw.close();

        } catch (IOException e) {
            System.err.println("Problem in creating report in StatsCollector !");
            e.printStackTrace();
        }
    }


    private void partialReport(int startRound, int endRound, String fileName) {
        try {
            PrintWriter pw = new PrintWriter(new File("./files/output/reg_"+fileName));
            if (startRound != 0) {
                ignore = -1;
            }

            DateFormat df = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss");
            Date dateobj = new Date();
            String currentTime = df.format(dateobj);
            StringBuffer tsb = new StringBuffer();
            tsb.append("#Response Times (in ms per iteration)\n");
            StringBuffer avgsb = new StringBuffer(currentTime);
            avgsb.append("\n\n#Avg Times (first " + ignore + " round(s) excluded)\n");
            Set<Pair> keys = qvToStat.keySet();
            Pair[] qvs = keys.toArray(new Pair[keys.size()]);
            Arrays.sort(qvs);
            for (Pair p : qvs) {
                QueryStat qs = qvToStat.get(p);
                tsb.append("Q").append(p.toString()).append("\t").append(qs.getTimes()).append("\n");
                double partialAvg = -1;
                if (avgsb != null) {
                    partialAvg = qs.getAverageRT(ignore);
                    avgsb.append("Q").append(p.toString()).append("\t").append(partialAvg).append("\n");
                }
            }
            if (avgsb != null) {
                pw.println(avgsb.toString());
                pw.println("\n");
            }
            pw.println(tsb.toString());
            pw.close();

        } catch (IOException e) {
            System.err.println("Problem in creating report in StatsCollector !");
            e.printStackTrace();
        }
    }
}
