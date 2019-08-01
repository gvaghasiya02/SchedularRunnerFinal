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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

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
        this.qvToStat = new HashMap<>();
        this.qvToAvgtimeX = new HashMap<>();
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

    public void updateStat(int qid, int vid, long time) {
        Pair p = new Pair(qid, vid);
        if (!qvToStat.containsKey(p)) {
            qvToStat.put(p, new QueryStat(qid));
        }
        qvToStat.get(p).addStat(time);
    }

    public void report() {
        generateReport(0,statsFile);
    }

    private void generateReport(int startRound, String fileName) {
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
            avgsb.append("[");
            tsb.append("[");
            int resCount = 0;
            for (Pair p : qvs) {
                if (resCount > 0){
                    tsb.append(",");
                    avgsb.append(",");
                }
                    QueryStat qs = qvToStat.get(p);
                    tsb.append("{\n \"qidvid\":\""+ p.toString()).append("\", \n")
                            .append("\"iterations\":[").append(qs.getIterations()).append("],\n")
                            .append("\"RT\":[").append(qs.getTimesForChart()).append("]\n}\n");
                    double partialAvg = qs.getAverageRT(ignore);
                    double partialSTD = qs.getSTD(ignore,partialAvg);
                    avgsb.append("\n{\n \"qidvid\":\""+ p.toString()).append("\", \n").
                            append("\"avgRT\":").append(partialAvg).append(",\n")
                            .append("\"STD\":" ).append(partialSTD).append("\n}\n");
                    resCount++;
            }
            tsb.append("]");
            avgsb.append("]");
            avgpw.println(avgsb.toString());
            pw.println(tsb.toString());
            pw.close();
            avgpw.close();

        } catch (IOException e) {
            System.err.println("Problem in creating report in StatsCollector !");
            e.printStackTrace();
        }
    }
}
