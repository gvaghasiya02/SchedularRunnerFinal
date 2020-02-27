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

import java.util.ArrayList;
import java.util.Iterator;

import config.Constants;

/**
 * @author pouria
 */
public class QueryStat {

    int qid;
    ArrayList<Long> clientTimes;
    ArrayList<Long> elapsedTimes;
    ArrayList<Long> executionTimes;
    double totalTime;
    double totalCount;

    public QueryStat() {
        this(Constants.INVALID_QID);
    }

    public QueryStat(int qId) {
        qid = qId;
        clientTimes = new ArrayList<Long>();
        totalTime = totalCount = 0;
    }

    public void reset(double lastAvg, double lastRSSize, double lastCount) {
        clientTimes.clear();
        if (lastAvg >= 0) {
            totalTime += (lastAvg * lastCount);
            totalCount += lastCount;
        }
    }

    public void setQid(int qId) {
        qid = qId;
    }

    public void addStat(long time) {
        clientTimes.add(time);
    }

    public String getClientTimes() {
        StringBuffer sb = new StringBuffer();
        for (long t : clientTimes) {
            sb.append(t + "\t");
        }
        return sb.toString();
    }
    public String getTimesForChart(){
        StringBuffer sb = new StringBuffer();
        int index=0;
        for (long t : clientTimes) {
            if(index>0)
                sb.append(",");
            sb.append(t);
            index++;
        }
        return sb.toString();
    }

    public String getIterations() {
        int i=1;
        String result="";
        for(long t: clientTimes) {
            if(i > 1)
                result = result+",";
            result = result+i;
            i++;
        }
        return result;
    }

    public int getTotalSize() {
        return clientTimes.size();
    }

    public double getAverageRT(int ignore) {
        if (ignore >= clientTimes.size()) {
            return Constants.INVALID_TIME;
        }
        double sum = 0;
        double count = 0;
        int skip = 0;
        Iterator<Long> it = clientTimes.iterator();
        while (it.hasNext()) {
            double d = it.next();
            if ((++skip) > ignore) {
                sum += d;
                count++;
            }
        }
        return sum / count;
    }

    public double getSTD(int ignore, double avg){
        double standardDeviation = 0;
        double count = 0;
        int skip = 0;
        Iterator<Long> it = clientTimes.iterator();
        while (it.hasNext()) {
            double d = it.next();
            if ((++skip) > ignore) {
                standardDeviation += Math.pow(d - avg, 2);
                count++;
            }
        }
        return Math.sqrt(standardDeviation/count);
    }
}
