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
package queryGenerator;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Random;

import config.Constants;
import datatype.*;

public class RandomQueryGenerator {

    DateTimeArgument START_DATE;
    DateTimeArgument END_DATE;
    long MAX_ID;
    long MIN_ID;

    Random rand;
    long seed;
    ArrayList<IArgument> args;

    public static QueryParamSetting qps;

    public RandomQueryGenerator(long seed, long maxUsrId) {
        this.rand = new Random(seed);
        setStartDate(ArgumentParser.parseDateTime(Constants.DEFAULT_START_DATE));
        setEndDate(ArgumentParser.parseDateTime(Constants.DEFAULT_END_DATE));
        setMaxFbuId(maxUsrId);
        this.args = new ArrayList<IArgument>();
    }
    public RandomQueryGenerator(long seed, long minUserId,long maxUsrId) {
        this(seed,maxUsrId);
        this.MIN_ID=minUserId;
        this.MAX_ID = maxUsrId;
    }
    public ArrayList<IArgument> nextQuery(int qIx, int vIx) {
        args.clear();
        switch (qIx) {
            case 5:
                nextQ5(qIx, vIx);
                break;
            case 101: //PK lookup
                nextQ101();
                break;
            case 102: //PK range scan
                nextQ102(qIx, vIx);
                break;
            case 103: //non-unique temporal attribute range scan
                nextQ103(qIx, vIx);
                break;
            case 104: //e-quantification
                nextQ104(qIx, vIx);
                break;
            case 105: //u-quantification 
                nextQ105(qIx, vIx);
                break;
            case 106: //global aggregation
                nextQ106(qIx, vIx);
                break;
            case 107: //grp and aggregation
                nextQ107(qIx, vIx);
                break;
            case 108: //top-k
                nextQ108(qIx, vIx);
                break;
            case 109: //spatial selection
                nextQ109(vIx);
                break;
            case 1010: //exact text search
                nextQ1010(vIx);
                break;
            case 1011: //text similarity search
                nextQ1011(vIx);
                break;
            case 1012: //equi-join
            case 2012: //equi-IX-join
                nextQ1012(qIx, vIx);
                break;
            case 1013: //left-Outer-Join
            case 2013: //left-Outer-IX-Join
                nextQ1013(qIx, vIx);
                break;
            case 1014: //join and grp
            case 2014: //IX-join and grp
                nextQ1014(qIx, vIx);
                break;
            case 1015: //join and top-k
            case 2015: //IX-join and top-k
                nextQ1015(qIx, vIx);
                break;
            case 1016: //spatial Join
                nextQ1016(qIx, vIx);
                break;
            case 3000:
            case 3002:
            case 3003:
                nextQ3000(qIx,vIx);
                break;
            case 3001:
                nextQ3001(qIx,vIx);
                break;
            case 3004:
                nextQ3004(qIx,vIx);
                break;
            case 3005:
                nextQ3005(qIx,vIx);
                break;
            case 3006:
                nextQ3006(qIx,vIx);
                break;
            case 3007:
            case 1:
                nextQ1(qIx,vIx);
                break;
            case 2:
                nextQ2(qIx,vIx);
                break;
            case 15:
                nextQ15(qIx, vIx);
                break;
            case 16:
                nextQ16(qIx,vIx);
                break;
            case 50:
            case 51:
            case 52:
            case 53:
                nextQ50(qIx,vIx);
                break;
            case 54:
                nextQ54(qIx,vIx);
                break;
            case 61:
                nextQ61(qIx, vIx);
                break;
            case 62:
                nextQ62(qIx, vIx);
                break;
            case 63:
                nextQ63(qIx, vIx);
                break;
            case 64:
                nextQ64(qIx, vIx);
                break;
            case 65:
                nextQ65(qIx, vIx);
                break;
            case 66:
                nextQ66(qIx, vIx);
                break;
            case 80:
                nextQ80(qIx, vIx);
                break;
            case 3008:
            case 3018:
                nextQ3008(qIx,vIx);
                break;
            case 3009:
            case 3013:
                nextQ3009(qIx,vIx);
                break;
            case 3010:
                nextQ3010(qIx,vIx);
                break;
            case 3011:
                nextQ3011(qIx,vIx);
                break;
            case 3019:
                nextQ3019(qIx,vIx);
                break;
            case 3020:
                nextQ3020(qIx,vIx);
                break;
            case 4000:
                nextQ4000(qIx,vIx);
                break;
            case 4100:
                nextQ4100(qIx,vIx);
                break;
            case 4200:
                nextQ4200(qIx,vIx);
                break;
            case 4300:
                nextQ4300(qIx,vIx);
                break;
            case 5201:
                nextQ5201(qIx,vIx);
                break;
            case 5401:
                nextQ5401(qIx,vIx);
                break;
            case 5601:
                nextQ5601(qIx,vIx);
                break;
            case 5801:
                nextQ5801(qIx,vIx);
                break;
            case 5210:
                nextQ5210(qIx,vIx);
                break;
            case 5410:
                nextQ5410(qIx,vIx);
                break;
            case 5610:
                nextQ5610(qIx,vIx);
                break;
            case 5810:
                nextQ5810(qIx,vIx);
                break;
            case 6100:
            case 6200:
            case 6201:
            case 6202:
            case 6203:
                nextQ6100(qIx,vIx);
                break;
            case 6400:
                nextQ6400(qIx,vIx);
                break;
            case 7000:
            case 7001:
            case 7002:
            case 7003:
            case 7004:
            case 7005:
            case 7006:
            case 7007:
            case 7008:
            case 7009:
                nextQ7000(qIx,vIx);
                break;
            case 8000:
            case 8001:
                nextQ8000(qIx,vIx);
                break;
            case 8401:
                nextQ8401(qIx,vIx);
                break;
            case 8410:
                nextQ8410(qIx, vIx);
                break;
            case 8411:
                nextQ8411(qIx, vIx);
                break;
            case 9000:
            case 9001:
            case 9002:
            case 9003:
            case 9004:
            case 9005:
            case 9006:
            case 9007:
            case 9008:
            case 9009:
                nextQ9000(qIx,vIx);
                break;
            case 20000:
                nextQ20000(qIx, vIx);
                break;
            case 20001:
                nextQ20002(qIx, vIx,1);
                break;
            case 20002:
                nextQ20002(qIx, vIx,2);
                break;
            case 20003:
                nextQ20002(qIx, vIx,3);
                break;
            case 20004:
                nextQ20002(qIx, vIx,4);
                break;
            case 20005:
                nextQ20002(qIx, vIx,5);
                break;
            case 20006:
                nextQ20002(qIx, vIx,6);
                break;
            case 20007:
                nextQ20002(qIx, vIx,7);
                break;
            case 20008:
                nextQ20002(qIx, vIx,8);
                break;
            case 30001:
                nextQ30001(qIx, vIx,1);
                break;
            case 30002:
                nextQ30001(qIx, vIx,2);
                break;
            case 30003:
                nextQ30001(qIx, vIx,3);
                break;
            case 30004:
                nextQ30001(qIx, vIx,4);
                break;
            case 30005:
                nextQ30001(qIx, vIx,5);
                break;
            case 30006:
                nextQ30001(qIx, vIx,6);
                break;
            case 30007:
                nextQ30001(qIx, vIx,7);
                break;
            case 30008:
                nextQ30001(qIx, vIx,8);
                break;
            case 40001:
                nextQ40001(qIx, vIx,1);
                break;
            case 40002:
                nextQ40001(qIx, vIx,2);
                break;
            case 40003:
                nextQ40001(qIx, vIx,3);
                break;
            case 40004:
                nextQ40001(qIx, vIx,4);
                break;
            case 40005:
                nextQ40001(qIx, vIx,5);
                break;
            case 40006:
                nextQ40001(qIx, vIx,6);
                break;
            case 40007:
                nextQ40001(qIx, vIx,7);
                break;
            case 40008:
                nextQ40001(qIx, vIx,8);
                break;
            default:
                next(qIx,vIx);
                break;
        }
        return (new ArrayList<IArgument>(args));
    }

    private void next(int qid, int vid){
        ArrayList<Number> p = qps.getParam(qid,vid);
        for(Number element:p){
            args.add(new LongArgument((Integer)element));
        }
    }

    private void nextQ1(int qid, int vid) {
        int minMem = (Integer)qps.getParam(qid, vid).get(0);
        IntArgument joinMem = new IntArgument(minMem);
        int numPartitions = (Integer) qps.getParam(qid, vid).get(1);
        IntArgument bs = new IntArgument(numPartitions);
        args.add(joinMem);
        args.add(bs);
    }

    private void nextQ2(int qid, int vid) {
        int minMem = (Integer)qps.getParam(qid, vid).get(0);
        IntArgument joinMem = new IntArgument(minMem);
        int numPartitions = (Integer) qps.getParam(qid, vid).get(1);
        IntArgument np = new IntArgument(numPartitions);
        int buildSize = (Integer) qps.getParam(qid, vid).get(2);
        IntArgument bs = new IntArgument(buildSize);
        args.add(joinMem);
        args.add(np);
        args.add(bs);
    }

    private void nextQ101() {
        LongArgument k = randomLongArg(1,MAX_ID);
        args.add(k);
    }

    private void nextQ102(int qid, int vid) {
        Number len = qps.getParam(qid, vid).get(0);
        LongArgument s = randomLongArg(1,MAX_ID - (Integer)len);
        LongArgument e = new LongArgument(s.getValue() + (Integer)len);
        args.add(s);
        args.add(e);
    }

    private void nextQ103(int qid, int vid) {
        DateTimeArgument s = randomDateTime(START_DATE, END_DATE);
        int shift = (Integer)qps.getParam(qid, vid).get(0);
        DateTimeArgument e = shift(s, shift);
        args.add(s);
        args.add(e);
    }

    private void nextQ104(int qid, int vid) {
        DateTimeArgument s = randomDateTime(START_DATE, END_DATE);
        int shift = (Integer)qps.getParam(qid, vid).get(0);
        DateTimeArgument e = shift(s, shift);
        args.add(s);
        args.add(e);
    }

    private void nextQ105(int qid, int vid) {
        DateTimeArgument s = randomDateTime(START_DATE, END_DATE);
        int shift = (Integer)qps.getParam(qid, vid).get(0);
        DateTimeArgument e = shift(s, shift);
        args.add(s);
        args.add(e);
    }

    private void nextQ106(int qid, int vid) {
        DateTimeArgument s = randomDateTime(START_DATE, END_DATE);
        int shift =(Integer) qps.getParam(qid, vid).get(0);
        DateTimeArgument e = shift(s, shift);
        args.add(s);
        args.add(e);
    }

    private void nextQ107(int qid, int vid) {
        DateTimeArgument s = randomDateTime(START_DATE, END_DATE);
        int shift =(Integer) qps.getParam(qid, vid).get(0);
        DateTimeArgument e = shift(s, shift);
        args.add(s);
        args.add(e);
    }

    private void nextQ108(int qid, int vid) {
        DateTimeArgument s = randomDateTime(START_DATE, END_DATE);
        int shift = (Integer)qps.getParam(qid, vid).get(0);
        DateTimeArgument e = shift(s, shift);
        args.add(s);
        args.add(e);
    }

    private void nextQ109(int vid) {
        double[] location = getRandomLocation();
        DoubleArgument radius = getRadius(vid);
        args.add(new DoubleArgument(location[0]));
        args.add(new DoubleArgument(location[1]));
        args.add(radius);
    }

    private void nextQ1010(int vid) {
        args.add(getKeyword(vid));
    }

    private void nextQ1011(int vid) {
        args.add(getKeyword(vid));
    }

    private void nextQ1014(int qid, int vid) {
        DateTimeArgument s = randomDateTime(START_DATE, END_DATE);
        ArrayList<Number> p = qps.getParam(qid, vid);
        DateTimeArgument e = shift(s, (Integer)p.get(0));
        DateTimeArgument s2 = randomDateTime(START_DATE, END_DATE);
        DateTimeArgument e2 = shift(s2,(Integer) p.get(1));
        args.add(s);
        args.add(e);
        args.add(s2);
        args.add(e2);
    }

    private void nextQ1015(int qid, int vid) {
        DateTimeArgument s = randomDateTime(START_DATE, END_DATE);
        ArrayList<Number> p = qps.getParam(qid, vid);
        DateTimeArgument e = shift(s, (Integer)p.get(0));
        DateTimeArgument s2 = randomDateTime(START_DATE, END_DATE);
        DateTimeArgument e2 = shift(s2, (Integer)p.get(1));
        args.add(s);
        args.add(e);
        args.add(s2);
        args.add(e2);
    }

    private void nextQ1012(int qid, int vid) {
        DateTimeArgument s1 = randomDateTime(START_DATE, END_DATE);
        ArrayList<Number> p = qps.getParam(qid, vid);
        DateTimeArgument e1 = shift(s1,(Integer) p.get(0));
        DateTimeArgument s2 = randomDateTime(START_DATE, END_DATE);
        DateTimeArgument e2 = shift(s2, (Integer)p.get(1));

        args.add(s1);
        args.add(e1);
        args.add(s2);
        args.add(e2);
    }

    private void nextQ1013(int qid, int vid) {
        DateTimeArgument s1 = randomDateTime(START_DATE, END_DATE);
        ArrayList<Number> p = qps.getParam(qid, vid);
        DateTimeArgument e1 = shift(s1, (Integer)p.get(0));
        DateTimeArgument s2 = randomDateTime(START_DATE, END_DATE);
        DateTimeArgument e2 = shift(s2, (Integer)p.get(1));

        args.add(s1);
        args.add(e1);
        args.add(s2);
        args.add(e2);
    }

    private void nextQ1016(int qid, int vid) {
        DateTimeArgument s = randomDateTime(START_DATE, END_DATE);
        int shift = (Integer)qps.getParam(qid, vid).get(0);
        DateTimeArgument e = shift(s, shift);
        DoubleArgument r = getRadius(vid);
        args.add(s);
        args.add(e);
        args.add(r);
    }

    private void nextQ50(int qid, int vid){
        double minMem = 0.0;
        if(qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer)qps.getParam(qid, vid).get(0);
        }
        else{
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
            int buildsize = (Integer) qps.getParam(qid, vid).get(1);
            IntArgument bs = new IntArgument(buildsize);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);

    }

    private void nextQ61(int qid, int vid){
        double minMem = 0.0;
        if(qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer)qps.getParam(qid, vid).get(0);
        }
        else{
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
        int buildsize = (Integer) qps.getParam(qid, vid).get(1);
        IntArgument bs = new IntArgument(buildsize);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
    }

    private void nextQ80(int qid, int vid){
        double minMem = 0.0;
        if(qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer)qps.getParam(qid, vid).get(0);
        }
        else{
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
        int buildsize = (Integer) qps.getParam(qid, vid).get(1);
        IntArgument bs = new IntArgument(buildsize);
        int partitions = (Integer) qps.getParam(qid, vid).get(2);
        IntArgument p = new IntArgument(partitions);
        args.add(bs);
        args.add(p);
        args.add(bs);
        args.add(p);
        args.add(bs);
        args.add(p);
        args.add(bs);
        args.add(p);
        args.add(bs);
        args.add(p);
        args.add(bs);
        args.add(p);
        args.add(bs);
        args.add(p);
        args.add(bs);
        args.add(p);
    }

    private void nextQ62(int qid, int vid){
        double minMem = 0.0;
        if(qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer)qps.getParam(qid, vid).get(0);
        }
        else{
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
        int buildsize = (Integer) qps.getParam(qid, vid).get(1);
        IntArgument bs = new IntArgument(buildsize);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
    }

    private void nextQ63(int qid, int vid){
        double minMem = 0.0;
        if(qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer)qps.getParam(qid, vid).get(0);
        }
        else{
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
        int buildsize = (Integer) qps.getParam(qid, vid).get(1);
        IntArgument bs = new IntArgument(buildsize);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
    }

    private void nextQ64(int qid, int vid){
        double minMem = 0.0;
        if(qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer)qps.getParam(qid, vid).get(0);
        }
        else{
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
        int buildsize = (Integer) qps.getParam(qid, vid).get(1);
        IntArgument bs = new IntArgument(buildsize);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
    }

    private void nextQ65(int qid, int vid){
        double minMem = 0.0;
        if(qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer)qps.getParam(qid, vid).get(0);
        }
        else{
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
        int buildsize = (Integer) qps.getParam(qid, vid).get(1);
        IntArgument bs = new IntArgument(buildsize);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
    }

    private void nextQ66(int qid, int vid){
        double minMem = 0.0;
        if(qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer)qps.getParam(qid, vid).get(0);
        }
        else{
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
        int buildsize = (Integer) qps.getParam(qid, vid).get(1);
        IntArgument bs = new IntArgument(buildsize);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
    }
    private void nextQ15(int qid, int vid){
        double minMem = 0.0;
        if(qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer)qps.getParam(qid, vid).get(0);
        }
        else{
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
        int buildsize = (Integer) qps.getParam(qid, vid).get(1);
        IntArgument bs = new IntArgument(buildsize);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
    }

    private void nextQ16(int qid, int vid){
        double minMem = 0.0;
        if(qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer)qps.getParam(qid, vid).get(0);
        }
        else{
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
        int buildsize = (Integer) qps.getParam(qid, vid).get(1);
        IntArgument bs = new IntArgument(buildsize);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
        args.add(bs);
    }

    private void nextQ54(int qid, int vid){
        //global memory is given, 1/3 of it goes to the first 3, full goes to the last as there is a break between
        //third and fourth join
        int minMem = (Integer)qps.getParam(qid, vid).get(0);

        IntArgument firstThreejoinmem = new IntArgument(minMem/3);
        IntArgument lastjoinmem = new IntArgument(minMem);
        int buildsize = (Integer) qps.getParam(qid, vid).get(1);
        IntArgument bs = new IntArgument(buildsize);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(firstThreejoinmem);
        args.add(bs);
        args.add(lastjoinmem);
        args.add(bs);
        args.add(lastjoinmem);
        args.add(bs);
        args.add(lastjoinmem);
        args.add(bs);
        args.add(lastjoinmem);
        args.add(bs);
        args.add(lastjoinmem);
        args.add(bs);
        args.add(lastjoinmem);
        args.add(bs);
        args.add(lastjoinmem);
        args.add(bs);
        args.add(lastjoinmem);

    }

    private void nextQ3000(int qid, int vid){
        double minMem = 0.0;
        if(qps.getParam(qid, vid).get(0) instanceof Integer) {
             minMem = (Integer)qps.getParam(qid, vid).get(0);
        }
        else{
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }

        int len = (Integer)qps.getParam(qid, vid).get(1);
        LongArgument s = randomLongArg(1,MAX_ID - len);
        LongArgument e = new LongArgument(s.getValue() + len);
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
        args.add(s);args.add(e);//ds1
        args.add(s);args.add(e);//ds2
        args.add(s);args.add(e);//ds3
        args.add(s);args.add(e);//ds4
        args.add(s);args.add(e);//ds5
        args.add(s);args.add(e);//ds6
        args.add(s);args.add(e);//ds7
        args.add(s);args.add(e);//ds8
        args.add(s);args.add(e);//ds9

    }
    private void nextQ3004(int qid,int vid){
        nextQ3000(qid,vid);
        int partitionSize = (Integer)qps.getParam(qid,vid).get(2);
        args.add(new IntArgument(partitionSize));
        args.add(new IntArgument(partitionSize));
        args.add(new IntArgument(partitionSize));
        args.add(new IntArgument(partitionSize));
        args.add(new IntArgument(partitionSize));
        args.add(new IntArgument(partitionSize));
        args.add(new IntArgument(partitionSize));
        args.add(new IntArgument(partitionSize));

    }
    private void nextQ3005(int qid, int vid) {
        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(0);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
        int len = (Integer)qps.getParam(qid, vid).get(1);
        //LongArgument s = randomLongArg(1,MAX_ID - len);
        //LongArgument e = new LongArgument(s.getValue() + len);
        LongArgument s = new LongArgument(1);
        LongArgument e = new LongArgument(s.getValue() + len);
        args.add(s);args.add(e);//ds1
        args.add(s);args.add(e);//ds2
        args.add(s);args.add(e);//ds3
        if (qps.getParam(qid,vid).size() >= 2){
            int buildsize = (Integer)qps.getParam(qid, vid).get(2);
            IntArgument bs = new IntArgument(buildsize);
            args.add(bs);
            args.add(bs);
        }
    }
    private void nextQ3006(int qid, int vid) {
        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(0);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
        int len = (Integer)qps.getParam(qid, vid).get(1);
        int buildsize = 0;
        boolean isbuildsizeset = false;
        if (qps.getParam(qid,vid).size() >= 2) {
            buildsize = (Integer) qps.getParam(qid, vid).get(2);
            isbuildsizeset = true;
        }
        IntArgument bs = new IntArgument(buildsize);
        //LongArgument s = randomLongArg(1,MAX_ID - len);
        //LongArgument e = new LongArgument(s.getValue() + len);
        LongArgument s = new LongArgument(1);
        LongArgument e = new LongArgument(s.getValue() + len);
        if(isbuildsizeset)
            args.add(bs);
        args.add(s);args.add(e);//ds1
        args.add(s);args.add(e);//ds2
        if(isbuildsizeset)
            args.add(bs);
        args.add(s);args.add(e);//ds3
    }
    private void nextQ3001(int qid, int vid){
        int minMem = (Integer)qps.getParam(qid, vid).get(0);
        int len = (Integer)qps.getParam(qid, vid).get(1);
        LongArgument s = randomLongArg(1,MAX_ID - len);
        LongArgument e = new LongArgument(s.getValue() + len);
        args.add(new StringArgument(Integer.toString(minMem)+"MB"));
        args.add(s);args.add(e);//ds1
        args.add(s);args.add(e);//ds2
        args.add(s);args.add(e);//ds3
    }
    private void nextQ3008(int qid, int vid) {
        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(0);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
        int len = (Integer)qps.getParam(qid, vid).get(1);
        //LongArgument s = randomLongArg(1,MAX_ID - len);
        //LongArgument e = new LongArgument(s.getValue() + len);
        LongArgument s = new LongArgument(1);
        LongArgument e = new LongArgument(s.getValue() + len);
        args.add(s);args.add(e);//ds1
        args.add(s);args.add(e);//ds2
        args.add(s);args.add(e);//ds3
        args.add(s);args.add(e);//ds4
        if (qps.getParam(qid,vid).size() >= 2){
            int buildsize = (Integer)qps.getParam(qid, vid).get(2);
            IntArgument bs = new IntArgument(buildsize);
            args.add(bs);
            args.add(bs);
            args.add(bs);
        }
    }
    private void nextQ3009(int qid, int vid) {
        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(0);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
        int len = (Integer)qps.getParam(qid, vid).get(1);
        int buildsize = 0;
        boolean isbuildsizeset = false;
        if (qps.getParam(qid,vid).size() >= 2) {
             buildsize = (Integer) qps.getParam(qid, vid).get(2);
             isbuildsizeset = true;
        }
        IntArgument bs = new IntArgument(buildsize);
        //LongArgument s = randomLongArg(1,MAX_ID - len);
        //LongArgument e = new LongArgument(s.getValue() + len);
        LongArgument s = new LongArgument(1);
        LongArgument e = new LongArgument(s.getValue() + len);
        if(isbuildsizeset)
            args.add(bs);
        args.add(s);args.add(e);//ds1
        args.add(s);args.add(e);//ds2
        if(isbuildsizeset)
            args.add(bs);
        args.add(s);args.add(e);//ds3
        if(isbuildsizeset)
            args.add(bs);
        args.add(s);args.add(e);//ds4
    }
    private void nextQ3010(int qid, int vid) {
        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(0);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));

        double framesize = 0.0;
        if (qps.getParam(qid, vid).get(1) instanceof Integer) {
            framesize = (Integer) qps.getParam(qid, vid).get(1);
        } else {
            framesize = (Double) qps.getParam(qid, vid).get(1);
        }
        args.add(new StringArgument(Double.toString(framesize)+"KB"));
        int len = (Integer)qps.getParam(qid, vid).get(2);
        //LongArgument s = randomLongArg(1,MAX_ID - len);
        //LongArgument e = new LongArgument(s.getValue() + len);
        LongArgument s = new LongArgument(1);
        LongArgument e = new LongArgument(s.getValue() + len);
        args.add(s);args.add(e);//ds1
        args.add(s);args.add(e);//ds2
        args.add(s);args.add(e);//ds3
        args.add(s);args.add(e);//ds4
        if (qps.getParam(qid,vid).size() >= 3){
            int buildsize = (Integer)qps.getParam(qid, vid).get(3);
            IntArgument bs = new IntArgument(buildsize);
            args.add(bs);
            args.add(bs);
            args.add(bs);
        }
    }
    private void nextQ3011(int qid, int vid) {
        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(0);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));

        double framesize = 0.0;
        if (qps.getParam(qid, vid).get(1) instanceof Integer) {
            framesize = (Integer) qps.getParam(qid, vid).get(1);
        } else {
            framesize = (Double) qps.getParam(qid, vid).get(1);
        }
        args.add(new StringArgument(Double.toString(framesize)+"KB"));

        int len = (Integer)qps.getParam(qid, vid).get(2);
        int buildsize = 0;
        boolean isbuildsizeset = false;
        if (qps.getParam(qid,vid).size() >= 3) {
            buildsize = (Integer) qps.getParam(qid, vid).get(3);
            isbuildsizeset = true;
        }
        IntArgument bs = new IntArgument(buildsize);
        //LongArgument s = randomLongArg(1,MAX_ID - len);
        //LongArgument e = new LongArgument(s.getValue() + len);
        LongArgument s = new LongArgument(1);
        LongArgument e = new LongArgument(s.getValue() + len);
        if(isbuildsizeset)
            args.add(bs);
        args.add(s);args.add(e);//ds1
        args.add(s);args.add(e);//ds2
        if(isbuildsizeset)
            args.add(bs);
        args.add(s);args.add(e);//ds3
        if(isbuildsizeset)
            args.add(bs);
        args.add(s);args.add(e);//ds4
    }
    private void nextQ3019(int qid, int vid) {
        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(0);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
        int len = (Integer)qps.getParam(qid, vid).get(1);
        int buildsize = 0;
        boolean isbuildsizeset = false;
        if (qps.getParam(qid,vid).size() >= 2) {
            buildsize = (Integer) qps.getParam(qid, vid).get(2);
            isbuildsizeset = true;
        }
        IntArgument bs = new IntArgument(buildsize);
        //LongArgument s = randomLongArg(1,MAX_ID - len);
        //LongArgument e = new LongArgument(s.getValue() + len);
        LongArgument s = new LongArgument(1);
        LongArgument e = new LongArgument(s.getValue() + len);
        if(isbuildsizeset) {
            IntArgument bs3 = new IntArgument(buildsize*3);
            args.add(bs3);
        }
        args.add(s);args.add(e);//ds1
        args.add(s);args.add(e);//ds2
        if(isbuildsizeset) {
            IntArgument bs2 = new IntArgument(buildsize*2);
            args.add(bs2);
        }
        args.add(s);args.add(e);//ds3
        if(isbuildsizeset)
            args.add(bs);
        args.add(s);args.add(e);//ds4
    }
    private void nextQ3020(int qid, int vid) {
        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(0);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }
        //args.add(new StringArgument(Double.toString(minMem)+"MB"));
        int len = (Integer)qps.getParam(qid, vid).get(1);
        int buildsize = 0;
        boolean isbuildsizeset = false;
        if (qps.getParam(qid,vid).size() >= 2) {
            buildsize = (Integer) qps.getParam(qid, vid).get(2);
            isbuildsizeset = true;
        }
        IntArgument bs = new IntArgument(buildsize);
        //LongArgument s = randomLongArg(1,MAX_ID - len);
        //LongArgument e = new LongArgument(s.getValue() + len);
        LongArgument s = new LongArgument(1);
        LongArgument e = new LongArgument(s.getValue() + len);
        if(isbuildsizeset) {
            IntArgument bs3 = new IntArgument(buildsize*3);
            args.add(bs3);
        }
        IntArgument ds3mem = new IntArgument((int)minMem*3);
        args.add(ds3mem);
        args.add(s);args.add(e);//ds1
        args.add(s);args.add(e);//ds2
        if(isbuildsizeset) {
            IntArgument bs2 = new IntArgument(buildsize*2);
            args.add(bs2);
        }
        IntArgument ds2mem = new IntArgument((int)minMem*2);
        args.add(ds2mem);
        args.add(s);args.add(e);//ds3
        if(isbuildsizeset)
            args.add(bs);
        IntArgument ds1mem = new IntArgument((int)minMem);
        args.add(ds1mem);
        args.add(s);args.add(e);//ds4
    }

    private void nextQ4000(int qid, int vid) {
        int core = (Integer) qps.getParam(qid, vid).get(0);
        args.add(new StringArgument(Integer.toString(core)));

        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(1) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(1);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(1);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
        int len = (Integer)qps.getParam(qid, vid).get(2);
        //LongArgument s = randomLongArg(1,MAX_ID - len);
        //LongArgument e = new LongArgument(s.getValue() + len);
        LongArgument e = new LongArgument(len);
        args.add(e);//ds1
        args.add(e);//ds2
        args.add(e);//ds3
        args.add(e);//ds4
        if (qps.getParam(qid,vid).size() >= 3){
            int buildsize = (Integer)qps.getParam(qid, vid).get(3);
            IntArgument bs = new IntArgument(buildsize);
            args.add(bs);
            args.add(bs);
            args.add(bs);
        }
    }
    private void nextQ4100(int qid, int vid) {
        int core = (Integer) qps.getParam(qid, vid).get(0);
        args.add(new StringArgument(Integer.toString(core)));
        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(1);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(1);
        }
        int len = (Integer)qps.getParam(qid, vid).get(2);
        int buildsize = 0;
        boolean isbuildsizeset = false;
        if (qps.getParam(qid,vid).size() >= 3) {
            buildsize = (Integer) qps.getParam(qid, vid).get(3);
            isbuildsizeset = true;
        }
        IntArgument bs = new IntArgument(buildsize);
        LongArgument e = new LongArgument(len);
        args.add(e);//ds1
        args.add(e);//ds2
        if(isbuildsizeset)
            args.add(bs);
        IntArgument ds1mem = new IntArgument((int)minMem);
        args.add(ds1mem);

        args.add(e);//ds3
        if(isbuildsizeset) {
            IntArgument bs2 = new IntArgument(buildsize*2);
            args.add(bs2);
        }
        IntArgument ds2mem = new IntArgument((int)Math.ceil(minMem*2.3));
        args.add(ds2mem);

        args.add(e);//ds4
        if(isbuildsizeset) {
            IntArgument bs3 = new IntArgument(buildsize*3);
            args.add(bs3);
        }
        IntArgument ds3mem = new IntArgument((int)Math.ceil(minMem*3.3));
        args.add(ds3mem);

    }

    private void nextQ4200(int qid, int vid) {
        int core = (Integer) qps.getParam(qid, vid).get(0);
        args.add(new StringArgument(Integer.toString(core)));
        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(1);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(1);
        }
        int len = (Integer)qps.getParam(qid, vid).get(2);
        int buildsize = 0;
        boolean isbuildsizeset = false;
        if (qps.getParam(qid,vid).size() >= 3) {
            buildsize = (Integer) qps.getParam(qid, vid).get(3);
            isbuildsizeset = true;
        }
        IntArgument bs = new IntArgument(buildsize);
        LongArgument e = new LongArgument(len);
        args.add(e);//ds1
        args.add(e);//ds2
        if(isbuildsizeset)
            args.add(bs);
        IntArgument ds1mem = new IntArgument((int)minMem);
        args.add(ds1mem);
        args.add(e);//ds3
        if(isbuildsizeset)
            args.add(bs);
        args.add(ds1mem);
        args.add(e);//ds4
        if(isbuildsizeset) {
            IntArgument bs2 = new IntArgument(buildsize*3);
            args.add(bs2);
        }
        IntArgument ds2mem = new IntArgument((int)Math.ceil(minMem*3.3));
        args.add(ds2mem);
    }
    private void nextQ4300(int qid, int vid) {
        int core = (Integer) qps.getParam(qid, vid).get(0);
        args.add(new StringArgument(Integer.toString(core)));
        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(1);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(1);
        }
        int len = (Integer) qps.getParam(qid, vid).get(2);
        int buildsize = 0;
        boolean isbuildsizeset = false;
        if (qps.getParam(qid, vid).size() >= 3) {
            buildsize = (Integer) qps.getParam(qid, vid).get(3);
            isbuildsizeset = true;
        }
        IntArgument bs = new IntArgument(buildsize);
        LongArgument e = new LongArgument(len);
        args.add(e);//ds1
        args.add(e);//ds2
        if (isbuildsizeset)
            args.add(bs);
        IntArgument ds1mem = new IntArgument((int) minMem);
        args.add(ds1mem);
        args.add(e);//ds3
        args.add(e);//ds4
        if (isbuildsizeset)
            args.add(bs);
        args.add(ds1mem);
        if(isbuildsizeset) {
            IntArgument bs2 = new IntArgument(buildsize*3);
            args.add(bs2);
        }
        IntArgument ds2mem = new IntArgument((int)Math.ceil(minMem*3.3));
        args.add(ds2mem);
    }

    //Q5201,Q5401,Q5601,Q5801 are for all fit-in memory RDT for 2,4,6,8 join respectively
    private void nextQ5201(int qid, int vid) {
        int core = (Integer) qps.getParam(qid, vid).get(0);
        args.add(new StringArgument(Integer.toString(core)));

        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(1) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(1);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(1);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
        int len = (Integer)qps.getParam(qid, vid).get(2);
        LongArgument e = new LongArgument(len);
        args.add(e);//ds1
        args.add(e);//ds2
        args.add(e);//ds3
        if (qps.getParam(qid,vid).size() >= 3){
            int buildsize = (Integer)qps.getParam(qid, vid).get(3);
            IntArgument bs = new IntArgument(buildsize);
            args.add(bs);
            args.add(bs);
        }
    }
    private void nextQ5401(int qid, int vid) {
        int core = (Integer) qps.getParam(qid, vid).get(0);
        args.add(new StringArgument(Integer.toString(core)));

        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(1) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(1);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(1);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
        int len = (Integer)qps.getParam(qid, vid).get(2);
        LongArgument e = new LongArgument(len);
        args.add(e);//ds1
        args.add(e);//ds2
        args.add(e);//ds3
        args.add(e);//ds4
        args.add(e);//ds5
        if (qps.getParam(qid,vid).size() >= 3){
            int buildsize = (Integer)qps.getParam(qid, vid).get(3);
            IntArgument bs = new IntArgument(buildsize);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
        }
    }
    private void nextQ5601(int qid, int vid) {
        int core = (Integer) qps.getParam(qid, vid).get(0);
        args.add(new StringArgument(Integer.toString(core)));

        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(1) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(1);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(1);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
        int len = (Integer)qps.getParam(qid, vid).get(2);
        LongArgument e = new LongArgument(len);
        args.add(e);//ds1
        args.add(e);//ds2
        args.add(e);//ds3
        args.add(e);//ds4
        args.add(e);//ds5
        args.add(e);//ds6
        args.add(e);//ds7
        if (qps.getParam(qid,vid).size() >= 3){
            int buildsize = (Integer)qps.getParam(qid, vid).get(3);
            IntArgument bs = new IntArgument(buildsize);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
        }
    }
    private void nextQ5801(int qid, int vid) {
        int core = (Integer) qps.getParam(qid, vid).get(0);
        args.add(new StringArgument(Integer.toString(core)));

        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(1) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(1);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(1);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
        int len = (Integer)qps.getParam(qid, vid).get(2);
        LongArgument e = new LongArgument(len);
        args.add(e);//ds1
        args.add(e);//ds2
        args.add(e);//ds3
        args.add(e);//ds4
        args.add(e);//ds5
        args.add(e);//ds6
        args.add(e);//ds7
        args.add(e);//ds8
        args.add(e);//ds9
        if (qps.getParam(qid,vid).size() >= 3){
            int buildsize = (Integer)qps.getParam(qid, vid).get(3);
            IntArgument bs = new IntArgument(buildsize);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
        }
    }

    //Q5210,5410,5610,5810 are for 2,4,6,8 joins LDT unlimited memory
    private void nextQ5210(int qid, int vid) {
        int core = (Integer) qps.getParam(qid, vid).get(0);
        args.add(new StringArgument(Integer.toString(core)));
        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(1);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(1);
        }
        int len = (Integer) qps.getParam(qid, vid).get(2);
        int buildsize = 0;
        boolean isbuildsizeset = false;
        if (qps.getParam(qid, vid).size() >= 3) {
            buildsize = (Integer) qps.getParam(qid, vid).get(3);
            isbuildsizeset = true;
        }
        IntArgument bs = new IntArgument(buildsize);
        LongArgument e = new LongArgument(len);
        args.add(e);//ds1
        args.add(e);//ds2
        if (isbuildsizeset)
            args.add(bs);
        IntArgument ds1mem = new IntArgument((int) minMem);
        args.add(ds1mem);

        args.add(e);//ds3
        if (isbuildsizeset) {
            IntArgument bs2 = new IntArgument(buildsize * 2);
            args.add(bs2);
        }
        IntArgument ds2mem = new IntArgument((int) Math.ceil(minMem * 2.3));
        args.add(ds2mem);
    }
    //--OLD: Memory assginemnt to limited LDT was proportional
    private void nextQ5410(int qid, int vid) {
        int core = (Integer) qps.getParam(qid, vid).get(0);
        args.add(new StringArgument(Integer.toString(core)));
        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(1) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(1);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(1);
        }
        int len = (Integer) qps.getParam(qid, vid).get(2);
        int buildsize = 0;
        boolean isbuildsizeset = false;
        if (qps.getParam(qid, vid).size() >= 3) {
            buildsize = (Integer) qps.getParam(qid, vid).get(3);
            isbuildsizeset = true;
        }
        IntArgument bs = new IntArgument(buildsize);
        LongArgument e = new LongArgument(len);
        args.add(e);//ds1
        args.add(e);//ds2
        if (isbuildsizeset)
            args.add(bs);
        IntArgument ds1mem = new IntArgument((int) minMem);
        args.add(ds1mem);

        args.add(e);//ds3
        if (isbuildsizeset) {
            IntArgument bs2 = new IntArgument((int) Math.ceil(buildsize * 2.5));
            args.add(bs2);
        }
        IntArgument ds2mem = new IntArgument((int) Math.ceil(minMem * 2.5));
        args.add(ds2mem);

        args.add(e);//ds4
        if (isbuildsizeset) {
            IntArgument bs3 = new IntArgument((int) Math.ceil(buildsize * 3.5));
            args.add(bs3);
        }
        IntArgument ds3mem = new IntArgument((int) Math.ceil(minMem * 3.5));
        args.add(ds3mem);

        args.add(e);//ds5
        if (isbuildsizeset) {
            IntArgument bs3 = new IntArgument((int) Math.ceil(buildsize * 6));
            args.add(bs3);
        }
        IntArgument ds4mem = new IntArgument((int) Math.ceil(minMem * 6));
        args.add(ds4mem);
    }

//    private void nextQ5410(int qid, int vid) {
//        int core = (Integer) qps.getParam(qid, vid).get(0);
//        args.add(new StringArgument(Integer.toString(core)));
//        double minMem = 0.0;
//        if (qps.getParam(qid, vid).get(1) instanceof Integer) {
//            minMem = (Integer) qps.getParam(qid, vid).get(1);
//        } else {
//            minMem = (Double) qps.getParam(qid, vid).get(1);
//        }
//        int len = (Integer) qps.getParam(qid, vid).get(2);
//        int buildsize = 0;
//        boolean isbuildsizeset = false;
//        if (qps.getParam(qid, vid).size() >= 3) {
//            buildsize = (Integer) qps.getParam(qid, vid).get(3);
//            isbuildsizeset = true;
//        }
//        IntArgument bs = new IntArgument(buildsize);
//        LongArgument e = new LongArgument(len);
//        args.add(e);//ds1
//        args.add(e);//ds2
//        if (isbuildsizeset)
//            args.add(bs);
//        IntArgument ds1mem = new IntArgument((int) Math.floor(1/3*minMem));
//        args.add(ds1mem);
//
//        args.add(e);//ds3
//        if (isbuildsizeset) {
//            IntArgument bs2 = new IntArgument((int) Math.ceil(buildsize * 2.5));
//            args.add(bs2);
//        }
//        IntArgument ds2mem = new IntArgument((int) Math.floor(2/3*minMem));
//        args.add(ds2mem);
//
//        args.add(e);//ds4
//        if (isbuildsizeset) {
//            IntArgument bs3 = new IntArgument((int) Math.ceil(buildsize * 3.5));
//            args.add(bs3);
//        }
//        args.add(ds1mem);
//
//        args.add(e);//ds5
//        if (isbuildsizeset) {
//            IntArgument bs3 = new IntArgument((int) Math.ceil(buildsize * 4.5));
//            args.add(bs3);
//        }
//        args.add(ds2mem);
//    }


    private void nextQ5610(int qid, int vid) {
        int core = (Integer) qps.getParam(qid, vid).get(0);
        args.add(new StringArgument(Integer.toString(core)));
        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(1);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(1);
        }
        int len = (Integer) qps.getParam(qid, vid).get(2);
        int buildsize = 0;
        boolean isbuildsizeset = false;
        if (qps.getParam(qid, vid).size() >= 3) {
            buildsize = (Integer) qps.getParam(qid, vid).get(3);
            isbuildsizeset = true;
        }
        IntArgument bs = new IntArgument(buildsize);
        LongArgument e = new LongArgument(len);
        args.add(e);//ds1
        args.add(e);//ds2
        if (isbuildsizeset)
            args.add(bs);
        IntArgument ds1mem = new IntArgument((int) minMem);
        args.add(ds1mem);

        args.add(e);//ds3
        if (isbuildsizeset) {
            IntArgument bs2 = new IntArgument(buildsize * 2);
            args.add(bs2);
        }
        IntArgument ds2mem = new IntArgument((int) Math.ceil(minMem * 2.3));
        args.add(ds2mem);

        args.add(e);//ds4
        if (isbuildsizeset) {
            IntArgument bs3 = new IntArgument(buildsize * 3);
            args.add(bs3);
        }
        IntArgument ds3mem = new IntArgument((int) Math.ceil(minMem * 5));
        args.add(ds3mem);

        args.add(e);//ds5
        if (isbuildsizeset) {
            IntArgument bs3 = new IntArgument(buildsize * 5);
            args.add(bs3);
        }
        IntArgument ds4mem = new IntArgument((int) Math.ceil(minMem * 7));
        args.add(ds4mem);

        args.add(e);//ds6
        if (isbuildsizeset) {
            IntArgument bs3 = new IntArgument(buildsize * 5);
            args.add(bs3);
        }
        IntArgument ds5mem = new IntArgument((int) Math.ceil(minMem * 7));
        args.add(ds5mem);

        args.add(e);//ds7
        if (isbuildsizeset) {
            IntArgument bs3 = new IntArgument(buildsize * 7);
            args.add(bs3);
        }
        IntArgument ds6mem = new IntArgument((int) Math.ceil(minMem * 9));
        args.add(ds6mem);
    }
    private void nextQ5(int qid, int vid) {
        int len = (Integer) qps.getParam(qid, vid).get(0);
        LongArgument s = randomLongArg(1, MAX_ID - len);
        LongArgument e = new LongArgument(s.getValue() + len);
        args.add(s);
        args.add(e);
    }

    private void nextQ5810(int qid, int vid) {
        int core = (Integer) qps.getParam(qid, vid).get(0);
        args.add(new StringArgument(Integer.toString(core)));
        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(1);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(1);
        }
        int len = (Integer) qps.getParam(qid, vid).get(2);
        int buildsize = 0;
        boolean isbuildsizeset = false;
        if (qps.getParam(qid, vid).size() >= 3) {
            buildsize = (Integer) qps.getParam(qid, vid).get(3);
            isbuildsizeset = true;
        }
        IntArgument bs = new IntArgument(buildsize);
        LongArgument e = new LongArgument(len);
        args.add(e);//ds1
        args.add(e);//ds2
        if (isbuildsizeset)
            args.add(bs);
        IntArgument ds1mem = new IntArgument((int) minMem);
        args.add(ds1mem);

        args.add(e);//ds3
        if (isbuildsizeset) {
            IntArgument bs2 = new IntArgument(buildsize * 2);
            args.add(bs2);
        }
        IntArgument ds2mem = new IntArgument((int) Math.ceil(minMem * 2.3));
        args.add(ds2mem);

        args.add(e);//ds4
        if (isbuildsizeset) {
            IntArgument bs3 = new IntArgument(buildsize * 3);
            args.add(bs3);
        }
        IntArgument ds3mem = new IntArgument((int) Math.ceil(minMem * 5));
        args.add(ds3mem);

        args.add(e);//ds5
        if (isbuildsizeset) {
            IntArgument bs3 = new IntArgument(buildsize * 5);
            args.add(bs3);
        }
        IntArgument ds4mem = new IntArgument((int) Math.ceil(minMem * 7));
        args.add(ds4mem);

        args.add(e);//ds6
        if (isbuildsizeset) {
            IntArgument bs3 = new IntArgument(buildsize * 5);
            args.add(bs3);
        }
        IntArgument ds5mem = new IntArgument((int) Math.ceil(minMem * 7));
        args.add(ds5mem);

        args.add(e);//ds7
        if (isbuildsizeset) {
            IntArgument bs3 = new IntArgument(buildsize * 7);
            args.add(bs3);
        }
        IntArgument ds6mem = new IntArgument((int) Math.ceil(minMem * 9));
        args.add(ds6mem);

        args.add(e);//ds8
        if (isbuildsizeset) {
            IntArgument bs3 = new IntArgument(buildsize * 7);
            args.add(bs3);
        }
        IntArgument ds7mem = new IntArgument((int) Math.ceil(minMem * 9));
        args.add(ds7mem);

        args.add(e);//ds9
        if (isbuildsizeset) {
            IntArgument bs3 = new IntArgument(buildsize * 15);
            args.add(bs3);
        }
        IntArgument ds8mem = new IntArgument((int) Math.ceil(minMem * 20));
        args.add(ds8mem);
    }

    private void nextQ6100(int qid, int vid) {

        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(0);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
        int len = (Integer)qps.getParam(qid, vid).get(1);
        LongArgument e = new LongArgument(len);
        args.add(e);//ds1
        args.add(e);//ds2
        args.add(e);//ds3
        args.add(e);//ds4
        args.add(e);//ds5
        args.add(e);//ds6
        if (qps.getParam(qid,vid).size() >= 2){
            int buildsize = (Integer)qps.getParam(qid, vid).get(2);
            IntArgument bs = new IntArgument(buildsize);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
        }
    }

    private void nextQ6400(int qid, int vid) {
        int core = (Integer) qps.getParam(qid, vid).get(0);
        args.add(new StringArgument(Integer.toString(core)));
        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(1);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(1);
        }
        int len = (Integer) qps.getParam(qid, vid).get(2);
        int buildsize = 0;
        boolean isbuildsizeset = false;
        if (qps.getParam(qid, vid).size() >= 3) {
            buildsize = (Integer) qps.getParam(qid, vid).get(3);
            isbuildsizeset = true;
        }
        IntArgument bs = new IntArgument(buildsize);
        LongArgument e = new LongArgument(len);
        args.add(e);//ds1
        args.add(e);//ds2
        if (isbuildsizeset)
            args.add(bs);
        IntArgument ds1mem = new IntArgument((int) minMem);
        args.add(ds1mem);

        args.add(e);//ds3
        if (isbuildsizeset) {
            IntArgument bs2 = new IntArgument(buildsize * 2);
            args.add(bs2);
        }
        IntArgument ds2mem = new IntArgument((int) Math.ceil(minMem * 2.3));
        args.add(ds2mem);

        args.add(e);//ds4
        if (isbuildsizeset) {
            IntArgument bs3 = new IntArgument(buildsize * 3);
            args.add(bs3);
        }
        IntArgument ds3mem = new IntArgument((int) Math.ceil(minMem * 5));
        args.add(ds3mem);

        args.add(e);//ds5
        if (isbuildsizeset) {
            IntArgument bs3 = new IntArgument(buildsize * 5);
            args.add(bs3);
        }
        IntArgument ds4mem = new IntArgument((int) Math.ceil(minMem * 7));
        args.add(ds4mem);

        args.add(e);//ds6
        if (isbuildsizeset) {
            IntArgument bs3 = new IntArgument(buildsize * 5);
            args.add(bs3);
        }
        IntArgument ds5mem = new IntArgument((int) Math.ceil(minMem * 7));
        args.add(ds5mem);
    }
    private void nextQ7000(int qid, int vid) {

        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(0);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
        int len = (Integer)qps.getParam(qid, vid).get(1);
        LongArgument e = new LongArgument(len);
        args.add(e);//ds1
        args.add(e);//ds2
        args.add(e);//ds3
        args.add(e);//ds4
        args.add(e);//ds5
        args.add(e);//ds6
        args.add(e);//ds7
        args.add(e);//ds8
        args.add(e);//ds9
        args.add(e);//ds10
        args.add(e);//ds11
        if (qps.getParam(qid,vid).size() >= 2){
            int buildsize = (Integer)qps.getParam(qid, vid).get(2);
            IntArgument bs = new IntArgument(buildsize);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
        }
    }
    private void nextQ8000(int qid, int vid) {

        int len = (Integer) qps.getParam(qid, vid).get(0);
        LongArgument e = new LongArgument(len);
        args.add(e);//ds1
        args.add(e);//ds2
        args.add(e);//ds3
        args.add(e);//ds4
        args.add(e);//ds5
        args.add(e);//ds6
        args.add(e);//ds7
        args.add(e);//ds8
        args.add(e);//ds9
        args.add(e);//ds10
        args.add(e);//ds11
    }
    private void nextQ9000(int qid, int vid) {

        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(0);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }
        args.add(new StringArgument(Double.toString(minMem)+"MB"));
        int len = (Integer)qps.getParam(qid, vid).get(1);
        LongArgument e = new LongArgument(len);
        args.add(e);//ds2
        args.add(e);//ds3
        args.add(e);//ds4
        args.add(e);//ds5
        args.add(e);//ds6
        args.add(e);//ds7
        args.add(e);//ds8
        args.add(e);//ds9
        args.add(e);//ds10
        args.add(e);//ds11
        if (qps.getParam(qid,vid).size() >= 2){
            int buildsize = (Integer)qps.getParam(qid, vid).get(2);
            IntArgument bs = new IntArgument(buildsize);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
        }
    }
    private void nextQ8401(int qid, int vid) {

        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(0);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }

        int len = (Integer) qps.getParam(qid, vid).get(1);
        LongArgument s = randomLongArg(1, MAX_ID - len);
        LongArgument e = new LongArgument(s.getValue() + len);
        args.add(new StringArgument(Double.toString(minMem) + "MB"));
        args.add(s);
        args.add(e);//ds1
        args.add(s);
        args.add(e);//ds2
        args.add(s);
        args.add(e);//ds3
        args.add(s);
        args.add(e);//ds4
        args.add(s);
        args.add(e);//ds5
        if (qps.getParam(qid,vid).size() >= 2){
            int buildsize = (Integer)qps.getParam(qid, vid).get(2);
            IntArgument bs = new IntArgument(buildsize);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
        }
    }
    private void nextQ8410(int qid, int vid) {
        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(0);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }
        int len = (Integer) qps.getParam(qid, vid).get(1);
        LongArgument s = randomLongArg(1, MAX_ID - len);
        LongArgument e = new LongArgument(s.getValue() + len);
        int buildsize = 0;
        boolean isbuildsizeset = false;
        if (qps.getParam(qid, vid).size() >= 3) {
            buildsize = (Integer) qps.getParam(qid, vid).get(2);
            isbuildsizeset = true;
        }
        IntArgument bs = new IntArgument(buildsize);

        args.add(s);
        args.add(e);//ds1
        args.add(s);
        args.add(e);//ds2
        if (isbuildsizeset)
            args.add(bs);
        IntArgument ds1mem = new IntArgument((int) minMem);
        args.add(ds1mem);

        args.add(s);
        args.add(e);//ds3
        if (isbuildsizeset) {
            IntArgument bs2 = new IntArgument((int) Math.ceil(buildsize * 2.5));
            args.add(bs2);
        }
        IntArgument ds2mem = new IntArgument((int) Math.ceil(minMem * 2.5));
        args.add(ds2mem);

        args.add(s);
        args.add(e);//ds4
        if (isbuildsizeset) {
            IntArgument bs3 = new IntArgument((int) Math.ceil(buildsize * 3.5));
            args.add(bs3);
        }
        IntArgument ds3mem = new IntArgument((int) Math.ceil(minMem * 3.5));
        args.add(ds3mem);

        args.add(s);
        args.add(e);//ds5
        if (isbuildsizeset) {
            IntArgument bs3 = new IntArgument((int) Math.ceil(buildsize * 6));
            args.add(bs3);
        }
        IntArgument ds4mem = new IntArgument((int) Math.ceil(minMem * 6));
        args.add(ds4mem);
    }
    private void nextQ8411(int qid, int vid) {
        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(0);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }
        int len = (Integer) qps.getParam(qid, vid).get(1);
        LongArgument s = randomLongArg(1, MAX_ID - len);
        LongArgument e = new LongArgument(s.getValue() + len);
        int buildsize = 0;
        boolean isbuildsizeset = false;
        if (qps.getParam(qid, vid).size() >= 3) {
            buildsize = (Integer) qps.getParam(qid, vid).get(2);
            isbuildsizeset = true;
        }
        IntArgument bs = new IntArgument(buildsize);
        args.add(new StringArgument(Double.toString(minMem*6) + "MB"));
        args.add(s);
        args.add(e);//ds1
        args.add(s);
        args.add(e);//ds2
        if (isbuildsizeset)
            args.add(bs);


        args.add(s);
        args.add(e);//ds3
        if (isbuildsizeset) {
            IntArgument bs2 = new IntArgument((int) Math.ceil(buildsize * 2.5));
            args.add(bs2);
        }

        args.add(s);
        args.add(e);//ds4
        if (isbuildsizeset) {
            IntArgument bs3 = new IntArgument((int) Math.ceil(buildsize * 3.5));
            args.add(bs3);
        }

        args.add(s);
        args.add(e);//ds5
        if (isbuildsizeset) {
            IntArgument bs3 = new IntArgument((int) Math.ceil(buildsize * 6));
            args.add(bs3);
        }
    }

    private void nextQ20000(int qid, int vid){
        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(0);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }

        int len = (Integer) qps.getParam(qid, vid).get(1);
        LongArgument s = randomLongArg(1, MAX_ID - len);
        LongArgument e = new LongArgument(s.getValue() + len);
        args.add(new StringArgument(Double.toString(minMem) + "MB"));
        args.add(s);
        args.add(e);//ds1
        args.add(s);
        args.add(e);//ds2
        args.add(s);
        args.add(e);//ds3
        args.add(s);
        args.add(e);//ds4
        if (qps.getParam(qid,vid).size() >= 2){
            int buildsize = (Integer)qps.getParam(qid, vid).get(2);
            IntArgument bs = new IntArgument(buildsize);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
            args.add(bs);
        }

    }

    private void nextQ20002(int qid, int vid, int num){
        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(0);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }

        args.add(new StringArgument(Double.toString(minMem) + "MB"));
        int len = (Integer) qps.getParam(qid, vid).get(1);
        for (int i=0;i<num;i++) {
            LongArgument s = randomLongArg(1, MAX_ID - len);
            LongArgument e = new LongArgument(s.getValue() + len);
            args.add(s);
            args.add(e);//ds1
            args.add(s);
            args.add(e);//ds2
            args.add(s);
            args.add(e);//ds3
            args.add(s);
            args.add(e);//ds3
            if (qps.getParam(qid, vid).size() >= 2) {
                int buildsize = (Integer) qps.getParam(qid, vid).get(2);
                IntArgument bs = new IntArgument(buildsize);
                args.add(bs);
                args.add(bs);
                args.add(bs);
            }
        }

    }

    private void nextQ30001(int qid, int vid, int numofqueries){
        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(0);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }


        args.add(new StringArgument(Double.toString(minMem) + "MB"));
        for (int i = 0;i < numofqueries;i++) {
            int len = (Integer) qps.getParam(qid, vid).get(1);
            LongArgument s = randomLongArg(1, MAX_ID - len);
            LongArgument e = new LongArgument(s.getValue() + len);
            args.add(s);
            args.add(e);//ds1
            args.add(s);
            args.add(e);//ds2
            args.add(s);
            args.add(e);//ds3
            args.add(s);
            args.add(e);//ds4
            if (qps.getParam(qid, vid).size() >= 2) {
                int buildsize = (Integer) qps.getParam(qid, vid).get(2);
                IntArgument bs = new IntArgument(buildsize);
                args.add(bs);
                args.add(bs);
                args.add(bs);
                args.add(bs);
                args.add(bs);
                args.add(bs);
                args.add(bs);
                args.add(bs);
                args.add(bs);
                args.add(bs);
                args.add(bs);
                args.add(bs);
                args.add(bs);
                args.add(bs);
                args.add(bs);
                args.add(bs);
                args.add(bs);
                args.add(bs);
                args.add(bs);
                args.add(bs);
                args.add(bs);
                args.add(bs);
                args.add(bs);
                args.add(bs);
            }
        }

    }

    private void nextQ40001(int qid, int vid, int numOfQueries){
        double minMem = 0.0;
        if (qps.getParam(qid, vid).get(0) instanceof Integer) {
            minMem = (Integer) qps.getParam(qid, vid).get(0);
        } else {
            minMem = (Double) qps.getParam(qid, vid).get(0);
        }


        args.add(new StringArgument(Double.toString(minMem) + "MB"));
        for (int i = 0; i<numOfQueries;i++) {
            int len = (Integer) qps.getParam(qid, vid).get(1);
            LongArgument s = randomLongArg(1, MAX_ID - len);
            LongArgument e = new LongArgument(s.getValue() + len);
            args.add(s);
            args.add(e);//ds1
            args.add(s);
            args.add(e);//ds2
            args.add(s);
            args.add(e);//ds3
            args.add(s);
            args.add(e);//ds4
        }

    }


    private void nextQ3007(int qid, int vid){

    }
    //Utility Methods
    public void setStartDate(DateTimeArgument sd) {
        this.START_DATE = sd;
    }

    public void setEndDate(DateTimeArgument ed) {
        this.END_DATE = ed;
    }

    public void setMaxFbuId(long max) {
        this.MAX_ID = max;
    }

    public void setQParamSetting(QueryParamSetting qps) {
        this.qps = qps;
    }

    private LongArgument randomLongArg(long min, long max) {
        if(min<1){
            min =1;
        }
        return new LongArgument(generateRandomLong(min, max));
    }


    private long generateRandomLong(long x, long y) {
        return (x + ((long) (rand.nextDouble() * (y - x))));
    }

    private DateTimeArgument randomDateTime(DateTimeArgument minDate, DateTimeArgument maxDate) {
        int yDiff = (maxDate.year - minDate.year) + 1;
        int year = rand.nextInt(yDiff) + minDate.year;
        int month;
        int day;
        if (year == maxDate.year) {
            month = rand.nextInt(maxDate.month) + 1;
            if (month == maxDate.month) {
                day = rand.nextInt(maxDate.day) + 1;
            } else {
                day = rand.nextInt(28) + 1;
            }
        } else {
            month = rand.nextInt(12) + 1;
            day = rand.nextInt(28) + 1;
        }

        int hour = rand.nextInt(24);
        int min = rand.nextInt(58);
        int sec = rand.nextInt(58);

        return new DateTimeArgument(year, month, day, hour, min, sec);
    }

    private DateTimeArgument shift(DateTimeArgument orig, int amount) {
        Calendar c = new GregorianCalendar();
        c.setTimeInMillis(orig.convertToMillis());
        c.add(Calendar.SECOND, amount);
        return new DateTimeArgument(c);
    }

    private double[] getRandomLocation() {
        int latMajor = Constants.MIN_LAT + rand.nextInt(Constants.MAX_LAT - Constants.MIN_LAT);
        int latMinor = rand.nextInt(10000);
        double latitude = latMajor + ((double) latMinor) / 10000;

        int longMajor = Constants.MIN_LON + rand.nextInt(Constants.MAX_LON - Constants.MIN_LON);
        int longMinor = rand.nextInt(10000);
        double longitude = longMajor + ((double) longMinor) / 10000;

        return new double[] { latitude, longitude };
    }

    private DoubleArgument getRadius(int ver) {
        switch (ver) {
            case 1:
                return Constants.SMALL_RADIUS;
            case 2:
                return Constants.MEDIUM_RADIUS;
            case 3:
                return Constants.LARGE_RADIUS;
            default:
                System.err.println(
                        "Invalid query version for spatial query, query version can be 1 (short), 2 (medium) or 3 (long)");
        }
        return null;
    }

    private StringArgument getKeyword(int ver) {
        switch (ver) {
            case 1:
                return getRareKW();
            case 2:
                return getMediumKW();
            case 3:
                return getFrquentKW();
            default:
                System.err.println(
                        "Invalid query version for Text search query, query version can be 1 (rare), 2 (medium) or 3 (frequent)");
        }
        return null;
    }

    private StringArgument getFrquentKW() {
        int ix = rand.nextInt(Constants.FREQ_KW.length);
        return new StringArgument(Constants.FREQ_KW[ix]);
    }

    private StringArgument getMediumKW() {
        int ix = rand.nextInt(Constants.MEDIUM_KW.length);
        return new StringArgument(Constants.MEDIUM_KW[ix]);
    }

    private StringArgument getRareKW() {
        int ix = rand.nextInt(Constants.RARE_KW.length);
        return new StringArgument(Constants.RARE_KW[ix]);
    }
}
