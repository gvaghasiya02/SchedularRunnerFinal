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
//            case 1200:
//            case 1201:
//            case 1202:
//                nextJoinMemory(qIx, vIx);
//            case 0:
//                nextQ0(qIx, vIx);
//                break;
//            case 1:
//            case 11:
//            case 1000:
//            case 1025:
//            case 1005:
//            case 1075:
//            case 1100:
//                nextAllJoinsParam(qIx,vIx,1,1);
//                break;
//            case 2:
//            case 12:
//                nextAllJoinsParam(qIx,vIx,2,1);
//                break;
//            case 3:
//            case 13:
//                nextAllJoinsParam(qIx,vIx,3,1);
//                break;
//            case 4:
//            case 14:
//                nextAllJoinsParam(qIx,vIx,4,1);
//                break;
//            case 5:
//            case 15:
//                nextAllJoinsParam(qIx,vIx,5,1);
//                break;
//            case 6:
//            case 16:
//                nextAllJoinsParam(qIx,vIx,6,1);
//                break;
//            case 7:
//            case 17:
//                nextAllJoinsParam(qIx,vIx,7,1);
//                break;
//            case 8:
//            case 18:
//            case 301:
//            case 302:
//            case 303:
//            case 304:
//            case 305:
//            case 306:
//            case 307:
//                nextAllJoinsParam(qIx,vIx,8,1);
//                break;
//            case 280://Hybrid RDT-8 joins
//                nextAllJoinsParam(qIx,vIx,8,8);
//                break;
//            case 281://LDT 8 joins
//                nextAllJoinsParam(qIx,vIx,8,2);
//                break;
        case 1:
            nextQ1(qIx, vIx);
            break;
            default:
                next(qIx,vIx);
                break;
        }
        return (new ArrayList<IArgument>(args));
    }


    private void nextQ1(int qid, int vid) {
     //nextJoinMemory(qid, vid);

    }
    private void next(int qid, int vid){
        ArrayList<Number> p = qps.getParam(qid,vid);
        for(Number element:p){
            args.add(new LongArgument((Integer)element));
        }
    }

    private void nextAllJoinsParam(int qid, int vid, int countOfArguments, int memoryDivision) {
        int numPartitions = (Integer) qps.getParam(qid, vid).get(1);
        IntArgument np = new IntArgument(numPartitions);
        int buildSize = (Integer) qps.getParam(qid, vid).get(2);
        IntArgument bs = new IntArgument(buildSize);
        //double buildTomemRatio  = qps.getParam(qid, vid).get(0).doubleValue();
        // int joinMem = (int)Math.round(buildSize / buildTomemRatio);
        int joinMem = (Integer) qps.getParam(qid, vid).get(0);
        IntArgument jm = new IntArgument(joinMem / memoryDivision);
        //first = second
        for (int i =0; i < countOfArguments; i++) {
            args.add(jm);
            args.add(np);
            args.add(bs);
        }
    }

    private void nextJoinMemory(int qid, int vid) {
        int joinMem = (Integer) qps.getParam(qid, vid).get(0);
        IntArgument jm = new IntArgument(joinMem);
        args.add(jm);
    }

    private void nextQ0(int qid, int vid){

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

}
