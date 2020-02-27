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
package asterixReadOnlyClient;

import client.AbstractReadOnlyClient;
import structure.Pair;
import structure.Query;
import workloadGenerator.ReadOnlyWorkloadGenerator;

import java.io.IOException;

public class AsterixClientReadOnlyWorkload extends AbstractReadOnlyClient {

    String ccUrl;
    String dvName;
    int iterations;
    ReadOnlyWorkloadGenerator rwg;
    public AsterixClientReadOnlyWorkload() {};

    public AsterixClientReadOnlyWorkload(String cc, String dvName, int iter, String qGenConfigFile, String qIxFile,
            String statsFile, int ignore, String qSeqFile, String resDumpFile, long seed, long minUserId,long maxUsrId, String server) {
        super();
        this.ccUrl = cc;
        this.dvName = dvName;
        this.iterations = iter;
        setClientUtil(qIxFile, qGenConfigFile, statsFile, ignore, qSeqFile, resDumpFile, server);
        clUtil.init();
        initReadOnlyWorkloadGen(seed,minUserId, maxUsrId);
        execQuery = true;
    }

    @Override
    protected void initReadOnlyWorkloadGen(long seed, long minUserId,long maxUsrId) {
        this.rwg = new ReadOnlyWorkloadGenerator(clUtil.getQIxFile(), clUtil.getQGenConfigFile(), seed, minUserId,maxUsrId);
    }

    @Override
    public void execute() throws Exception {
        long starttime=System.currentTimeMillis();
        System.out.println("started at : " +starttime);
        long iteration_start;
        long iteration_end;
        int iterationCount = 0;
        System.out.print("[");
        for (int i = 0; i < iterations; i++) {
            if (iterationCount > 0) {
                System.out.println(",");
            }
            System.out.println("\n{ \"Iteration\":" + i+
                    ",");
            iteration_start = System.currentTimeMillis();
            System.out.println("\"queries\":[");
            int loopCount = 0;
            for (Pair qvPair : clUtil.qvids) {
                if (loopCount > 0) {
                    System.out.print(",");
                }
                int qid = qvPair.getQId();
                int vid = qvPair.getVId();
                Query q = rwg.nextQuery(qid, vid);
                if (q == null) {
                    continue; //do not break, if one query is not found
                }
                if (execQuery) {
                    clUtil.executeQuery(qid, vid, q.aqlPrint(dvName));
                }
                loopCount++;
            }
            iteration_end = System.currentTimeMillis();
            System.out.print("],\n\"TotalTime " + i + "\" :" + (iteration_end - iteration_start) + "\n}");
            iterationCount++;
        }
        System.out.print("]");
        long endtime= System.currentTimeMillis();
        System.out.println("finished at : " +endtime);
        System.out.println("Total experiment execution time : " +(endtime-starttime)+" (ms)");
        System.out.println("Throughput for user 0: " +(long)(1*1.0/(endtime-starttime)*1.0));
        clUtil.terminate();
    }

    @Override
    public void setClientUtil(String qIxFile, String qGenConfigFile, String statsFile, int ignore, String qSeqFile,
            String resultsFile, String server) {
        try {
            clUtil = new AsterixReadOnlyClientUtility(ccUrl, qIxFile, qGenConfigFile, statsFile, ignore, qSeqFile,
                    resultsFile, server);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
