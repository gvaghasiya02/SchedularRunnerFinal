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
package config;

import asterixReadOnlyClient.AsterixClientReadOnlyWorkload;
import asterixReadOnlyClient.AsterixConcurrentReadOnlyWorkload;
import asterixUpdateClient.AsterixClientUpdateWorkload;
import client.AbstractReadOnlyClient;
import client.AbstractUpdateClient;
import driver.Driver;
import structure.UpdateTag;

import java.nio.file.Path;
import java.nio.file.Paths;

public class AsterixClientConfig extends AbstractClientConfig {

    public AsterixClientConfig(String clientConfigFile) {
        super(clientConfigFile);
    }

    public AbstractReadOnlyClient readReadOnlyClientConfig(String bigFunHomePath,int cid) {
        String cc = (String) getParamValue(Constants.CC_URL,cid);
        String dvName = (String) getParamValue(Constants.ASTX_DV_NAME,cid);
        int iter = (int) getParamValue(Constants.ITERATIONS,cid);

        String qIxFile = bigFunHomePath + "/files/" + Constants.Q_IX_FILE_NAME;
        String qGenConfigFile = bigFunHomePath + "/files/" + Constants.Q_GEN_CONFIG_FILE_NAME;
//        String workloadFile = bigFunHomePath + "/files/workloads/" + Constants.WORKLOAD_FILE_NAME;

        String statsFile = bigFunHomePath + "/files/output/" + Constants.STATS_FILE_NAME;
        if (isParamSet(Constants.STATS_FILE,cid)) {
            statsFile = (String) getParamValue(Constants.STATS_FILE,cid);
        }

        long seed = Constants.DEFAULT_SEED;
        if (isParamSet(Constants.SEED,cid)) {
            Object value = getParamValue(Constants.SEED,cid);
            if (value instanceof Long) {
                seed = (long) value;
            } else if (value instanceof Integer) {
                seed = ((Integer) value).longValue();
            } else {
                System.err.println("WARNING: Invalid Seed value in " + Constants.BIGFUN_CONFIG_FILE_NAME
                        + " . Using default seed value for the generator.");
            }

        }

        long maxId = Constants.DEFAULT_MAX_ID;
        if (isParamSet(Constants.MAX_ID,cid)) {
            Object value = getParamValue(Constants.MAX_ID,cid);
            if (value instanceof Long) {
                maxId = (long) value;
            } else if (value instanceof Integer) {
                maxId = ((Integer) value).longValue();
            } else {
                System.err.println("WARNING: Invalid " + Constants.MAX_ID + " value in "
                        + Constants.BIGFUN_CONFIG_FILE_NAME + " . Using the default value for the generator.");
            }
        }

        long minId = Constants.DEFAULT_MIN_ID;
        if (isParamSet(Constants.MIN_ID,cid)) {
            Object value = getParamValue(Constants.MIN_ID,cid);
            if (value instanceof Long) {
                minId = (long) value;
            } else if (value instanceof Integer) {
                minId = ((Integer) value).longValue();
            } else {
                System.err.println("WARNING: Invalid " + Constants.MIN_ID + " value in "
                        + Constants.BIGFUN_CONFIG_FILE_NAME + " . Using the default value for the generator.");
            }
        }

        int ignore = -1;
        if (isParamSet(Constants.IGNORE,cid)) {
            ignore = (int) getParamValue(Constants.IGNORE,cid);
        }
        String workloadFile= Driver.workload;
//        if(isParamSet(Constants.WORKLOAD, cid)) {
//            final Path wlPath=Paths.get(bigFunHomePath , "/files/workloads/",
//                    getParamValue(Constants.WORKLOAD,cid).toString());
//            workloadFile = wlPath.toString();
//        }

        boolean qExec = true;
        if (isParamSet(Constants.EXECUTE_QUERY,cid)) {
            qExec = (boolean) getParamValue(Constants.EXECUTE_QUERY,cid);
        }

        boolean dumpResults = false;
        String[] splits = Driver.workload.split("/");
        String wl = splits[splits.length -1];
        String resultsFile = "/tmp/resdump_"+wl;
//        if (isParamSet(Constants.ASTX_DUMP_RESULTS,cid)) {
//            dumpResults = (boolean) getParamValue(Constants.ASTX_DUMP_RESULTS,cid);
//            resultsFile = (String) getParamValue(Constants.RESULTS_DUMP_FILE,cid);
//        }
        int numReaders = 1;
        if (isParamSet(Constants.NUM_CONCURRENT_READERS,cid)) {
            numReaders = (int) getParamValue(Constants.NUM_CONCURRENT_READERS,cid);
        }
        AsterixClientReadOnlyWorkload rClient;
        if (numReaders == 1) {
            rClient = getAsterixClientReadOnlyWorkload(cc, dvName, iter, qIxFile, qGenConfigFile, workloadFile, statsFile, seed, maxId, ignore, resultsFile);
        }
        else {
           rClient = new AsterixConcurrentReadOnlyWorkload(cc, dvName, iter, qGenConfigFile,
                qIxFile, statsFile, ignore, workloadFile, /*dumpDirFile,*/ resultsFile, seed,minId, maxId, numReaders);
              }
//    rClient = new AsterixClientReadOnlyWorkload(cc, dvName, iter, qGenConfigFile,
//                qIxFile, statsFile, ignore, workloadFile, /*dumpDirFile,*/ resultsFile, seed, maxUserId);

        rClient.setExecQuery(qExec);
       // rClient.setDumpResults(dumpResults);
        return rClient;
    }
    private AsterixClientReadOnlyWorkload getAsterixClientReadOnlyWorkload(String cc, String dvName, int iter, String qIxFile, String qGenConfigFile, String workloadFile, String statsFile, long seed, long maxUserId, int ignore, String resultsFile) {
        return new AsterixClientReadOnlyWorkload(cc, dvName, iter, qGenConfigFile,
                qIxFile, statsFile, ignore, workloadFile, /*dumpDirFile,*/ resultsFile, seed, maxUserId,maxUserId);
    }
    @Override
    public AbstractUpdateClient readUpdateClientConfig(String bigFunHomePath,int cid) {
        String cc = (String) getParamValue(Constants.CC_URL,cid);
        String oprType = (String) getParamValue(Constants.UPDATE_OPR_TYPE_TAG,cid);

        String updatesFile = (String) getParamValue(Constants.UPDATES_FILE,cid);
        String statsFile = bigFunHomePath + "/files/output/" + Constants.STATS_FILE_NAME;
        if (isParamSet(Constants.STATS_FILE,cid)) {
            statsFile = (String) getParamValue(Constants.STATS_FILE,cid);
        }

        String dvName = (String) getParamValue(Constants.ASTX_DV_NAME,cid);
        String dsName = (String) getParamValue(Constants.ASTX_DS_NAME,cid);
        String keyName = (String) getParamValue(Constants.ASTX_KEY_NAME,cid);
        int batchSize = (int) getParamValue(Constants.UPDATE_BATCH_SIZE,cid);

        int limit = -1;
        if (isParamSet(Constants.UPDATE_LIMIT,cid)) {
            limit = (int) getParamValue(Constants.UPDATE_LIMIT,cid);
        }

        int ignore = -1;
        if (isParamSet(Constants.IGNORE,cid)) {
            ignore = (int) getParamValue(Constants.IGNORE,cid);
        }

        UpdateTag upTag = null;
        if (oprType.equals(Constants.INSERT_OPR_TYPE)) {
            upTag = UpdateTag.INSERT;
        } else if (oprType.equals(Constants.DELETE_OPR_TYPE)) {
            upTag = UpdateTag.DELETE;
        } else {
            System.err.println("Unknown Data Manipulation Operation for AsterixDB - " + oprType);
            System.err.println("You can only run " + Constants.INSERT_OPR_TYPE + " and " + Constants.DELETE_OPR_TYPE
                    + " against AsterixDB");
            return null;
        }

        return new AsterixClientUpdateWorkload(cc, dvName, dsName, keyName, upTag, batchSize, limit, updatesFile,
                statsFile, ignore);
    }
}
