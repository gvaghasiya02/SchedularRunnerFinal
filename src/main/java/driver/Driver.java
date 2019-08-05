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
package driver;

import client.AbstractClient;
import config.AbstractClientConfig;
import config.AsterixClientConfig;
import config.Constants;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class Driver {
public static String bigFunHome = "";
public static String workload = "";
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Correct Usage:\n");
            System.out.println("\t[0]: BigFUN home has to be set to a valid path.");
            return;
        }

        bigFunHome = args[0].replaceAll("/$", "");
        workload = args[1].replaceAll("/$", "");

         String clientConfigFile = bigFunHome + "/conf/bigfun-conf.json";
        AbstractClientConfig clientConfig = new AsterixClientConfig(clientConfigFile);
        clientConfig.parseConfigFile();
        ExecutorService executorService = Executors.newFixedThreadPool(clientConfig.getParams().size());
            IntStream.range(0,clientConfig.getParams().size()).forEach(c ->
            executorService.submit(()-> {
                Thread.currentThread().setName("Client"+c);
                if (!clientConfig.isParamSet(Constants.CLIENT_TYPE, c)) {
                    System.err.println("The Client Type is not set to a valid value in the config file.");
                    return;
                }
                String clientTypeTag = (String) clientConfig.getParamValue(Constants.CLIENT_TYPE, c);
                AbstractClient client = null;
                switch (clientTypeTag) {
                    case Constants.ASTX_RANDOM_CLIENT_TAG:
                        client = clientConfig.readReadOnlyClientConfig(bigFunHome, c);
                        break;
                    case Constants.ASTX_UPDATE_CLIENT_TAG:
                        client = clientConfig.readUpdateClientConfig(bigFunHome, c);
                        break;

                    default:
                        System.err.println("Unknown/Invalid client type:\t" + clientTypeTag);
                }
                client.bigFunHome = bigFunHome;
                client.execute();
                client.generateReport();
            }));
        try {
            executorService.shutdown();
            executorService.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS); // TODO: Is this necessary?

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            if (!executorService.isTerminated()) {
                System.out.println("canceling all pending tasks");
            }
            //System.out.println("Finished at: " + System.currentTimeMillis());
            executorService.shutdownNow();
        }

        //System.out.println("\nBigFUN Benchmark is done.\n");
    }
}
