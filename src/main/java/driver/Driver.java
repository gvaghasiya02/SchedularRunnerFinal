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

import Prometheus.BigFunCollector;
import client.AbstractClient;
import config.AbstractClientConfig;
import config.AsterixClientConfig;
import config.Constants;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class Driver {
    private static Map<String, String> env = System.getenv();
    public static String BIGFUN_HOME = env.get("BIGFUN_HOME");
    public static String workloadsFolder = BIGFUN_HOME +"/workloads/";
    public static HashMap<String,String> clientToRunningQueries = new HashMap<>();

    public static void main(String[] args) throws IOException {
        //Prometheus Setup
        CollectorRegistry registery = new CollectorRegistry();
        registery.register(new BigFunCollector());
        HTTPServer server = new HTTPServer(new InetSocketAddress(2020), registery);

         String clientConfigFile = BIGFUN_HOME + "/conf/bigfun-conf.json";

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
                        client = clientConfig.readReadOnlyClientConfig(BIGFUN_HOME, c);
                        break;
                    case Constants.ASTX_UPDATE_CLIENT_TAG:
                        client = clientConfig.readUpdateClientConfig(BIGFUN_HOME, c);
                        break;

                    default:
                        System.err.println("Unknown/Invalid client type:\t" + clientTypeTag);
                }
                client.bigFunHome = BIGFUN_HOME;
                try {
                    client.execute();
                } catch (Exception e) {
                    e.printStackTrace();
                    server.stop();
                    executorService.shutdownNow();
                }
                //Stats Report
                client.generateReport();
            }));
        try {
            server.stop();
            executorService.shutdown();
            executorService.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS); // TODO: Is this necessary?

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            if (!executorService.isTerminated()) {
                System.out.println("canceling all pending tasks");
            }
            executorService.shutdownNow();
        }
    }
    private static Map<String, Object> processCommandLineConfig(String[] args) {
        Map<String, Object> commandLineConfig = new HashMap<>();
        if (args != null) {
            for (String arg: args) {
                if (arg.contains("=")) {
                    commandLineConfig.put(arg.substring(0, arg.indexOf("=")).toLowerCase(),
                            arg.substring(arg.indexOf("=")+1));
                }
            }
        }
        return commandLineConfig;
    }
}
