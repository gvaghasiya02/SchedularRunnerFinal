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

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import driver.Driver;
import okhttp3.*;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import client.AbstractReadOnlyClientUtility;
import config.Constants;

public class AsterixReadOnlyClientUtility extends AbstractReadOnlyClientUtility {

    String ccUrl;
    OkHttpClient httpclient;
    ArrayList<NameValuePair> httpPostParams;
    HttpPost httpPost;
    URIBuilder roBuilder;
    String content;
    String server;

    public AsterixReadOnlyClientUtility(String cc, String qIxFile, String qGenConfigFile, String statsFile, int ignore,
            String qSeqFile, String resultsFile, String server) throws IOException {
        super(qIxFile, qGenConfigFile, statsFile, ignore, qSeqFile, resultsFile);
        this.ccUrl = cc;
        this.server = server;
    }

    @Override
    public void init() {
        httpclient=new OkHttpClient.Builder().readTimeout(1, TimeUnit.MINUTES).retryOnConnectionFailure(true).build();
    }

    @Override
    public void terminate() {
        if (resPw != null) {
            resPw.close();
        }
    }

    @Override
    public void executeQuery(int qid, int vid, String qBody) throws Exception {

       Driver.clientToRunningQueries.put(Thread.currentThread().getName(), qid+"-"+vid);
        content = null;

        Map<Object, Object> data = new HashMap<>();
        data.put("\"statement\"", qBody);
        data.put("mode", "immediate");
        //HttpRequest request = HttpRequest.newBuilder().uri(URI.create(getReadUrl())).setHeader("User-Agent", "Bigfun").header("Authorization", basicAuth("Administrator", "pass123")).POST(HttpRequest.BodyPublishers.ofString("statement=select 1;")).build();
        RequestBody formBody = new FormBody.Builder().add("statement", qBody).add("mode", "immediate").build();
        Request request = new Request.Builder().url(getReadUrl()).addHeader("Connection","close").addHeader("User-Agent", "Bigfun").header("Authorization", basicAuth("Administrator", "pass123")).post(formBody).build();

            long s = System.currentTimeMillis();
            Timestamp startTimeStamp = new Timestamp(System.currentTimeMillis());
            try (Response response = httpclient.newCall(request).execute()){

                Driver.clientToRunningQueries.remove(Thread.currentThread().getName());
                long e = System.currentTimeMillis();
                content = response.body().string();
                Timestamp endTimeStamp = new Timestamp(System.currentTimeMillis());
                long rspTime = (e - s);
                System.out.println("{\"qidvid\": \"Q(" + qid + "," + vid + ")\", \n" + "\"rt\":" + rspTime + ","); //trace the
                // progress
                System.out.println("\"start\":\"" + startTimeStamp + "\",");
                System.out.println("\"end\":\"" + endTimeStamp + "\"\n}");
                updateStat(qid, vid, rspTime);
                if (resPw != null) {
                    resPw.println(qid);
                    resPw.println("Ver " + vid);
                    resPw.println(qBody + "\n");
                    resPw.println("responseTime : " + rspTime + " Msec");
                    System.out.println(content+"\n");
                    if (dumpResults) {
                        resPw.println(content + "\n");
                    }
                }
            }
    }


    private static String basicAuth(String username, String password) {
        return "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
    }

    private String getReadUrl() throws Exception {
        if (server.equalsIgnoreCase(Constants.ASTX_SERVER_TAG)) {
            return ("http://" + ccUrl + ":" + Constants.ASTX_AQL_REST_API_PORT + Constants.ASTX_READ_API_URL);
        } else if (server.equalsIgnoreCase(Constants.CB_SERVER_TAG)) {
            return ("http://" + ccUrl + ":" + Constants.CB_REST_API_PORT + Constants.CB_ANALYTICS_API_URL);
        } else {
            throw new Exception("Unknown server type.");
        }
    }
}
