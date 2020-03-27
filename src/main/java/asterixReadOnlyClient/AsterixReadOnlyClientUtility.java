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

import com.google.gson.JsonParser;
import driver.Driver;
import okhttp3.*;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import client.AbstractReadOnlyClientUtility;
import config.Constants;
import org.json.JSONObject;

public class AsterixReadOnlyClientUtility extends AbstractReadOnlyClientUtility {

    String ccUrl;
    OkHttpClient httpclient;
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
        httpclient=new OkHttpClient.Builder().readTimeout(60, TimeUnit.MINUTES).connectTimeout(5, TimeUnit.MINUTES).retryOnConnectionFailure(true).build();
    }

    @Override
    public void terminate() {
        if (resPw != null) {
            resPw.close();
        }
    }

    @Override
    public String executeQuery(int qid, int vid, String qBody) throws Exception {

       Driver.clientToRunningQueries.put(Thread.currentThread().getName(), qid+"-"+vid);
        content = null;
        StringBuilder sb =  new StringBuilder();
        Map<Object, Object> data = new HashMap<>();
//         RequestBody formBody = new FormBody.Builder().add("statement", qBody).add("mode", "immediate").add("scan_consistency","request_plus").add("profile","timings").build();
        RequestBody formBody = new FormBody.Builder().add("statement", qBody).add("mode", "immediate").add("scan_consistency","request_plus").build();
        Request request = new Request.Builder().url(getReadUrl()).addHeader("Connection","close").addHeader("User-Agent", "Bigfun").header("Authorization", basicAuth("Administrator", "pass123")).post(formBody).build();

            long s = System.currentTimeMillis();
            Timestamp startTimeStamp = new Timestamp(System.currentTimeMillis());
            try (Response response = httpclient.newCall(request).execute()){

                Driver.clientToRunningQueries.remove(Thread.currentThread().getName());
                long e = System.currentTimeMillis();
                System.out.println(request.headers());
                content = response.body().string();
                com.google.gson.JsonObject resJsObject = new JsonParser().parse(content).getAsJsonObject();
                String elapsedTime_str = resJsObject.get("metrics").getAsJsonObject().get("elapsedTime").getAsString();
                String executionTime_str = resJsObject.get("metrics").getAsJsonObject().get("executionTime").getAsString();
                double elapsedTime;
                double executionTime;
                if (elapsedTime_str.contains("ms")) {
                    elapsedTime = Double.parseDouble(elapsedTime_str.split("ms")[0]);
                } else{
                    elapsedTime = Double.parseDouble(elapsedTime_str.split("s")[0])*1000;
                }
                if (executionTime_str.contains("ms")) {
                    executionTime = Double.parseDouble(executionTime_str.split("ms")[0]);
                } else{
                    executionTime = Double.parseDouble(executionTime_str.split("s")[0])*1000;
                }


                Timestamp endTimeStamp = new Timestamp(System.currentTimeMillis());
                long rspTime = (e - s);

                sb.append("{\"qidvid\": \"Q(" + qid + "," + vid + ")\", \n" + "\"rt\":" + rspTime + ",\n");
                sb.append("\"query\":\""+qBody+"\",\n");
                sb.append("\"start\":\"" + startTimeStamp + "\",\n");
                sb.append("\"end\":\"" + endTimeStamp + "\",\n");
                sb.append("\"content\":"+content + "}\n");
                updateStat(qid, vid, rspTime, elapsedTime, executionTime);
                if (resPw != null) {
                    resPw.println(sb.toString());
                    }
                }
            return sb.toString();
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
