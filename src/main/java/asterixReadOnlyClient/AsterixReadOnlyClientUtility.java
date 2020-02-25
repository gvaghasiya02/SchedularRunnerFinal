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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;

import driver.Driver;
import org.apache.http.HttpHeaders;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.params.AuthPolicy;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import client.AbstractReadOnlyClientUtility;
import config.Constants;

public class AsterixReadOnlyClientUtility extends AbstractReadOnlyClientUtility {

    String ccUrl;
    DefaultHttpClient httpclient;
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

        httpclient = new DefaultHttpClient();
        UsernamePasswordCredentials usernamepass = new UsernamePasswordCredentials("Administrator", "pass123");
        httpclient.getCredentialsProvider().setCredentials(AuthScope.ANY, usernamepass);

        httpPost = new HttpPost();
        httpPostParams = new ArrayList<>();
        try {
            roBuilder = new URIBuilder(getReadUrl());
            roBuilder.setUserInfo("Administrator", "pass123");
            System.out.println(roBuilder.toString());
        } catch (URISyntaxException e) {
            System.err.println("Problem in initializing Read-Only URI Builder");
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void terminate() {
        if (resPw != null) {
            resPw.close();
        }
        httpclient.getConnectionManager().shutdown();
    }

    @Override
    public void executeQuery(int qid, int vid, String qBody) throws Exception {

       Driver.clientToRunningQueries.put(Thread.currentThread().getName(), qid+"-"+vid);
        content = null;
            URI uri = roBuilder.build();
            httpPost.setURI(uri);
            httpPostParams.clear();
            httpPostParams.add(new BasicNameValuePair("statement", qBody));
            httpPostParams.add(new BasicNameValuePair("mode", "immediate"));
            httpPost.setEntity(new UrlEncodedFormEntity(httpPostParams));



            long s = System.currentTimeMillis();
            Timestamp startTimeStamp = new Timestamp(System.currentTimeMillis());
            HttpResponse response = httpclient.execute(httpPost);

           Driver.clientToRunningQueries.remove(Thread.currentThread().getName());
            long e = System.currentTimeMillis();
            HttpEntity entity = response.getEntity();
            content = EntityUtils.toString(entity);
            Timestamp endTimeStamp = new Timestamp(System.currentTimeMillis());
            long rspTime = (e - s);
            System.out.println("{\"qidvid\": \"Q("+qid+","+vid+")\", \n" + "\"rt\":"+rspTime+","); //trace the
            // progress
            System.out.println("\"start\":\"" +startTimeStamp +"\",");
            System.out.println("\"end\":\""+endTimeStamp+"\"\n}");
                updateStat(qid, vid, rspTime);
                if (resPw != null) {
                    resPw.println(qid);
                    resPw.println("Ver " + vid);
                    resPw.println(qBody + "\n");
                    resPw.println("responseTime : " + rspTime + " Msec");
                    if (dumpResults) {
                        resPw.println(content + "\n");
                    }
                }

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
