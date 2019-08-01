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
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import client.AbstractReadOnlyClientUtility;
import config.Constants;

public class AsterixReadOnlyClientUtility extends AbstractReadOnlyClientUtility {

    String ccUrl;
    String apiPort;
    String apiPath;
    DefaultHttpClient httpclient;
    ArrayList<NameValuePair> httpPostParams;
    HttpPost httpPost;
    URIBuilder roBuilder;
    String content;

    public AsterixReadOnlyClientUtility(String cc, String qIxFile, String qGenConfigFile, String statsFile, int ignore,
            String qSeqFile, String resultsFile) throws IOException {
        super(qIxFile, qGenConfigFile, statsFile, ignore, qSeqFile, resultsFile);
        this.ccUrl = cc;
        this.apiPort="19002";
        this.apiPath="/query/service";
    }

    @Override
    public void init() {
        httpclient = new DefaultHttpClient();
        httpPost = new HttpPost();
        httpPostParams = new ArrayList<>();
        try {
            roBuilder = new URIBuilder("http://" + ccUrl + ":" + apiPort + apiPath);
            System.out.println(roBuilder.toString());
        } catch (URISyntaxException e) {
            System.err.println("Problem in initializing Read-Only URI Builder");
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
    public void executeQuery(int qid, int vid, String qBody) {

        content = null;
        long rspTime = -1;
        try {
            URI uri = roBuilder.build();
            httpPost.setURI(uri);
            httpPostParams.clear();
            httpPostParams.add(new BasicNameValuePair("statement", qBody));
            httpPostParams.add(new BasicNameValuePair("mode", "immediate"));
            httpPost.setEntity(new UrlEncodedFormEntity(httpPostParams));

            long s = System.currentTimeMillis();
            Timestamp startTimeStamp = new Timestamp(System.currentTimeMillis());
            HttpResponse response = httpclient.execute(httpPost);
            long e = System.currentTimeMillis();
            HttpEntity entity = response.getEntity();
            content = EntityUtils.toString(entity);
            Timestamp endTimeStamp = new Timestamp(System.currentTimeMillis());
            rspTime = (e - s);
            System.out.println("{\"qidvid\": \"Q("+qid+","+vid+")\", \n" + "\"rt\":"+rspTime+","); //trace the
            // progress
            System.out.println("\"start\":\"" +startTimeStamp +"\",");
            System.out.println("\"end\":\""+endTimeStamp+"\"\n}");
        }  catch (Exception ex) {
                    System.err.println("Problem in read-only query execution against Asterix");
                    ex.printStackTrace();
                    updateStat(qid, vid, Constants.INVALID_TIME);
                    return;
                }
                updateStat(qid, vid, rspTime);
                if (resPw != null) {
                    resPw.println(qid);
                    resPw.println("Ver " + vid);
                    resPw.println(qBody + "\n");
                    resPw.println("responseTime : "+rspTime +" Msec");
                    if (dumpResults) {
                        resPw.println(content + "\n");
                    }
                }

    }
}
