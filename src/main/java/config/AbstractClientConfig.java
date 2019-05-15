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

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import client.AbstractReadOnlyClient;
import client.AbstractUpdateClient;

public abstract class AbstractClientConfig {
    private String clientConfigFile;

    private List<Map<String, Object>> params;

    public AbstractClientConfig(String clientConfigFile) {
        this.clientConfigFile = clientConfigFile;
        this.params = new LinkedList<>();
    }

    public abstract AbstractReadOnlyClient readReadOnlyClientConfig(String bigFunHomePath,int cid);

    public abstract AbstractUpdateClient readUpdateClientConfig(String bigFunHomePath,int cid);

    public void parseConfigFile() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            params = mapper.readValue(new File(clientConfigFile), new TypeReference<List<Map<String,Object>>>(){});
        } catch (Exception e) {
            System.err.println("Problem in parsing the JSON config file.");
            e.printStackTrace();
        }

    }

    public boolean isParamSet(String paramName, int userId) {
        return params.get(userId).containsKey(paramName);
    }

    public Object getParamValue(String paramName,int userId) {
        return params.get(userId).get(paramName);
    }

    /**
     * Added for debug/trace purposes only
     * 
     * @return List all params and their values in the current config
     */
    public String printConfig() {
        StringBuilder sb = new StringBuilder();
        int i=0;
        for(Map<String,Object> map: params) {
            for (String s : map.keySet()) {
                sb.append(s).append(":\t").append(getParamValue(s,i).toString()).append("\n");
            }
            i++;
        }
        return sb.toString();
    }
    public List<Map<String, Object>> getParams() {
        return params;
    }
}
