/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.websocket;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DPushSource;
import com.streamsets.pipeline.lib.websocket.Groups;

/*@StageDef(
    version = 2,
    label = "WebSocket Client",
    description = "Uses a WebSocket client to read from a resource URL",
    icon = "websockets.png",
    execution = {ExecutionMode.STANDALONE},
    recordsByRef = true,
    onlineHelpRefUrl ="index.html#datacollector/UserGuide/Origins/WebSocketClient.html#task_u4n_rzk_fbb",
    upgrader = WebSocketClientSourceUpgrader.class
)*/
@HideConfigs({
    "conf.dataFormatConfig.jsonContent",
    "conf.tlsConfig.keyStoreFilePath",
    "conf.tlsConfig.keyStoreType",
    "conf.tlsConfig.keyStorePassword",
    "conf.tlsConfig.keyStoreAlgorithm"
})
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class WebSocketClientDSource extends DPushSource {

  @ConfigDefBean
  public WebSocketClientSourceConfigBean conf;


  @Override
  protected PushSource createPushSource() {
    return new WebSocketClientSource(conf);
  }
}
