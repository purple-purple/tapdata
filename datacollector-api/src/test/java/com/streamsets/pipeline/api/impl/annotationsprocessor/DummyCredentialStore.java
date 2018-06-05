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
package com.streamsets.pipeline.api.impl.annotationsprocessor;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialStoreDef;
import com.streamsets.pipeline.api.credential.CredentialValue;

import java.util.List;

@CredentialStoreDef(
    label = "dummy"
)
public class DummyCredentialStore implements CredentialStore {

  @Override
  public List<ConfigIssue> init(Context context) {
    return null;
  }

  @Override
  public CredentialValue get(String group, String name, String options) throws StageException {
    return null;
  }

  @Override
  public void destroy() {
  }

}
