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
package com.streamsets.datacollector.execution.preview.common;

import com.streamsets.datacollector.execution.PreviewOutput;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.validation.Issues;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class PreviewOutputImpl implements PreviewOutput {

    private final PreviewStatus previewStatus;
    private final Issues issues;
    private final List<List<StageOutput>> output;
    private final String message;

    private final static String JDBC_LIB_EDITMAPPING_ERROR_PREFIX = "EDITMAPPING_ERROR:";

    public PreviewOutputImpl(PreviewStatus previewStatus, Issues issues, List<List<StageOutput>> output, String message) {
        this.previewStatus = previewStatus;
        this.issues = issues;
        this.output = output;
        this.message = this.handleMessage(message);
    }

    @Override
    public PreviewStatus getStatus() {
        return previewStatus;
    }

    @Override
    public Issues getIssues() {
        return issues;
    }

    @Override
    public List<List<StageOutput>> getOutput() {
        return output;
    }

    @Override
    public String getMessage() {
        return message;
    }

    private String handleMessage(String message) {
        String retMessage = "";
        if (StringUtils.isNotBlank(message)) {
            retMessage = message.replace("com.streamsets.", "");
            if (retMessage.contains(JDBC_LIB_EDITMAPPING_ERROR_PREFIX)) {
                retMessage = retMessage.substring(retMessage.indexOf(JDBC_LIB_EDITMAPPING_ERROR_PREFIX, 0))
                        .replace(JDBC_LIB_EDITMAPPING_ERROR_PREFIX, "");
            }
        }
        return retMessage;
    }
}
