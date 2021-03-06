/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.core.util.collection;

import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;

/**
 * Created by suho on 5/21/16.
 */
public class UpdateAttributeMapper {
    private final int updatingAttributePosition;
    private final int candidateAttributePosition;
    private int matchingStreamEventPosition;

    public UpdateAttributeMapper(int updatingAttributePosition, int candidateAttributePosition, int matchingStreamEventPosition) {

        this.updatingAttributePosition = updatingAttributePosition;
        this.candidateAttributePosition = candidateAttributePosition;
        this.matchingStreamEventPosition = matchingStreamEventPosition;
    }

    public int getCandidateAttributePosition() {
        return candidateAttributePosition;
    }

    public Object getOutputData(StateEvent updatingEvent) {
        return updatingEvent.getStreamEvent(matchingStreamEventPosition).getOutputData()[updatingAttributePosition];
    }

    public StreamEvent getOverwritingStreamEvent(StateEvent updatingEvent) {
        return updatingEvent.getStreamEvent(matchingStreamEventPosition);
    }
}

