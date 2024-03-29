/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This wrapper supports both v0 and v1 of OffsetFetchRequest.
 */
public class OffsetFetchRequest extends AbstractRequest {
    
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.OFFSET_FETCH.id);
    private static final String GROUP_ID_KEY_NAME = "group_id";
    private static final String TOPICS_KEY_NAME = "topics";

    // topic level field names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    // partition level field names
    private static final String PARTITION_KEY_NAME = "partition";

    /**
     * 所属 groupId
     */
    private final String groupId;
    /**
     * 具体哪些 分区的消费进度
     */
    private final List<TopicPartition> partitions;

    public OffsetFetchRequest(String groupId, List<TopicPartition> partitions) {
        super(new Struct(CURRENT_SCHEMA));

        Map<String, List<Integer>> topicsData = CollectionUtils.groupDataByTopic(partitions);

        struct.set(GROUP_ID_KEY_NAME, groupId);
        List<Struct> topicArray = new ArrayList<Struct>();
        for (Map.Entry<String, List<Integer>> entries: topicsData.entrySet()) {
            Struct topicData = struct.instance(TOPICS_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, entries.getKey());
            List<Struct> partitionArray = new ArrayList<Struct>();
            for (Integer partiitonId : entries.getValue()) {
                Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                partitionData.set(PARTITION_KEY_NAME, partiitonId);
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS_KEY_NAME, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(TOPICS_KEY_NAME, topicArray.toArray());
        this.groupId = groupId;
        this.partitions = partitions;
    }

    public OffsetFetchRequest(Struct struct) {
        super(struct);
        partitions = new ArrayList<TopicPartition>();
        for (Object topicResponseObj : struct.getArray(TOPICS_KEY_NAME)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.getString(TOPIC_KEY_NAME);
            for (Object partitionResponseObj : topicResponse.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.getInt(PARTITION_KEY_NAME);
                partitions.add(new TopicPartition(topic, partition));
            }
        }
        groupId = struct.getString(GROUP_ID_KEY_NAME);
    }

    @Override
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
        Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData = new HashMap<TopicPartition, OffsetFetchResponse.PartitionData>();

        for (TopicPartition partition: partitions) {
            responseData.put(partition, new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET,
                    OffsetFetchResponse.NO_METADATA,
                    Errors.forException(e).code()));
        }

        switch (versionId) {
            // OffsetFetchResponseV0 == OffsetFetchResponseV1
            case 0:
            case 1:
                return new OffsetFetchResponse(responseData);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.OFFSET_FETCH.id)));
        }
    }

    public String groupId() {
        return groupId;
    }

    public List<TopicPartition> partitions() {
        return partitions;
    }

    public static OffsetFetchRequest parse(ByteBuffer buffer, int versionId) {
        return new OffsetFetchRequest(ProtoUtils.parseRequest(ApiKeys.OFFSET_FETCH.id, versionId, buffer));
    }

    public static OffsetFetchRequest parse(ByteBuffer buffer) {
        return new OffsetFetchRequest(CURRENT_SCHEMA.read(buffer));
    }
}
