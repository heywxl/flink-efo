/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kinesis2.proxy;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kinesis2.internals.KinesisRecordConsumer;
import org.apache.flink.streaming.connectors.kinesis2.model.StreamShardHandle;

import com.amazonaws.services.kinesis.model.GetRecordsResult;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;

import java.util.Map;

/**
 * Interface for a Kinesis proxy that operates on multiple Kinesis streams within the same AWS service region.
 */
@Internal
public interface KinesisProxyInterface<T> {

	/**
	 * Get a shard iterator from the specified position in a shard.
	 * The retrieved shard iterator can be used in {@link KinesisProxyInterface#getRecords(String, int)}}
	 * to read data from the Kinesis shard.
	 *
	 * @param shard the shard to get the iterator
	 * @param shardIteratorType the iterator type, defining how the shard is to be iterated
	 *                          (one of: TRIM_HORIZON, LATEST, AT_TIMESTAMP, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER)
	 * @param startingMarker should be {@code null} if shardIteratorType is TRIM_HORIZON or LATEST,
	 *                       should be a {@code Date} value if shardIteratorType is AT_TIMESTAMP,
	 *                       should be a {@code String} representing the sequence number if shardIteratorType is AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER
	 * @return shard iterator which can be used to read data from Kinesis
	 * @throws InterruptedException this method will retry with backoff if AWS Kinesis complains that the
	 *                              operation has exceeded the rate limit; this exception will be thrown
	 *                              if the backoff is interrupted.
	 */
	String getShardIterator(StreamShardHandle shard, String shardIteratorType, Object startingMarker) throws InterruptedException;

	/**
	 * Get the next batch of data records using a specific shard iterator.
	 *
	 * @param shardIterator a shard iterator that encodes info about which shard to read and where to start reading
	 * @param maxRecordsToGet the maximum amount of records to retrieve for this batch
	 * @return the batch of retrieved records, also with a shard iterator that can be used to get the next batch
	 * @throws InterruptedException this method will retry with backoff if AWS Kinesis complains that the
	 *                              operation has exceeded the rate limit; this exception will be thrown
	 *                              if the backoff is interrupted.
	 */
	GetRecordsResponse getRecords(String shardIterator, int maxRecordsToGet) throws InterruptedException;

	/**
	 * Get shard list of multiple Kinesis streams, ignoring the
	 * shards of each stream before a specified last seen shard id.
	 *
	 * @param streamNamesWithLastSeenShardIds a map with stream as key, and last seen shard id as value
	 * @return result of the shard list query
	 * @throws InterruptedException this method will retry with backoff if AWS Kinesis complains that the
	 *                              operation has exceeded the rate limit; this exception will be thrown
	 *                              if the backoff is interrupted.
	 */
	GetShardListResult getShardList(Map<String, String> streamNamesWithLastSeenShardIds) throws InterruptedException;

	/**
	 * Get Kinesis Stream Arn and enhanced-fanout Arn if the consumerName has been registered before.
	 * Otherwise, register and return.
	 * @param streamName Kinesis stream name
	 * @param consumerName the enhanced fan-out consumer name you want to register.
	 * @return result of register consumer in a tuple: streamArn , consumerArn
	 * @throws InterruptedException this method will retry with backoff if AWS Kinesis complains that the
	 * 	 *                          operation has exceeded the rate limit; this exception will be thrown
	 * 	 *                          if the backoff is interrupted.
	 */
	default Tuple2<String, String> getOrRegisterConsumer(String streamName, String consumerName) throws InterruptedException{
		//for test purpose.
		return null;
	}

	default StartingPosition subscribeToShard(String shardId, String consumerArn, StartingPosition nextPosition, int bufferNum, KinesisRecordConsumer<T> consumer) throws InterruptedException{
		//for test purpose.
		return null;
	}

}
