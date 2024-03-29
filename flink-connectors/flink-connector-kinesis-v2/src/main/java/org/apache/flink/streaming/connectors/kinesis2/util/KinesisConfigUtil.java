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

package org.apache.flink.streaming.connectors.kinesis2.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.kinesis2.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis2.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis2.config.ConsumerConfigConstants;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utilities for Flink Kinesis connector configuration.
 */
@Internal
public class KinesisConfigUtil {

	/** Maximum number of items to pack into an PutRecords request. **/
	protected static final String COLLECTION_MAX_COUNT = "CollectionMaxCount";

	/** Maximum number of items to pack into an aggregated record. **/
	protected static final String AGGREGATION_MAX_COUNT = "AggregationMaxCount";

	/** Limits the maximum allowed put rate for a shard, as a percentage of the backend limits.
	 * The default value is set as 100% in Flink. KPL's default value is 150% but it makes KPL throw
	 * RateLimitExceededException too frequently and breaks Flink sink as a result.
	 **/
	protected static final String RATE_LIMIT = "RateLimit";

	/**
	 * The threading model that KinesisProducer will use.
	 **/
	protected static final String THREADING_MODEL = "ThreadingModel";

	/**
	 * The maximum number of threads that the native process' thread pool will be configured with.
	 **/
	protected static final String THREAD_POOL_SIZE = "ThreadPoolSize";

	/** Default values for RateLimit. **/
	protected static final long DEFAULT_RATE_LIMIT = 100L;

	/** Default value for ThreadingModel. **/
	protected static final KinesisProducerConfiguration.ThreadingModel DEFAULT_THREADING_MODEL = KinesisProducerConfiguration.ThreadingModel.POOLED;

	/** Default values for ThreadPoolSize. **/
	protected static final int DEFAULT_THREAD_POOL_SIZE = 10;

	/**
	 * Validate configuration properties for {@link FlinkKinesisConsumer}.
	 */
	public static void validateConsumerConfiguration(Properties config) {
		checkNotNull(config, "config can not be null");

		validateAwsConfiguration(config);

		if (!(config.containsKey(AWSConfigConstants.AWS_REGION) || config.containsKey(ConsumerConfigConstants.AWS_ENDPOINT))) {
			// per validation in AwsClientBuilder
			throw new IllegalArgumentException(String.format("For FlinkKinesisConsumer AWS region ('%s') and/or AWS endpoint ('%s') must be set in the config.",
					AWSConfigConstants.AWS_REGION, AWSConfigConstants.AWS_ENDPOINT));
		}

		if (config.containsKey(ConsumerConfigConstants.STREAM_INITIAL_POSITION)) {
			String initPosType = config.getProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION);

			// specified initial position in stream must be either LATEST, TRIM_HORIZON or AT_TIMESTAMP
			try {
				ConsumerConfigConstants.InitialPosition.valueOf(initPosType);
			} catch (IllegalArgumentException e) {
				StringBuilder sb = new StringBuilder();
				for (ConsumerConfigConstants.InitialPosition pos : ConsumerConfigConstants.InitialPosition.values()) {
					sb.append(pos.toString()).append(", ");
				}
				throw new IllegalArgumentException("Invalid initial position in stream set in config. Valid values are: " + sb.toString());
			}

			// specified initial timestamp in stream when using AT_TIMESTAMP
			if (ConsumerConfigConstants.InitialPosition.valueOf(initPosType) == ConsumerConfigConstants.InitialPosition.AT_TIMESTAMP) {
				if (!config.containsKey(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP)) {
					throw new IllegalArgumentException("Please set value for initial timestamp ('"
						+ ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP + "') when using AT_TIMESTAMP initial position.");
				}
				validateOptionalDateProperty(config,
					ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP,
					config.getProperty(ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT, ConsumerConfigConstants.DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT),
					"Invalid value given for initial timestamp for AT_TIMESTAMP initial position in stream. "
						+ "Must be a valid format: yyyy-MM-dd'T'HH:mm:ss.SSSXXX or non-negative double value. For example, 2016-04-04T19:58:46.480-00:00 or 1459799926.480 .");
			}
		}

		validateOptionalPositiveIntProperty(config, ConsumerConfigConstants.SHARD_GETRECORDS_MAX,
			"Invalid value given for maximum records per getRecords shard operation. Must be a valid non-negative integer value.");

		validateOptionalPositiveIntProperty(config, ConsumerConfigConstants.SHARD_GETRECORDS_RETRIES,
			"Invalid value given for maximum retry attempts for getRecords shard operation. Must be a valid non-negative integer value.");

		validateOptionalPositiveLongProperty(config, ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_BASE,
			"Invalid value given for get records operation base backoff milliseconds. Must be a valid non-negative long value.");

		validateOptionalPositiveLongProperty(config, ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_MAX,
			"Invalid value given for get records operation max backoff milliseconds. Must be a valid non-negative long value.");

		validateOptionalPositiveDoubleProperty(config, ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_EXPONENTIAL_CONSTANT,
			"Invalid value given for get records operation backoff exponential constant. Must be a valid non-negative double value.");

		validateOptionalPositiveLongProperty(config, ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS,
			"Invalid value given for getRecords sleep interval in milliseconds. Must be a valid non-negative long value.");

		validateOptionalPositiveIntProperty(config, ConsumerConfigConstants.SHARD_GETITERATOR_RETRIES,
			"Invalid value given for maximum retry attempts for getShardIterator shard operation. Must be a valid non-negative integer value.");

		validateOptionalPositiveLongProperty(config, ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_BASE,
			"Invalid value given for get shard iterator operation base backoff milliseconds. Must be a valid non-negative long value.");

		validateOptionalPositiveLongProperty(config, ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_MAX,
			"Invalid value given for get shard iterator operation max backoff milliseconds. Must be a valid non-negative long value.");

		validateOptionalPositiveDoubleProperty(config, ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_EXPONENTIAL_CONSTANT,
			"Invalid value given for get shard iterator operation backoff exponential constant. Must be a valid non-negative double value.");

		validateOptionalPositiveLongProperty(config, ConsumerConfigConstants.SHARD_DISCOVERY_INTERVAL_MILLIS,
			"Invalid value given for shard discovery sleep interval in milliseconds. Must be a valid non-negative long value.");

		validateOptionalPositiveLongProperty(config, ConsumerConfigConstants.LIST_SHARDS_BACKOFF_BASE,
			"Invalid value given for list shards operation base backoff milliseconds. Must be a valid non-negative long value.");

		validateOptionalPositiveLongProperty(config, ConsumerConfigConstants.LIST_SHARDS_BACKOFF_MAX,
			"Invalid value given for list shards operation max backoff milliseconds. Must be a valid non-negative long value.");

		validateOptionalPositiveDoubleProperty(config, ConsumerConfigConstants.LIST_SHARDS_BACKOFF_EXPONENTIAL_CONSTANT,
			"Invalid value given for list shards operation backoff exponential constant. Must be a valid non-negative double value.");

		if (config.containsKey(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS)) {
			checkArgument(
				Long.parseLong(config.getProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS))
					< ConsumerConfigConstants.MAX_SHARD_GETRECORDS_INTERVAL_MILLIS,
				"Invalid value given for getRecords sleep interval in milliseconds. Must be lower than " +
					ConsumerConfigConstants.MAX_SHARD_GETRECORDS_INTERVAL_MILLIS + " milliseconds."
			);
		}
	}

	/**
	 * <p>
	 *  A set of configuration paremeters associated with the describeStreams API may be used if:
	 * 	1) an legacy client wants to consume from Kinesis
	 * 	2) a current client wants to consumer from DynamoDB streams
	 *
	 * In the context of 1), the set of configurations needs to be translated to the corresponding
	 * configurations in the Kinesis listShards API. In the mean time, keep these configs since
	 * they are applicable in the context of 2), i.e., polling data from a DynamoDB stream.
	 * </p>
	 *
	 * @param configProps original config properties.
	 * @return backfilled config properties.
	 */
	@SuppressWarnings("UnusedReturnValue")
	public static Properties backfillConsumerKeys(Properties configProps) {
		HashMap<String, String> oldKeyToNewKeys = new HashMap<>();
		oldKeyToNewKeys.put(ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_BASE, ConsumerConfigConstants.LIST_SHARDS_BACKOFF_BASE);
		oldKeyToNewKeys.put(ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_MAX, ConsumerConfigConstants.LIST_SHARDS_BACKOFF_MAX);
		oldKeyToNewKeys.put(ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT, ConsumerConfigConstants.LIST_SHARDS_BACKOFF_EXPONENTIAL_CONSTANT);
		for (Map.Entry<String, String> entry : oldKeyToNewKeys.entrySet()) {
			String oldKey = entry.getKey();
			String newKey = entry.getValue();
			if (configProps.containsKey(oldKey)) {
				configProps.setProperty(newKey, configProps.getProperty(oldKey));
				// Do not remove the oldKey since they may be used in the context of talking to DynamoDB streams
			}
		}
		return configProps;
	}


	/**
	 * Validate configuration properties related to Amazon AWS service.
	 */
	public static void validateAwsConfiguration(Properties config) {
		if (config.containsKey(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER)) {
			String credentialsProviderType = config.getProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER);

			// value specified for AWSConfigConstants.AWS_CREDENTIALS_PROVIDER needs to be recognizable
			AWSConfigConstants.CredentialProvider providerType;
			try {
				providerType = AWSConfigConstants.CredentialProvider.valueOf(credentialsProviderType);
			} catch (IllegalArgumentException e) {
				StringBuilder sb = new StringBuilder();
				for (AWSConfigConstants.CredentialProvider type : AWSConfigConstants.CredentialProvider.values()) {
					sb.append(type.toString()).append(", ");
				}
				throw new IllegalArgumentException("Invalid AWS Credential Provider Type set in config. Valid values are: " + sb.toString());
			}

			// if BASIC type is used, also check that the Access Key ID and Secret Key is supplied
			if (providerType == AWSConfigConstants.CredentialProvider.BASIC) {
				if (!config.containsKey(AWSConfigConstants.AWS_ACCESS_KEY_ID)
					|| !config.containsKey(AWSConfigConstants.AWS_SECRET_ACCESS_KEY)) {
					throw new IllegalArgumentException("Please set values for AWS Access Key ID ('" + AWSConfigConstants.AWS_ACCESS_KEY_ID + "') " +
						"and Secret Key ('" + AWSConfigConstants.AWS_SECRET_ACCESS_KEY + "') when using the BASIC AWS credential provider type.");
				}
			}
		}

		if (config.containsKey(AWSConfigConstants.AWS_REGION)) {
			// specified AWS Region name must be recognizable
			if (!AWSUtil.isValidRegion(config.getProperty(AWSConfigConstants.AWS_REGION))) {
				StringBuilder sb = new StringBuilder();
				for (Regions region : Regions.values()) {
					sb.append(region.getName()).append(", ");
				}
				throw new IllegalArgumentException("Invalid AWS region set in config. Valid values are: " + sb.toString());
			}
		}
	}

	private static void validateOptionalPositiveLongProperty(Properties config, String key, String message) {
		if (config.containsKey(key)) {
			try {
				long value = Long.parseLong(config.getProperty(key));
				if (value < 0) {
					throw new NumberFormatException();
				}
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException(message);
			}
		}
	}

	private static void validateOptionalPositiveIntProperty(Properties config, String key, String message) {
		if (config.containsKey(key)) {
			try {
				int value = Integer.parseInt(config.getProperty(key));
				if (value < 0) {
					throw new NumberFormatException();
				}
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException(message);
			}
		}
	}

	private static void validateOptionalPositiveDoubleProperty(Properties config, String key, String message) {
		if (config.containsKey(key)) {
			try {
				double value = Double.parseDouble(config.getProperty(key));
				if (value < 0) {
					throw new NumberFormatException();
				}
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException(message);
			}
		}
	}

	private static void validateOptionalDateProperty(Properties config, String timestampKey, String format, String message) {
		if (config.containsKey(timestampKey)) {
			try {
				SimpleDateFormat customDateFormat = new SimpleDateFormat(format);
				customDateFormat.parse(config.getProperty(timestampKey));
			} catch (IllegalArgumentException | NullPointerException exception) {
				throw new IllegalArgumentException(message);
			} catch (ParseException exception) {
				try {
					double value = Double.parseDouble(config.getProperty(timestampKey));
					if (value < 0) {
						throw new IllegalArgumentException(message);
					}
				} catch (NumberFormatException numberFormatException) {
					throw new IllegalArgumentException(message);
				}
			}
		}
	}
}
