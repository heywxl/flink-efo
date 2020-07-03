package org.apache.flink.streaming.connectors.kinesis2.util;

import org.apache.flink.streaming.connectors.kinesis2.config.AWSConfigConstants;

import com.amazonaws.auth.AWSCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * Some utilities specific to Aws sdk v2 for enhanced fan-out.
 */
public class AWS2Util {

	/**
	 * Creates a KinesisAsyncClient.
	 *
	 * @param configProps configuration properties containing the access key, secret key, and region
	 * @return a new KinesisAsyncClient
	 */
	public static KinesisAsyncClient createKinesisAsyncClient(Properties configProps) {
		AwsCredentialsProvider provider = getCredentialsProviderV2(configProps);

		return KinesisAsyncClient
			.builder()
			.region(
				Region.of(configProps.getProperty(AWSConfigConstants.AWS_REGION))
			)
			.credentialsProvider(provider)
			.build();
	}

	/**
	 * Return a {@link AwsCredentialsProvider} instance corresponding to the configuration properties.
	 * Attention: This is different from instance {@link AWSCredentialsProvider}
	 *
	 * @param configProps the configuration properties
	 * @return The corresponding Aws Credentials Provider instance
	 */
	public static AwsCredentialsProvider getCredentialsProviderV2(final Properties configProps) {
		return getCredentialsProviderV2(configProps, AWSConfigConstants.AWS_CREDENTIALS_PROVIDER);
	}

	/**
	 * Return a {@link AwsCredentialsProvider} instance corresponding to the configuration properties.
	 * Attention: This is different from instance {@link AWSCredentialsProvider}.
	 * Attention: For now it only supports the basic way for quick development purpose.
	 *
	 * @param configProps  the configuration properties
	 * @param configPrefix the prefix of the config properties for this credentials provider,
	 *                     e.g. aws.credentials.provider for the base credentials provider,
	 *                     aws.credentials.provider.role.provider for the credentials provider
	 *                     for assuming a role, and so on.
	 * @return The corresponding Aws Credentials Provider instance
	 */
	//TODO support all kinds of credential providers
	private static AwsCredentialsProvider getCredentialsProviderV2(Properties configProps, final String configPrefix) {
		AWSConfigConstants.CredentialProvider credentialProviderType;
		if (!configProps.containsKey(configPrefix)) {
			if (configProps.containsKey(AWSConfigConstants.accessKeyId(configPrefix))
				&& configProps.containsKey(AWSConfigConstants.secretKey(configPrefix))) {
				// if the credential provider type is not specified, but the Access Key ID and Secret Key are given, it will default to BASIC
				credentialProviderType = AWSConfigConstants.CredentialProvider.BASIC;
			} else {
				// if the credential provider type is not specified, it will default to AUTO
				credentialProviderType = AWSConfigConstants.CredentialProvider.AUTO;
			}
		} else {
			credentialProviderType = AWSConfigConstants.CredentialProvider.valueOf(configProps.getProperty(configPrefix));
		}

		switch (credentialProviderType) {
			case BASIC:
				return StaticCredentialsProvider.create(
					AwsBasicCredentials.create(
						configProps.getProperty(AWSConfigConstants.accessKeyId(configPrefix)),
						configProps.getProperty(AWSConfigConstants.secretKey(configPrefix))
					)
				);

			default:
				return null;
		}
	}

	/**
	 * Describe the stream in order to find out stream info like streamArn, etc.
	 *
	 * @param client     a KinesisAsyncClient
	 * @param streamName the stream to describe
	 * @return a CompletableFuture of describe stream
	 */
	public static CompletableFuture<DescribeStreamResponse> executeDescribeStream(KinesisAsyncClient client, String streamName) {
		DescribeStreamRequest request = DescribeStreamRequest
			.builder()
			.streamName(streamName)
			.build();

		return client.describeStream(request);
	}

	/**
	 * Describe the enhanced-fanout consumer if the consumer exists.
	 *
	 * @param client       a KinesisAsyncClient
	 * @param streamArn    the streamArn to describe
	 * @param consumerName the specific consumer name we want to describe
	 * @return a CompletableFuture of describe stream consumer
	 */
	public static CompletableFuture<DescribeStreamConsumerResponse> executeDescribeStreamConsumer(KinesisAsyncClient client, String streamArn, String consumerName) {
		DescribeStreamConsumerRequest request = DescribeStreamConsumerRequest
			.builder()
			.streamARN(streamArn)
			.consumerName(consumerName)
			.build();

		return client.describeStreamConsumer(request);
	}

	/**
	 * @param client       a KinesisAsyncClient
	 * @param streamArn    the streamArn to describe
	 * @param consumerName the specific consumer name we want to describe
	 * @return a CompletableFuture of register stream consumer
	 */
	public static CompletableFuture<RegisterStreamConsumerResponse> executeRegisterStreamConsumer(KinesisAsyncClient client, String streamArn, String consumerName) {
		RegisterStreamConsumerRequest request = RegisterStreamConsumerRequest
			.builder()
			.streamARN(streamArn)
			.consumerName(consumerName)
			.build();

		return client.registerStreamConsumer(request);
	}
}
