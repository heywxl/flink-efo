package org.apache.flink.streaming.connectors.kinesis2.internals;

import org.apache.flink.streaming.connectors.kinesis2.metrics.ShardMetricsReporter;
import org.apache.flink.streaming.connectors.kinesis2.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis2.serialization.KinesisDeserializationSchema;

import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import io.reactivex.functions.Consumer;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import javax.swing.text.html.Option;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This is a User Record Consumer of RxJava to consumer Kinesis User Record
 */
public class KinesisRecordConsumerImpl<T> extends KinesisRecordConsumer<T>{
	private String streamName;

	private String shardId;

	private KinesisDataFetcher<T> fetcherRef;

	private KinesisDeserializationSchema<T> deserializer;

	private ShardMetricsReporter shardMetricsReporter;

	private String startingHashKey;
	private String endingHashKey;
	private final int subscribedShardStateIndex;
	private AtomicReference<SequenceNumber> sequenceNumberAtomicReference = null;

	public KinesisRecordConsumerImpl(
		String streamName,
		String shardId,
		String startingHashKey,
		String endingHashKey,
		int subscribedShardStateIndex,
		KinesisDataFetcher<T> fetcherRef,
		KinesisDeserializationSchema<T> deserializer,
		ShardMetricsReporter shardMetricsReporter
	){
		this.streamName = streamName;
		this.shardId = shardId;
		this.startingHashKey = startingHashKey;
		this.endingHashKey = endingHashKey;
		this.subscribedShardStateIndex = subscribedShardStateIndex;
		this.fetcherRef = fetcherRef;
		this.deserializer = deserializer;
		this.shardMetricsReporter = shardMetricsReporter;
	}

	@Override
	public void setSeqNumRef(AtomicReference<SequenceNumber> seqRef){
		this.sequenceNumberAtomicReference = seqRef;
	}

	@Override
	public void accept(List<Record> records) throws Exception {

		int numberOfAggregatedRecords = records.size();
		shardMetricsReporter.setNumberOfAggregatedRecords(numberOfAggregatedRecords);
		List<KinesisClientRecord> fetchedRecords = deaggregateRecords(records);
		for (KinesisClientRecord record : fetchedRecords) {
			deserializeRecordForCollectionAndUpdateState(record);
		}
		int numberOfDeaggregatedRecords = fetchedRecords.size();
		shardMetricsReporter.setNumberOfDeaggregatedRecords(numberOfDeaggregatedRecords);
	}

	private void deserializeRecordForCollectionAndUpdateState(KinesisClientRecord record)
		throws IOException {
		if(sequenceNumberAtomicReference == null){
			throw new IOException("The SequenceNumber Reference is not set, please check your code.");
		}

		ByteBuffer recordData = record.data();

		byte[] dataBytes = new byte[recordData.remaining()];
		recordData.get(dataBytes);

		final long approxArrivalTimestamp = record.approximateArrivalTimestamp().getEpochSecond();

		final T value = deserializer.deserialize(
			dataBytes,
			record.partitionKey(),
			record.sequenceNumber(),
			approxArrivalTimestamp,
			streamName,
			shardId);

		SequenceNumber collectedSequenceNumber = (record.aggregated())
			? new SequenceNumber(record.sequenceNumber(), record.subSequenceNumber())
			: new SequenceNumber(record.sequenceNumber());

		fetcherRef.emitRecordAndUpdateState(
			value,
			approxArrivalTimestamp,
			subscribedShardStateIndex,
			collectedSequenceNumber);

		sequenceNumberAtomicReference.set(collectedSequenceNumber);
	}

	private List<KinesisClientRecord> deaggregateRecords(List<Record> records) {
		AggregatorUtil util = new AggregatorUtil();

		List<KinesisClientRecord> aggregatedRecords = records
			.stream()
			.map(KinesisClientRecord::fromRecord)
			.collect(Collectors.toList());

		return util.deaggregate(aggregatedRecords,startingHashKey, endingHashKey);
	}


}
