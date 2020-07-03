package org.apache.flink.streaming.connectors.kinesis2.internals;

import org.apache.flink.streaming.connectors.kinesis2.model.SequenceNumber;

import io.reactivex.functions.Consumer;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public abstract class KinesisRecordConsumer<T> implements Consumer<List<Record>> {
	public abstract void setSeqNumRef(AtomicReference<SequenceNumber> seqRef);
}
