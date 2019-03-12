package io.jaegertracing.analytics.avro;

import com.uber.jaeger.Span;
import com.uber.jaeger.SpanEnvelope;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

/**
 * Converts a byte array into a {@link Span}
 */
@Slf4j
public class SpanDeserializer extends AbstractDeserializationSchema<Span> {
    private transient DatumReader<SpanEnvelope> reader;
    private transient BinaryDecoder decoder;

    @Override
    public Span deserialize(byte[] message) {
        decoder = DecoderFactory.get().binaryDecoder(message, 0, message.length, decoder);

        if (reader == null) {
            reader = new SpecificDatumReader<>(SpanEnvelope.class);
        }

        SpanEnvelope envelope;
        try {
            // Set the reuse parameter to null to return fresh objects to clients.
            envelope = reader.read(null, decoder);
            return envelope.getMsg();
        } catch (IOException e) {
            log.warn("Unable to deserialize message.", e);
            return null;
        }
    }
}
