package fr.univ_pau.kafkaandflink;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDecoder;

public class ConfluentAvroDeserializationSchema implements DeserializationSchema<String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final String schemaRegistryUrl;
	private final int identityMapCapacity;
	private KafkaAvroDecoder kafkaAvroDecoder;

	public ConfluentAvroDeserializationSchema(String schemaRegistyUrl) {
		this(schemaRegistyUrl, 1000);
	}

	public ConfluentAvroDeserializationSchema(String schemaRegistryUrl, int identityMapCapacity) {
		this.schemaRegistryUrl = schemaRegistryUrl;
		this.identityMapCapacity = identityMapCapacity;
	}

	public String deserialize(byte[] message) {
		if (kafkaAvroDecoder == null) {
			SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(this.schemaRegistryUrl,
					this.identityMapCapacity);
			this.kafkaAvroDecoder = new KafkaAvroDecoder(schemaRegistry);
		}
		return this.kafkaAvroDecoder.fromBytes(message).toString();
	}

	public boolean isEndOfStream(String nextElement) {
		return false;
	}

	public TypeInformation<String> getProducedType() {
		return BasicTypeInfo.STRING_TYPE_INFO;
	}

}