package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;
import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaxwellMqttProducer extends AbstractProducer {

	private final Logger LOGGER = LoggerFactory.getLogger(MaxwellMqttProducer.class);
	private final IMqttClient publisher;
	private final String topic;
	private final boolean topicPerTable;
	private final int qos;

	public MaxwellMqttProducer(MaxwellContext context) {
		super(context);
		String url = context.getConfig().mqttUrl;
		if (url == null || "".equals(url)) {
			throw new RuntimeException("MQTT Url must be supplied.");
		}
		String publisherId = context.getConfig().mqttPublisherId;
		topic = context.getConfig().mqttTopic;
		topicPerTable = context.getConfig().mqttTopicPerTable;
		if (!topicPerTable && (topic == null || "".equals(topic))) {
			throw new RuntimeException("Topic must be supplied if topic per table is not set");
		}
		int connectionTimeout = context.getConfig().mqttConnectionTimeout;
		qos = context.getConfig().mqttQos;

		try {
			publisher = new MqttClient(url, publisherId);

			MqttConnectOptions options = new MqttConnectOptions();
			options.setAutomaticReconnect(true);
			options.setCleanSession(true);
			options.setConnectionTimeout(connectionTimeout);
			publisher.connect(options);
		} catch (MqttException e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public void push(RowMap r) throws Exception {
		if (!r.shouldOutput(outputConfig)) {
			context.setPosition(r.getNextPosition());
			return;
		}

		String value = r.toJSON(outputConfig);
		MqttMessage msg = new MqttMessage(value.getBytes());
		msg.setQos(qos);
		msg.setRetained(true);

		if (topicPerTable) {
			publisher.publish(r.getDatabase() + "/" + r.getTable(), msg);
		} else {
			publisher.publish(topic, msg);
		}

		if (r.isTXCommit()) {
			context.setPosition(r.getNextPosition());
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("->  mqtt topic:{}, message:{}", topic, value);
		}
	}
}
