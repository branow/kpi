package com.npp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.npp.model.Models.*;
import com.npp.util.JsonSerde;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Main {

    private static final String INPUT_TOPIC = "npp-raw-telemetry";
    private static final String ALERT_TOPIC = "npp-alerts";
    private static CqlSession cassandraSession;
    private static PreparedStatement insertEventStmt;
    private static PreparedStatement insertStateStmt;
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    public static void main(String[] args) {
        initCassandra();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "npp-monitoring-lab-v1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_MOST_ONCE);
        // props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        final StreamsBuilder builder = new StreamsBuilder();
        createTopology(builder);

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            if (cassandraSession != null) cassandraSession.close();
            latch.countDown();
        }));

        try {
            System.out.println("Starting NPP Stream Application...");
            streams.start();
            latch.await();
        } catch (InterruptedException e) {
            System.exit(1);
        }
    }

    private static void initCassandra() {
        try {
            cassandraSession = CqlSession.builder()
                    .withLocalDatacenter("datacenter1")
                    .addContactPoint(new InetSocketAddress("localhost", 9042))
                    .withKeyspace("npp_lab4")
                    .build();

            insertEventStmt = cassandraSession.prepare(
                "INSERT INTO nuclear_event_log (unit_id, event_time, event_type, payload) VALUES (?, ?, ?, ?)"
            );
            
            insertStateStmt = cassandraSession.prepare(
                "INSERT INTO reactor_state (unit_id, window_start, avg_neutron_flux, thermal_efficiency, safety_margin) VALUES (?, ?, ?, ?, ?)"
            );
            System.out.println("Connected to Cassandra.");
        } catch (Exception e) {
            System.err.println("Failed to connect to Cassandra (Is Docker up?): " + e.getMessage());
            System.exit(1);
        }
    }

    private static void createTopology(StreamsBuilder builder) {
        JsonSerde<Telemetry> telemetrySerde = new JsonSerde<>(Telemetry.class);
        JsonSerde<EnrichedEvent> eventSerde = new JsonSerde<>(EnrichedEvent.class);
        JsonSerde<AggregatedState> aggSerde = new JsonSerde<>(AggregatedState.class);

        KStream<String, Telemetry> rawStream = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), telemetrySerde));

        rawStream.peek(new com.npp.util.ThroughputMonitor<>("Ingestion", 1000));

        KStream<String, EnrichedEvent> enrichedStream = rawStream.mapValues(t -> {
            String type = "PARAMETER_RECORDED";
            String desc = "Normal Operation";

            if (t.reactor_pressure > 17.0 || t.neutron_flux > 1200) {
                type = "SCRAM_EVENT";
                desc = "CRITICAL: Pressure/Flux exceeded safe limits!";
            } else if (t.reactor_pressure > 16.5) {
                type = "SAFETY_LIMIT_APPROACHED";
                desc = "WARNING: Pressure high";
            }

            return EnrichedEvent.builder()
                    .telemetry(t)
                    .eventType(type)
                    .description(desc)
                    .build();
        });

        enrichedStream.foreach((key, event) -> {
            try {
                String jsonPayload = jsonMapper.writeValueAsString(event.telemetry);
                cassandraSession.execute(insertEventStmt.bind(
                    event.telemetry.unit_id,
                    Instant.ofEpochMilli(event.telemetry.timestamp),
                    event.eventType,
                    jsonPayload
                ));
                
                if (event.eventType.equals("SCRAM_EVENT")) {
                    // System.out.println("!!! SCRAM EVENT DETECTED ON " + key + " !!!");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        
        enrichedStream.filter((k, v) -> !v.eventType.equals("PARAMETER_RECORDED"))
                      .to(ALERT_TOPIC, Produced.with(Serdes.String(), eventSerde));

        KTable<Windowed<String>, AggregatedState> aggregatedTable = enrichedStream
            .groupByKey(Grouped.with(Serdes.String(), eventSerde))
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
            .aggregate(
                () -> new AggregatedState(0, 0.0, 0.0, 0.0, 0.0, 0),
                (key, event, agg) -> {
                    agg.count++;
                    agg.sumNeutronFlux += event.telemetry.neutron_flux;

                    long deltaT = event.telemetry.timestamp - agg.lastTimestamp;

                    double currentEff = (deltaT > 0) ? (event.telemetry.reactor_power / deltaT) : 0.0;

                    agg.runningEfficiencySum += currentEff;
                    agg.avgEfficiency = agg.runningEfficiencySum / agg.count;

                    agg.safetyMargin = 18.0 - event.telemetry.reactor_pressure;

                    agg.lastTimestamp = event.telemetry.timestamp;

                    return agg;
                },
                Materialized.with(Serdes.String(), aggSerde)
            );

        aggregatedTable.toStream().foreach((window, agg) -> {
            try {
                cassandraSession.execute(insertStateStmt.bind(
                    window.key(),
                    window.window().startTime(),
                    agg.sumNeutronFlux / (double)agg.count,
                    agg.avgEfficiency,
                    agg.safetyMargin
                ));
                // System.out.println("Aggregated window saved for: " + window.key());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}
