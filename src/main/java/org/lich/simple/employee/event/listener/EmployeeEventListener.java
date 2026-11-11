package org.lich.simple.employee.event.listener;

import org.lich.simple.employee.event.model.Employee;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

@Component
public class EmployeeEventListener {

    static final int MAX_KEPT = 100;
    private static final Logger log = LoggerFactory.getLogger(EmployeeEventListener.class);

    private final Deque<ReceivedEmployeeEvent> recent = new ConcurrentLinkedDeque<>();

    // Consumer settings (group id, deserializers, registry URL, target type) come from application.yml
    @KafkaListener(topics = "${app.topic}")
    public void onMessage(ConsumerRecord<String, Employee> rec) {
        log.info("received p={} o={} key={} value={}", rec.partition(), rec.offset(), rec.key(), rec.value());
        recent.addLast(new ReceivedEmployeeEvent(rec.key(), rec.partition(), rec.offset(), rec.value()));
        while (recent.size() > MAX_KEPT) {
            recent.pollFirst();
        }
    }

    /** Most recent messages received (oldest first, capped at 100). */
    public List<ReceivedEmployeeEvent> recent() {
        return List.copyOf(recent);
    }
}
