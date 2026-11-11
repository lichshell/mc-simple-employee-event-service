package org.lich.simple.employee.event.listener;

import org.lich.simple.employee.event.model.Employee;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit test: listener bookkeeping, no Kafka involved. */
class EmployeeEventListenerTest {

    private static Employee emp(String id) {
        return new Employee(id, "First", "Last", null, null, null, null, null, null, null);
    }

    private static ConsumerRecord<String, Employee> record(long offset, String id) {
        return new ConsumerRecord<>("employees", 0, offset, id, emp(id));
    }

    @Test
    void storesReceivedMessageWithPartitionAndOffset() {
        EmployeeEventListener listener = new EmployeeEventListener();
        listener.onMessage(record(5L, "E-1"));

        List<ReceivedEmployeeEvent> recent = listener.recent();
        assertEquals(1, recent.size());
        assertEquals("E-1", recent.get(0).key());
        assertEquals(0, recent.get(0).partition());
        assertEquals(5L, recent.get(0).offset());
        assertEquals("E-1", recent.get(0).value().employeeId());
    }

    @Test
    void keepsOnlyTheNewestMessages() {
        EmployeeEventListener listener = new EmployeeEventListener();
        int total = EmployeeEventListener.MAX_KEPT + 25;
        for (int i = 0; i < total; i++) {
            listener.onMessage(record(i, "E-" + i));
        }

        List<ReceivedEmployeeEvent> recent = listener.recent();
        assertEquals(EmployeeEventListener.MAX_KEPT, recent.size());
        assertEquals(25L, recent.get(0).offset());                    // oldest kept
        assertEquals(total - 1L, recent.get(recent.size() - 1).offset()); // newest
    }

    @Test
    void recentReturnsASnapshotCopy() {
        EmployeeEventListener listener = new EmployeeEventListener();
        listener.onMessage(record(1L, "E-1"));
        List<ReceivedEmployeeEvent> snapshot = listener.recent();
        listener.onMessage(record(2L, "E-2"));

        assertEquals(1, snapshot.size());
        assertTrue(listener.recent().size() == 2);
    }
}
