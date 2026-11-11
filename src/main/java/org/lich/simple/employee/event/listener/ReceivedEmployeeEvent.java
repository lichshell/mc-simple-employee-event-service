package org.lich.simple.employee.event.listener;

import org.lich.simple.employee.event.model.Employee;

public record ReceivedEmployeeEvent(String key, int partition, long offset, Employee value) { }
