package edu.montana.csci.csci440.helpers;

import edu.montana.csci.csci440.model.Employee;

import java.util.*;

public class EmployeeHelper {

    public static String makeEmployeeTree() {
        Employee employee = Employee.find(1); // root employee
        // and use this data structure to maintain reference information needed to build the tree structure
        Map<Long, List<Employee>> employeeMap = new HashMap<>();

        Employee.all().forEach(e -> {
            long report = e.getReportsTo();
            List<Employee> reportsTo = (employeeMap.get(report) != null) ? employeeMap.get(report) : new LinkedList<>();
            employeeMap.putIfAbsent(e.getReportsTo(), reportsTo);
            reportsTo.add(e);
        });
        return "<ul>" + makeTree(Objects.requireNonNull(employee), employeeMap) + "</ul>";
    }

    public static String makeTree(Employee employee, Map<Long, List<Employee>> employeeMap) {
        StringBuilder list = new StringBuilder("<li><a href='/employees" + employee.getEmployeeId() + "'>"
                + employee.getEmail() + "</a><ul>");
        List<Employee> reports = employeeMap.get(employee.getEmployeeId());
        if (reports != null) {
            for (Employee report : reports) {
                list.append(makeTree(report, employeeMap));
            }
        }
        return list + "</ul></li>";
    }
}
