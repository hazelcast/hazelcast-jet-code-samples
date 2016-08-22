package averagesalary.model;

import java.io.Serializable;

import static java.lang.Integer.valueOf;

public class Employee implements Serializable {

    private String id;
    private int salary;

    public Employee() {
    }

    public Employee(String id, int salary) {
        this.id = id;
        this.salary = salary;
    }

    public String getId() {
        return id;
    }

    public int getSalary() {
        return salary;
    }

    public void fromString(String value) {
        String[] split = value.split(",");
        if (split.length < 2) throw new IllegalArgumentException(value);
        id = split[0];
        salary = valueOf(split[1]);
    }

    @Override
    public String toString() {
        return "Employee{"
                + "id='" + id + '\''
                + ", salary=" + salary
                + '}';
    }
}
