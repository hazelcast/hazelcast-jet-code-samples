/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.samples.cogroup;

import java.io.Serializable;

/**
 * A DTO for employee.
 * <p>
 * For the sake of simplicity we use {@link Serializable} here. For better
 * performance we should have used Hazelcast Custom Serialization.
 */
public class Person implements Serializable {

    // sample data
    public static final Person[] STUDENTS = {
            new Person("Alice", 21),
            new Person("Bob", 22),
            new Person("Cecile", 23),
            new Person("Dennis", 21),
            new Person("Ed", 22),
    };

    public static final Person[] EMPLOYEES = {
            new Person("Victor", 22),
            new Person("Wendy", 23),
            new Person("Xenia", 24),
            new Person("Yvette", 22),
            new Person("Zachary", 23),
    };

    private final String name;
    private final int age;

    public Person(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    @Override
    public String toString() {
        return "Person{name='" + name + '\'' + ", age=" + age + '}';
    }
}
