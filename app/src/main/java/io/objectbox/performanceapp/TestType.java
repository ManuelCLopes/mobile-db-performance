/*
 * Copyright 2017 ObjectBox Ltd. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.objectbox.performanceapp;


import androidx.annotation.NonNull;

public class TestType {
    public static final String CREATE_UPDATE = "Create & Update";
    public static final String CREATE_UPDATE_SCALARS = "Create & Update - scalars";
    public static final String CREATE_UPDATE_INDEXED = "Create & Update - indexed";
    public static final String CRUD = "Basic operations (CRUD)";
    public static final String CRUD_SCALARS = "Basic operations (CRUD) - scalars";
    public static final String CRUD_INDEXED = "Basic operations (CRUD) - indexed";
    public static final String QUERY_STRING = "Query by string";
    public static final String QUERY_STRING_INDEXED = "Query by string - indexed";
    public static final String QUERY_INTEGER = "Query by integer";
    public static final String QUERY_INTEGER_INDEXED = "Query by integer - indexed";
    public static final String QUERY_ID = "Query by ID";
    public static final String QUERY_ID_RANDOM = "Query by ID - random";
    public static final String DELETE_ALL = "Delete All";


    public static TestType[] ALL = {
            new TestType(CREATE_UPDATE, "create-update"),
            new TestType(CREATE_UPDATE_SCALARS, "create-update-scalars"),
            new TestType(CREATE_UPDATE_INDEXED, "create-update-indexed"),
            new TestType(CRUD, "crud"),
            new TestType(CRUD_SCALARS, "crud-scalars"),
            new TestType(CRUD_INDEXED, "crud-indexed"),
            new TestType(QUERY_STRING, "query-string"),
            new TestType(QUERY_STRING_INDEXED, "query-string-indexed"),
            new TestType(QUERY_INTEGER, "query-integer"),
            new TestType(QUERY_INTEGER_INDEXED, "query-integer-indexed"),
            new TestType(QUERY_ID, "query-id"),
            new TestType(QUERY_ID_RANDOM, "query-id-random"),
            new TestType(DELETE_ALL, "delete-all"),
    };

    public final String name;
    public final String nameShort;

    public TestType(String name, String nameShort) {
        this.name = name;
        this.nameShort = nameShort;
    }

    @NonNull
    @Override
    public String toString() {
        return name;
    }
}
