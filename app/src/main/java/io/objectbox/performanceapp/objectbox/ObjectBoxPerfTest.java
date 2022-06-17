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

package io.objectbox.performanceapp.objectbox;

import android.content.Context;

import java.util.ArrayList;
import java.util.List;

import io.objectbox.Box;
import io.objectbox.BoxStore;
import io.objectbox.performanceapp.PerfTest;
import io.objectbox.performanceapp.PerfTestRunner;
import io.objectbox.performanceapp.TestType;
import io.objectbox.query.Query;

import static io.objectbox.query.QueryBuilder.StringOrder.CASE_SENSITIVE;

public class ObjectBoxPerfTest extends PerfTest {
    private BoxStore store;

    private boolean versionLoggedOnce;
    private Box<SimpleEntity> box;
    private Box<SimpleEntityIndexed> boxIndexed;

    @Override
    public String name() {
        return "ObjectBox";
    }

    public void setUp(Context context, PerfTestRunner testRunner) {
        super.setUp(context, testRunner);
        store = MyObjectBox.builder().androidContext(context).build();
        store.close();
        //store.deleteAllFiles();
        // 8 GB for DB to allow putting millions of objects
        store = MyObjectBox.builder().androidContext(context).maxSizeInKByte(8 * 1024 * 1024).build();
        box = store.boxFor(SimpleEntity.class);
        boxIndexed = store.boxFor(SimpleEntityIndexed.class);

        if (!versionLoggedOnce) {
            String versionNative = BoxStore.getVersionNative();
            String versionJava = BoxStore.getVersion();
            if (versionJava.equals(versionNative)) {
                log("ObjectBox " + versionNative);
            } else {
                log("ObjectBox " + versionNative + " (Java: " + versionJava + ")");
            }
            versionLoggedOnce = true;
        }
    }

    @Override
    public void run(TestType type) {
        log("Current data on db: " + (box.count() + boxIndexed.count()) + " objects");

        switch (type.name) {
            case TestType.CREATE_UPDATE:
                runCreateUpdateTest();
                break;
            case TestType.CREATE_UPDATE_INDEXED:
                runCreateUpdateIndexedTest();
                break;
            case TestType.CRUD:
                runBatchPerfTest();
                break;
            case TestType.CRUD_INDEXED:
                runBatchPerfTestIndexed();
                break;
            case TestType.QUERY_STRING:
                runQueryByString();
                break;
            case TestType.QUERY_STRING_INDEXED:
                runQueryByStringIndexed();
                break;
            case TestType.QUERY_INTEGER:
                runQueryByInteger();
                break;
            case TestType.QUERY_INTEGER_INDEXED:
                runQueryByIntegerIndexed();
                break;
            case TestType.QUERY_ID:
                runQueryById();
                break;
            case TestType.DELETE_ALL:
                runDeleteAll();
                break;
        }
    }

    public void runDeleteAll(){
        startBenchmark("delete");
        box.removeAll();
        stopBenchmark();

        startBenchmark("delete indexed");
        boxIndexed.removeAll();
        stopBenchmark();

        store.close();
        store.deleteAllFiles();
    }

    public void runCreateUpdateTest(){
        List<SimpleEntity> list = prepareAndPutEntities();

        for (SimpleEntity entity : list) {
            setRandomValues(entity);

        }
        startBenchmark("update");
        box.put(list);
        stopBenchmark();
    }

    public void runCreateUpdateIndexedTest(){
        List<SimpleEntityIndexed> list = prepareAndPutEntitiesIndexed();

        for (SimpleEntityIndexed entity : list) {
            setRandomValues(entity);
        }
        startBenchmark("update");
        boxIndexed.put(list);
        stopBenchmark();
    }

    public void runBatchPerfTest() {
        List<SimpleEntity> list = prepareAndPutEntities();

        for (SimpleEntity entity : list) {
            setRandomValues(entity);

        }
        startBenchmark("update");
        box.put(list);
        stopBenchmark();

        startBenchmark("load");
        List<SimpleEntity> reloaded = box.getAll();
        stopBenchmark();

        startBenchmark("access");
        accessAll(reloaded);
        stopBenchmark();

        startBenchmark("delete all");
        box.remove(reloaded);
        stopBenchmark();

        store.close();
        store.deleteAllFiles();
    }

    protected void setRandomValues(SimpleEntity entity) {
        setRandomScalars(entity);
        entity.setSimpleString(randomString());
        entity.setSimpleByteArray(randomBytes());
    }

    private void setRandomScalars(SimpleEntity entity) {
        entity.setSimpleBoolean(random.nextBoolean());
        entity.setSimpleByte((byte) random.nextInt());
        entity.setSimpleShort((short) random.nextInt());
        entity.setSimpleInt(random.nextInt(1000));
        entity.setSimpleLong(random.nextLong());
        entity.setSimpleDouble(random.nextDouble());
        entity.setSimpleFloat(random.nextFloat());
    }

    public SimpleEntity createEntity() {
        SimpleEntity entity = new SimpleEntity();
        setRandomValues(entity);

        return entity;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    protected void accessAll(List<SimpleEntity> list) {
        for (SimpleEntity entity : list) {
            entity.getId();
            entity.getSimpleBoolean();
            entity.getSimpleByte();
            entity.getSimpleShort();
            entity.getSimpleInt();
            entity.getSimpleLong();
            entity.getSimpleFloat();
            entity.getSimpleDouble();
            entity.getSimpleString();
            entity.getSimpleByteArray();
        }
    }

    public void runBatchPerfTestIndexed() {
        List<SimpleEntityIndexed> list = prepareAndPutEntitiesIndexed();

        for (SimpleEntityIndexed entity : list) {
            setRandomValues(entity);
        }
        startBenchmark("update");
        boxIndexed.put(list);
        stopBenchmark();

        startBenchmark("load");
        List<SimpleEntityIndexed> reloaded = boxIndexed.getAll();
        stopBenchmark();

        startBenchmark("access");
        accessAllIndexed(reloaded);
        stopBenchmark();

        startBenchmark("delete all");
        boxIndexed.remove(reloaded);
        stopBenchmark();
    }

    protected void setRandomValues(SimpleEntityIndexed entity) {
        entity.setSimpleBoolean(random.nextBoolean());
        entity.setSimpleByte((byte) random.nextInt());
        entity.setSimpleShort((short) random.nextInt());
        entity.setSimpleInt(random.nextInt(1000));
        entity.setSimpleLong(random.nextLong());
        entity.setSimpleDouble(random.nextDouble());
        entity.setSimpleFloat(random.nextFloat());
        entity.setSimpleString(randomString());
        entity.setSimpleByteArray(randomBytes());
    }

    public SimpleEntityIndexed createEntityIndexed() {
        SimpleEntityIndexed entity = new SimpleEntityIndexed();
        setRandomValues(entity);
        return entity;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    protected void accessAllIndexed(List<SimpleEntityIndexed> list) {
        for (SimpleEntityIndexed entity : list) {
            entity.getId();
            entity.getSimpleBoolean();
            entity.getSimpleByte();
            entity.getSimpleShort();
            entity.getSimpleInt();
            entity.getSimpleLong();
            entity.getSimpleFloat();
            entity.getSimpleDouble();
            entity.getSimpleString();
            entity.getSimpleByteArray();
        }
    }

    private void runQueryByString() {
        String s = box.get(1).simpleString;

        startBenchmark("query");

        Query<SimpleEntity> query = box.query()
                .equal(SimpleEntity_.simpleString, "", CASE_SENSITIVE)
                .parameterAlias("string")
                .build();
        query.setParameter("string", s);
        List<SimpleEntity> result = query.find();
        accessAll(result);

        stopBenchmark();
        log("Entities found: " + result.size());
    }

    private void runQueryByInteger() {
        int i = box.get(1).simpleInt;

        startBenchmark("query");

        Query<SimpleEntity> query = box.query()
                .equal(SimpleEntity_.simpleInt, i)
                .parameterAlias("int")
                .build();
        query.setParameter("int", i);
        List<SimpleEntity> result = query.find();
        accessAll(result);

        stopBenchmark();
        log("Entities found: " + result.size());
    }

    private List<SimpleEntity> prepareAndPutEntities() {
        List<SimpleEntity> entities = new ArrayList<>(numberEntities);
        for (int i = 0; i < numberEntities; i++) {
            entities.add(createEntity());
        }

        startBenchmark("insert");
        box.put(entities);
        log("Test data inserted: " + box.count() + " objects");
        stopBenchmark();

        return entities;
    }

    private void runQueryByStringIndexed() {
        String s = boxIndexed.get(1).getSimpleString();

        startBenchmark("query");
        Query<SimpleEntityIndexed> query = boxIndexed.query()
                .equal(SimpleEntityIndexed_.simpleString, "", CASE_SENSITIVE)
                .parameterAlias("string")
                .build();
        query.setParameter("string", s);
        List<SimpleEntityIndexed> result = query.find();
        accessAllIndexed(result);

        stopBenchmark();
        log("Entities found: " + result.size());
    }

    private void runQueryByIntegerIndexed() {
        int i = boxIndexed.get(1).getSimpleInt();

        startBenchmark("query");

        Query<SimpleEntityIndexed> query = boxIndexed.query()
                .equal(SimpleEntityIndexed_.simpleInt, 0)
                .parameterAlias("int")
                .build();
        query.setParameter("int", i);
        List<SimpleEntityIndexed> result = query.find();
        accessAllIndexed(result);

        stopBenchmark();
        log("Entities found: " + result.size());
    }

    private List<SimpleEntityIndexed> prepareAndPutEntitiesIndexed() {
        List<SimpleEntityIndexed> entities = new ArrayList<>(numberEntities);
        for (int i = 0; i < numberEntities; i++) {
            entities.add(createEntityIndexed());
        }

        startBenchmark("insert");
        boxIndexed.put(entities);
        stopBenchmark();

        return entities;
    }

    private void runQueryById() {
        int i = random.nextInt((int) box.count());

        benchmark("query", () -> {
            SimpleEntity results = box.get(i);
        });
    }

    @Override
    public void tearDown() {
        store.close();
        //store.deleteAllFiles();
    }

}
