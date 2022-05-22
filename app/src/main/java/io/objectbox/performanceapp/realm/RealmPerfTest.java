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

package io.objectbox.performanceapp.realm;

import android.content.Context;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import io.objectbox.performanceapp.PerfTest;
import io.objectbox.performanceapp.PerfTestRunner;
import io.objectbox.performanceapp.TestType;
import io.realm.Realm;
import io.realm.RealmConfiguration;
import io.realm.RealmResults;

public class RealmPerfTest extends PerfTest {

    private boolean versionLoggedOnce;
    private Realm realm;

    @Override
    public String name() {
        return "Realm";
    }

    public void setUp(Context context, PerfTestRunner testRunner) {
        super.setUp(context, testRunner);
        Realm.init(context);
        realm = Realm.getDefaultInstance();

        //RealmConfiguration configuration = realm.getConfiguration();
        realm.close();
        //Realm.deleteRealm(configuration);
        realm = Realm.getDefaultInstance();

        if (!versionLoggedOnce) {
            //log("Realm " + ??);
            versionLoggedOnce = true;
        }
    }

    @Override
    public void run(TestType type) {
        switch (type.name) {
            case TestType.CRUD:
                runBatchPerfTest(false);
                break;
            case TestType.CRUD_SCALARS:
                runBatchPerfTest(true);
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
            case TestType.QUERY_ID_RANDOM:
                runQueryById();
                break;
        }
    }

    public void runBatchPerfTest(boolean scalarsOnly) {
        List<SimpleEntity> list = new ArrayList<>(numberEntities);
        for (int i = 0; i < numberEntities; i++) {
            list.add(createEntity(i, scalarsOnly));
        }
        startBenchmark("insert");
        realm.beginTransaction();
        realm.insert(list);
        realm.commitTransaction();
        stopBenchmark();

        for (SimpleEntity entity : list) {
            if (scalarsOnly) {
                setRandomScalars(entity);
            } else {
                setRandomValues(entity);
            }
        }
        startBenchmark("update");
        realm.beginTransaction();
        realm.insertOrUpdate(list);
        realm.commitTransaction();
        stopBenchmark();

        /*
        //noinspection UnusedAssignment
        list = null;

        startBenchmark("load");
        RealmResults<SimpleEntity> reloaded = realm.where(SimpleEntity.class).findAll();
        stopBenchmark();

        startBenchmark("access");
        accessAll(reloaded);
        stopBenchmark();

        startBenchmark("delete");
        realm.beginTransaction();
        reloaded.deleteAllFromRealm();
        realm.commitTransaction();
        stopBenchmark();
         */
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
        entity.setSimpleInt(random.nextInt());
        entity.setSimpleLong(random.nextLong());
        entity.setSimpleDouble(random.nextDouble());
        entity.setSimpleFloat(random.nextFloat());
    }

    public SimpleEntity createEntity(long id, boolean scalarsOnly) {
        SimpleEntity entity = new SimpleEntity();
        entity.setId(id);
        if (scalarsOnly) {
            setRandomScalars(entity);
        } else {
            setRandomValues(entity);
        }
        return entity;
    }

    public void runBatchPerfTestIndexed() {
        List<SimpleEntityIndexed> list = new ArrayList<>(numberEntities);
        for (int i = 0; i < numberEntities; i++) {
            list.add(createEntityIndexed(i));
        }
        startBenchmark("insert");
        realm.beginTransaction();
        realm.insert(list);
        realm.commitTransaction();
        stopBenchmark();

        for (SimpleEntityIndexed entity : list) {
            setRandomValues(entity);
        }
        startBenchmark("update");
        realm.beginTransaction();
        realm.insertOrUpdate(list);
        realm.commitTransaction();
        stopBenchmark();

        /*
        //noinspection UnusedAssignment
        list = null;

        startBenchmark("load");
        RealmResults<SimpleEntityIndexed> reloaded = realm.where(SimpleEntityIndexed.class).findAll();
        stopBenchmark();

        startBenchmark("access");
        accessAllIndexed(reloaded);
        stopBenchmark();

        startBenchmark("delete");
        realm.beginTransaction();
        reloaded.deleteAllFromRealm();
        realm.commitTransaction();
        stopBenchmark();

         */
    }

    protected void setRandomValues(SimpleEntityIndexed entity) {
        entity.setSimpleBoolean(random.nextBoolean());
        entity.setSimpleByte((byte) random.nextInt());
        entity.setSimpleShort((short) random.nextInt());
        entity.setSimpleInt(random.nextInt());
        entity.setSimpleLong(random.nextLong());
        entity.setSimpleDouble(random.nextDouble());
        entity.setSimpleFloat(random.nextFloat());
        entity.setSimpleString(randomString());
        entity.setSimpleByteArray(randomBytes());
    }

    public SimpleEntityIndexed createEntityIndexed(long id) {
        SimpleEntityIndexed entity = new SimpleEntityIndexed();
        entity.setId(id);
        setRandomValues(entity);
        return entity;
    }

    private void runQueryByString() {
        String s = Objects.requireNonNull(realm.where(SimpleEntity.class).equalTo("id", 1).findFirst()).getSimpleString();

        startBenchmark("query");
        long entitiesFound = 0;
        for (int i = 0; i < numberEntities; i++) {
            List<SimpleEntity> result = realm.where(SimpleEntity.class).equalTo("simpleString", s).findAll();
            accessAll(result);
            entitiesFound += result.size();
        }
        stopBenchmark();
        log("Entities found: " + entitiesFound);
    }

    private void runQueryByStringIndexed() {
        String s = Objects.requireNonNull(realm.where(SimpleEntityIndexed.class).equalTo("id", 1).findFirst()).getSimpleString();

        startBenchmark("query");
        long entitiesFound = 0;
        for (int i = 0; i < numberEntities; i++) {
            List<SimpleEntityIndexed> result = realm.where(SimpleEntityIndexed.class).equalTo("simpleString", s).findAll();
            accessAllIndexed(result);
            entitiesFound += result.size();
        }
        stopBenchmark();
        log("Entities found: " + entitiesFound);
    }

    private void runQueryById() {
        int i = random.nextInt((int) realm.where(SimpleEntity.class).count());

        startBenchmark("query");
        SimpleEntity entity = realm.where(SimpleEntity.class).equalTo("id", i).findFirst();
        assert entity != null;
        accessAll(entity);
        stopBenchmark();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void accessAll(SimpleEntity entity) {
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

    @Override
    public void tearDown() {
        //RealmConfiguration configuration = realm.getConfiguration();
        realm.close();
        //Realm.deleteRealm(configuration);
    }

}
