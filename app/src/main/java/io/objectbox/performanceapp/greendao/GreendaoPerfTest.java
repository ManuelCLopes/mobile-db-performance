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

package io.objectbox.performanceapp.greendao;

import android.content.Context;
import android.database.Cursor;

import org.greenrobot.greendao.database.Database;
import org.greenrobot.greendao.identityscope.IdentityScopeType;
import org.greenrobot.greendao.query.Query;

import java.util.ArrayList;
import java.util.List;

import io.objectbox.performanceapp.PerfTest;
import io.objectbox.performanceapp.PerfTestRunner;
import io.objectbox.performanceapp.TestType;
import io.objectbox.performanceapp.greendao.DaoMaster.DevOpenHelper;
import io.objectbox.performanceapp.greendao.SimpleEntityIndexedDao.Properties;

public class GreendaoPerfTest extends PerfTest {
    public static final String DB_NAME = "sqlite-greendao";

    private Database db;
    private DaoSession daoSession;
    private SimpleEntityDao dao;

    private boolean versionLoggedOnce;
    private SimpleEntityIndexedDao daoIndexed;

    @Override
    public String name() {
        return "greenDAO";
    }

    public void setUp(Context context, PerfTestRunner testRunner) {
        super.setUp(context, testRunner);

        db = new DevOpenHelper(context, DB_NAME).getWritableDb();
        daoSession = new DaoMaster(db).newSession(IdentityScopeType.None);
        dao = daoSession.getSimpleEntityDao();
        daoIndexed = daoSession.getSimpleEntityIndexedDao();

        if (!versionLoggedOnce) {
            Cursor cursor = db.rawQuery("select sqlite_version() AS sqlite_version", null);
            if (cursor != null) {
                try {
                    if (cursor.moveToFirst()) {
                        log("SQLite version " + cursor.getString(0));
                    }
                } finally {
                    cursor.close();
                }
            }
            versionLoggedOnce = true;
        }
    }

    @Override
    public void run(TestType type) {
        switch (type.name) {
            case TestType.CREATE_UPDATE:
                runCreateUpdateTest();
                break;
            case TestType.CREATE_UPDATE_INDEXED:
                runCreateUpdateIndexedTest();
                break;
            case TestType.CRUD:
                runCRUDTest();
                break;
            case TestType.CRUD_INDEXED:
                runCRUDIndexed();
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

    private void runDeleteAll() {
        startBenchmark("load");
        List<SimpleEntity> loaded = dao.loadAll();
        stopBenchmark();

        startBenchmark("delete");
        dao.deleteInTx(loaded);
        stopBenchmark();

        startBenchmark("load indexed");
        List<SimpleEntityIndexed> indexedLoaded = daoIndexed.loadAll();
        stopBenchmark();

        startBenchmark("delete indexed");
        daoIndexed.deleteInTx(indexedLoaded);
        stopBenchmark();

        boolean deleted = context.deleteDatabase(DB_NAME);
        log("DB deleted: " + deleted);
    }

    private void runCreateUpdateTest() {
        int existentEntities = (int) dao.count();
        List<SimpleEntity> list = new ArrayList<>(numberEntities);
        for (int i = existentEntities; i < existentEntities + numberEntities; i++) {
            list.add(createEntity((long) i));
        }
        startBenchmark("insert");
        dao.insertInTx(list);
        stopBenchmark();

        for (SimpleEntity entity : list) {
            setRandomValues(entity);
        }
        startBenchmark("update");
        dao.updateInTx(list);
        stopBenchmark();
    }

    private void runCreateUpdateIndexedTest(){
        int existentEntities = (int) daoIndexed.count();
        List<SimpleEntityIndexed> list = new ArrayList<>(numberEntities);
        for (int i = existentEntities; i < existentEntities + numberEntities; i++) {
            list.add(createEntityIndexed((long) i));
        }
        startBenchmark("insert");
        daoIndexed.insertInTx(list);
        stopBenchmark();

        for (SimpleEntityIndexed entity : list) {
            setRandomValues(entity);
        }
        startBenchmark("update");
        daoIndexed.updateInTx(list);
        stopBenchmark();
    }

    public void runCRUDTest() {
        runCreateUpdateTest();

        startBenchmark("load");
        List<SimpleEntity> reloaded = dao.loadAll();
        stopBenchmark();

        startBenchmark("access");
        accessAll(reloaded);
        stopBenchmark();

        startBenchmark("delete");
        dao.deleteInTx(reloaded);
        stopBenchmark();

        boolean deleted = context.deleteDatabase(DB_NAME);
        log("DB deleted: " + deleted);
    }

    public void runCRUDIndexed() {
        runCreateUpdateIndexedTest();

        startBenchmark("load");
        List<SimpleEntityIndexed> reloaded = daoIndexed.loadAll();
        stopBenchmark();

        startBenchmark("access");
        accessAllIndexed(reloaded);
        stopBenchmark();

        startBenchmark("delete");
        daoIndexed.deleteInTx(reloaded);
        stopBenchmark();

        boolean deleted = context.deleteDatabase(DB_NAME);
        log("DB deleted: " + deleted);
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

    public SimpleEntity createEntity(Long key) {
        SimpleEntity entity = new SimpleEntity();
        if (key != null) {
            entity.setId(key);
        }
        setRandomValues(entity);

        return entity;
    }

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

    public SimpleEntityIndexed createEntityIndexed(Long key) {
        SimpleEntityIndexed entity = new SimpleEntityIndexed();
        if (key != null) {
            entity.setId(key);
        }
        setRandomValues(entity);
        return entity;
    }

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
        String s = dao.queryBuilder().limit(1).build().list().get(0).getSimpleString();

        startBenchmark("query");
        Query<SimpleEntity> query = dao.queryBuilder().where(SimpleEntityDao.Properties.SimpleString.eq(s)).build();
        db.beginTransaction();
        List<SimpleEntity> result = query.list();
        accessAll(result);

        db.endTransaction();
        stopBenchmark();
        log("Entities found: " + result.size());
    }

    private void runQueryByStringIndexed() {
        String s = daoIndexed.queryBuilder().limit(1).build().list().get(0).getSimpleString();

        startBenchmark("query");
        Query<SimpleEntityIndexed> query = daoIndexed.queryBuilder().where(Properties.SimpleString.eq(null)).build();
        db.beginTransaction();
        List<SimpleEntityIndexed> result = query.list();
        accessAllIndexed(result);

        db.endTransaction();
        stopBenchmark();
        log("Entities found: " + result.size());
    }

    private void runQueryByInteger(){
        int i = dao.queryBuilder().limit(1).build().list().get(0).getSimpleInt();

        startBenchmark("query");
        Query<SimpleEntity> query = dao.queryBuilder().where(SimpleEntityDao.Properties.SimpleInt.eq(i)).build();
        db.beginTransaction();
        List<SimpleEntity> result = query.list();
        accessAll(result);

        db.endTransaction();
        stopBenchmark();
        log("Entities found: " + result.size());
    }

    private void runQueryByIntegerIndexed(){
        int i = daoIndexed.queryBuilder().limit(1).build().list().get(0).getSimpleInt();

        startBenchmark("query");
        Query<SimpleEntityIndexed> query = daoIndexed.queryBuilder().where(SimpleEntityIndexedDao.Properties.SimpleInt.eq(i)).build();
        db.beginTransaction();
        List<SimpleEntityIndexed> result = query.list();
        accessAllIndexed(result);

        db.endTransaction();
        stopBenchmark();
        log("Entities found: " + result.size());
    }

    private void runQueryById() {
        long i = random.nextInt((int) dao.count());

        startBenchmark("query");
        SimpleEntity entity = dao.load(i);
        accessAll(entity);

        stopBenchmark();
    }

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

    @Override
    public void tearDown() {
        daoSession.getDatabase().close();
    }
}
