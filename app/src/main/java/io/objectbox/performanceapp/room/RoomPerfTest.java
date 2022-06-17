package io.objectbox.performanceapp.room;

import androidx.room.Room;
import android.content.Context;
import android.database.Cursor;

import java.util.ArrayList;
import java.util.List;

import io.objectbox.performanceapp.PerfTest;
import io.objectbox.performanceapp.PerfTestRunner;
import io.objectbox.performanceapp.TestType;

public class RoomPerfTest extends PerfTest {

    public static final String DB_NAME = "sqlite-room";

    private boolean versionLoggedOnce;
    private AppDatabase db;
    private SimpleEntityDao dao;
    private SimpleEntityIndexedDao daoIndexed;

    @Override
    public String name() {
        return "Room";
    }

    @Override
    public void setUp(Context context, PerfTestRunner testRunner) {
        super.setUp(context, testRunner);
        db = Room.databaseBuilder(context.getApplicationContext(), AppDatabase.class, DB_NAME)
                .build();
        dao = db.simpleEntityDao();
        daoIndexed = db.simpleEntityIndexedDao();

        if (!versionLoggedOnce) {
            try (Cursor cursor = db.query("select sqlite_version() AS sqlite_version", null)) {
                if (cursor.moveToFirst()) {
                    log("SQLite version " + cursor.getString(0));
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
                runCRUDTestIndexed();
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
        int existentEntities = dao.count();
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
        int existentEntities = daoIndexed.loadAll().size();
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

    private void runCRUDTest() {
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
    }

    private void runCRUDTestIndexed() {
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
    }

    private void runQueryByString() {
        String s = dao.load(0).getSimpleString();

        startBenchmark("query");
        long entitiesFound = db.runInTransaction(() -> {
                List<SimpleEntity> result = dao.whereSimpleStringEq(s);
                accessAll(result);
                return result.size();
        });
        stopBenchmark();
        log("Entities found: " + entitiesFound);
    }

    private void runQueryByStringIndexed() {
        String s = daoIndexed.loadAll().get(0).getSimpleString();

        startBenchmark("query");
        long entitiesFound = db.runInTransaction(() -> {
                List<SimpleEntityIndexed> result = daoIndexed.whereSimpleStringEq(s);
                accessAllIndexed(result);
                return result.size();
        });
        stopBenchmark();
        log("Entities found: " + entitiesFound);
    }

    private void runQueryByInteger() {
        int i = dao.load(1).getSimpleInt();

        startBenchmark("query");
        List<SimpleEntity> result = dao.whereSimpleIntEq(i);
        accessAll(result);

        stopBenchmark();
        log("Entities found: " + result.size());
    }

    private void runQueryByIntegerIndexed() {
        int i = dao.load(1).getSimpleInt();

        startBenchmark("query");
        List<SimpleEntityIndexed> result = daoIndexed.whereSimpleIntEq(i);
        accessAllIndexed(result);

        stopBenchmark();
        log("Entities found: " + result.size());
    }

    private void runQueryById() {
        int i = random.nextInt((int) dao.count()) ;

        startBenchmark("query");
        SimpleEntity entity = dao.load(i);
        accessAll(entity);

        stopBenchmark();
    }

    @Override
    public void tearDown() {
        super.tearDown();
        db.close();
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

    private void setRandomValues(SimpleEntity entity) {
        setRandomScalars(entity);
        entity.setSimpleString(randomString());
        entity.setSimpleByteArray(randomBytes());
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

    private void setRandomScalars(SimpleEntity entity) {
        entity.setSimpleBoolean(random.nextBoolean());
        entity.setSimpleByte((byte) random.nextInt());
        entity.setSimpleShort((short) random.nextInt());
        entity.setSimpleInt(random.nextInt(1000));
        entity.setSimpleLong(random.nextLong());
        entity.setSimpleDouble(random.nextDouble());
        entity.setSimpleFloat(random.nextFloat());
    }

    private SimpleEntity createEntity(Long key) {
        SimpleEntity entity = new SimpleEntity();
        if (key != null) {
            entity.setId(key);
        }
        setRandomValues(entity);

        return entity;
    }

    public SimpleEntityIndexed createEntityIndexed(Long key) {
        SimpleEntityIndexed entity = new SimpleEntityIndexed();
        if (key != null) {
            entity.setId(key);
        }
        setRandomValues(entity);
        return entity;
    }
}
