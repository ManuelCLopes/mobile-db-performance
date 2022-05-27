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
        boolean deleted = context.deleteDatabase(DB_NAME);
        if (deleted) {
            log("DB existed before start - deleted");
        }
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
                runCreateUpdateTest(false);
                break;
            case TestType.CREATE_UPDATE_SCALARS:
                runCreateUpdateTest(true);
                break;
            case TestType.CREATE_UPDATE_INDEXED:
                runCreateUpdateIndexedTest();
                break;
            case TestType.CRUD:
                runCRUDTest(false);
                break;
            case TestType.CRUD_SCALARS:
                runCRUDTest(true);
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

    private void runCreateUpdateTest(boolean scalarsOnly) {
        List<SimpleEntity> list = new ArrayList<>(numberEntities);
        for (int i = 0; i < numberEntities; i++) {
            list.add(createEntity((long) i, scalarsOnly));
        }
        startBenchmark("insert");
        dao.insertInTx(list);
        stopBenchmark();

        for (SimpleEntity entity : list) {
            if (scalarsOnly) {
                setRandomScalars(entity);
            } else {
                setRandomValues(entity);
            }
        }
        startBenchmark("update");
        dao.updateInTx(list);
        stopBenchmark();
    }

    private void runCreateUpdateIndexedTest(){
        List<SimpleEntityIndexed> list = new ArrayList<>(numberEntities);
        for (int i = 0; i < numberEntities; i++) {
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

    private void runCRUDTest(boolean scalarsOnly) {
        runCreateUpdateTest(scalarsOnly);

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
        String s = dao.load(1).getSimpleString();

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
        String s = "a";

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
        int i = 5;

        startBenchmark("query");
        List<SimpleEntityIndexed> result = daoIndexed.whereSimpleIntEq(5);
        accessAllIndexed(result);

        stopBenchmark();
        log("Entities found: " + result.size());
        assertGreaterOrEqualToNumberOfEntities(result.size());
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
        entity.setSimpleInt(random.nextInt());
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
        entity.setSimpleInt(random.nextInt());
        entity.setSimpleLong(random.nextLong());
        entity.setSimpleDouble(random.nextDouble());
        entity.setSimpleFloat(random.nextFloat());
    }

    private SimpleEntity createEntity(Long key, boolean scalarsOnly) {
        SimpleEntity entity = new SimpleEntity();
        if (key != null) {
            entity.setId(key);
        }
        if (scalarsOnly) {
            setRandomScalars(entity);
        } else {
            setRandomValues(entity);
        }
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
