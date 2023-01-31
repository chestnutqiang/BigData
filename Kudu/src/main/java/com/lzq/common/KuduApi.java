package com.lzq.common;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.client.SessionConfiguration;
import org.apache.kudu.client.Upsert;
import org.apache.kudu.Type;
import org.apache.kudu.Schema;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author wujuan
 * @version 1.0
 * @date 2023/1/31 21:50 星期二
 * @email wujuan@dtstack.com
 * @company www.dtstack.com
 */
public class KuduApi {
    // 声明全局变量 KuduClient后期通过它来操作kudu表
    private KuduClient kuduClient;
    // 指定kuduMaster地址
    private String kuduMaster;
    // 指定表名
    private String tableName;

    @Before
    public void init() {
        // 初始化操作
        kuduMaster = "172.16.100.109:7051";
        // 指定表名
        // tableName = "flinkx_kudu_dest_01_0000003";
        // tableName = "wujuan_kudu_1";
        // tableName = "wujuan_kudu_2";
        // tableName = "flinkx_kudu_dest_01_78708";
        tableName = "flinkx_kudu_0000003";
        KuduClient.KuduClientBuilder kuduClientBuilder =
                new KuduClient.KuduClientBuilder(kuduMaster);
        kuduClientBuilder.defaultSocketReadTimeoutMs(10000);
        kuduClient = kuduClientBuilder.build();
    }

    /** 创建表 */
    @Test
    public void createTable() throws KuduException {

        LinkedBlockingDeque<Integer> linkedBlockingDeque = new LinkedBlockingDeque(50);

        for (int i = 0; i < 10; i++) {
            try {
                linkedBlockingDeque.put(i);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("============================");
        // 判断表是否存在，不存在就构建
        if (!kuduClient.tableExists(tableName)) {

            // 构建创建表的schema信息-----就是表的字段和类型
            ArrayList<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();
            columnSchemas.add(
                    new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());

            // columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("age",
            // Type.INT32).nullable(true).build());
            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("age", Type.INT32).build());

            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING).build());
            Schema schema = new Schema(columnSchemas);

            // 指定创建表的相关属性
            CreateTableOptions options = new CreateTableOptions();
            ArrayList<String> partitionList = new ArrayList<String>();
            // 指定kudu表的分区字段是什么
            partitionList.add("id"); //  按照 id.hashcode % 分区数 = 分区号
            options.addHashPartitions(partitionList, 16);

            kuduClient.createTable(tableName, schema, options);
        }
    }

    /**
     * 8．删除表
     *
     * @throws KuduException
     */
    @Test
    public void dropTable() throws KuduException {

        if (kuduClient.tableExists(tableName)) {
            kuduClient.deleteTable(tableName);
        }
    }

    /** 创建表 */
    @Test
    public void showTableSchema() throws KuduException {

        // ListTablesResponse listTablesResponse = kuduClient.getTablesList();
        // List<String> tables = listTablesResponse.getTablesList();
        // tables.forEach(System.out::println);
        KuduTable table = kuduClient.openTable(tableName);
        int columnCount = table.getSchema().getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            System.out.println(
                    table.getSchema().getColumnByIndex(i).getName()
                            + " "
                            + table.getSchema().getColumnByIndex(i).getType());
        }
    }
    /** 4．插入数据 向表加载数据 */
    @Test
    public void insertTable() throws KuduException {
        // 向表加载数据需要一个kuduSession对象
        KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);

        // 需要使用kuduTable来构建Operation的子类实例对象
        KuduTable kuduTable = kuduClient.openTable(tableName);

        for (int i = 1; i <= 2; i++) {
            Insert insert = kuduTable.newInsert();
            PartialRow row = insert.getRow();
            row.addInt("id", i);
            row.addString("name", "zhangsan-" + i);

            OperationResponse apply = kuduSession.apply(insert); // 最后实现执行数据的加载操作
            if (apply.hasRowError()) {
                RowError error = apply.getRowError();
                System.out.println(error);
                String errorMsg = error.getErrorStatus().toString();
                System.out.println(errorMsg);

                System.out.println(error.getErrorStatus().isNotFound());
                System.out.println(error.getErrorStatus().isIOError());
                System.out.println(error.getErrorStatus().isRuntimeError());
                System.out.println(error.getErrorStatus().isServiceUnavailable());
                System.out.println(error.getErrorStatus().isIllegalState());
            }
            System.out.println(apply);
        }
    }

    /** 5．查询数据 查询表的数据结果 */
    @Test
    public void queryData() throws KuduException {

        // 构建一个查询的扫描器
        KuduScanner.KuduScannerBuilder kuduScannerBuilder =
                kuduClient.newScannerBuilder(kuduClient.openTable(tableName));
        ArrayList<String> columnsList = new ArrayList<String>();
        columnsList.add("id");
        columnsList.add("name");

        kuduScannerBuilder.setProjectedColumnNames(columnsList);
        // 返回结果集
        KuduScanner kuduScanner = kuduScannerBuilder.build();
        // 遍历
        while (kuduScanner.hasMoreRows()) {
            RowResultIterator rowResults = kuduScanner.nextRows();

            while (rowResults.hasNext()) {
                RowResult row = rowResults.next();
                int id = row.getInt("id");
                String name = row.getString("name");

                System.out.println("id=" + id + "  name=" + name);
            }
        }
    }

    /** 5.1 查询数据 查询表的数据结果 */
    @Test
    public void queryDataWithFilter() throws KuduException {

        // 构建一个查询的扫描器
        // KuduScanner.KuduScannerBuilder kuduScannerBuilder =
        // kuduClient.newScannerBuilder(kuduClient.openTable(tableName));
        KuduScanner.KuduScannerBuilder kuduScannerBuilder =
                kuduClient.newScannerBuilder(kuduClient.openTable(tableName));
        ArrayList<String> columnsList = new ArrayList<>();
        columnsList.add("id");
        columnsList.add("name");

        kuduScannerBuilder.setProjectedColumnNames(columnsList);
        // 返回结果集
        KuduScanner kuduScanner = kuduScannerBuilder.build();

        ColumnSchema columnSchema = new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).build();
        // KuduPredicate.ComparisonOp op = KuduPredicate.ComparisonOp.GREATER_EQUAL; // >=
        // KuduPredicate.ComparisonOp  op = KuduPredicate.ComparisonOp.LESS_EQUAL; // <=
        KuduPredicate.ComparisonOp op = KuduPredicate.ComparisonOp.LESS; // <
        // KuduPredicate.ComparisonOp  op = KuduPredicate.ComparisonOp.EQUAL; // =
        // Object v = Timestamp.valueOf("2021-12-06 15:48:37.556");
        // Object v = Timestamp.valueOf("2021-12-06 15:48:37.556000000");

        // Object v = Timestamp.valueOf("2021-12-01 00:00:10");
        // Object v = Timestamp.valueOf("10"); // +8 小时
        Object v = Integer.valueOf("10"); // +8 小时

        // ColumnSchema columnSchema = new ColumnSchema.ColumnSchemaBuilder("t_timestamp",
        // Type.STRING).build();
        // KuduPredicate.ComparisonOp  op = KuduPredicate.ComparisonOp.GREATER_EQUAL; // >=
        // Object v = "nullable";
        KuduPredicate predicate = KuduPredicate.newComparisonPredicate(columnSchema, op, v);
        kuduScannerBuilder.addPredicate(predicate);

        // 遍历
        columnsList.forEach(
                s -> {
                    System.out.print(s);
                    System.out.print("\t");
                });
        System.out.print("\n");
        while (kuduScanner.hasMoreRows()) {
            RowResultIterator rowResults = kuduScanner.nextRows();
            while (rowResults.hasNext()) {
                RowResult row = rowResults.next();
                int id = row.getInt("id");
                Object name = row.getObject("name");
                System.out.println(id + "\t" + name);
            }
        }
    }
    /** 6．修改数据 修改表的数据 */
    @Test
    public void updateData() throws KuduException {
        // 修改表的数据需要一个kuduSession对象
        KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);

        // 需要使用kuduTable来构建Operation的子类实例对象
        KuduTable kuduTable = kuduClient.openTable(tableName);

        // Update update = kuduTable.newUpdate();
        Upsert upsert = kuduTable.newUpsert(); // 如果id存在就表示修改，不存在就新增
        PartialRow row = upsert.getRow();
        row.addInt("id", 100);
        row.addString("name", "zhangsan-100");

        kuduSession.apply(upsert); // 最后实现执行数据的修改操作
    }

    /** 7．删除数据 删除数据 */
    @Test
    public void deleteData() throws KuduException {
        // 删除表的数据需要一个kuduSession对象
        KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);

        // 需要使用kuduTable来构建Operation的子类实例对象
        KuduTable kuduTable = kuduClient.openTable(tableName);

        Delete delete = kuduTable.newDelete();
        PartialRow row = delete.getRow();
        row.addInt("id", 100);

        kuduSession.apply(delete); // 最后实现执行数据的删除操作
    }

    /**
     * 9．kudu分区方式 为了提供可扩展性，Kudu 表被划分为称为 tablet 的单元，并分布在许多 tablet servers 上。行总是属于单个tablet 。将行分配给
     * tablet 的方法由在表创建期间设置的表的分区决定。 kudu提供了3种分区方式。 9.1．Range Partitioning ( 范围分区 )
     * 范围分区可以根据存入数据的数据量，均衡的存储到各个机器上，防止机器出现负载不均衡现象. 测试分区： RangePartition
     */
    @Test
    public void testRangePartition() throws KuduException {
        // 设置表的schema
        LinkedList<ColumnSchema> columnSchemas = new LinkedList<ColumnSchema>();
        columnSchemas.add(newColumn("CompanyId", Type.INT32, true));
        columnSchemas.add(newColumn("WorkId", Type.INT32, false));
        columnSchemas.add(newColumn("Name", Type.STRING, false));
        columnSchemas.add(newColumn("Gender", Type.STRING, false));
        columnSchemas.add(newColumn("Photo", Type.STRING, false));

        // 创建schema
        Schema schema = new Schema(columnSchemas);

        // 创建表时提供的所有选项
        CreateTableOptions tableOptions = new CreateTableOptions();
        // 设置副本数
        tableOptions.setNumReplicas(1);
        // 设置范围分区的规则
        LinkedList<String> parcols = new LinkedList<String>();
        parcols.add("CompanyId");
        // 设置按照那个字段进行range分区
        tableOptions.setRangePartitionColumns(parcols);

        /** range 0 < value < 10 10 <= value < 20 20 <= value < 30 ........ 80 <= value < 90 */
        int count = 0;
        for (int i = 0; i < 10; i++) {
            // 范围开始
            PartialRow lower = schema.newPartialRow();
            lower.addInt("CompanyId", count);

            // 范围结束
            PartialRow upper = schema.newPartialRow();
            count += 10;
            upper.addInt("CompanyId", count);

            // 设置每一个分区的范围
            tableOptions.addRangePartition(lower, upper);
        }

        try {
            kuduClient.createTable("student", schema, tableOptions);
        } catch (KuduException e) {
            e.printStackTrace();
        }
        kuduClient.close();
    }

    /**
     * 9.2．Hash Partitioning ( 哈希分区 ) 哈希分区通过哈希值将行分配到许多 buckets ( 存储桶 )之一；
     * 哈希分区是一种有效的策略，当不需要对表进行有序访问时。 哈希分区对于在 tablet 之间随机散布这些功能是有效的，这有助于减轻热点和 tablet 大小不均匀。 测试分区：
     * hash分区
     */
    @Test
    public void testHashPartition() throws KuduException {
        // 设置表的schema
        LinkedList<ColumnSchema> columnSchemas = new LinkedList<ColumnSchema>();
        columnSchemas.add(newColumn("CompanyId", Type.INT32, true));
        columnSchemas.add(newColumn("WorkId", Type.INT32, false));
        columnSchemas.add(newColumn("Name", Type.STRING, false));
        columnSchemas.add(newColumn("Gender", Type.STRING, false));
        columnSchemas.add(newColumn("Photo", Type.STRING, false));

        // 创建schema
        Schema schema = new Schema(columnSchemas);

        // 创建表时提供的所有选项
        CreateTableOptions tableOptions = new CreateTableOptions();
        // 设置副本数
        tableOptions.setNumReplicas(1);
        // 设置范围分区的规则
        LinkedList<String> parcols = new LinkedList<String>();
        parcols.add("CompanyId");
        // 设置按照那个字段进行range分区
        tableOptions.addHashPartitions(parcols, 6);
        try {
            kuduClient.createTable("dog", schema, tableOptions);
        } catch (KuduException e) {
            e.printStackTrace();
        }

        kuduClient.close();
    }

    /**
     * 9.3．Multilevel Partitioning ( 多级分区 ) Kudu 允许一个表在单个表上组合多级分区。
     * 当正确使用时，多级分区可以保留各个分区类型的优点，同时减少每个分区的缺点 需求.
     *
     * <p>测试分区： 多级分区 Multilevel Partition 混合使用hash分区和range分区
     *
     * <p>哈希分区有利于提高写入数据的吞吐量，而范围分区可以避免tablet无限增长问题， hash分区和range分区结合，可以极大的提升kudu的性能
     */
    @Test
    public void testMultilevelPartition() throws KuduException {
        // 设置表的schema
        LinkedList<ColumnSchema> columnSchemas = new LinkedList<ColumnSchema>();
        columnSchemas.add(newColumn("CompanyId", Type.INT32, true));
        columnSchemas.add(newColumn("WorkId", Type.INT32, false));
        columnSchemas.add(newColumn("Name", Type.STRING, false));
        columnSchemas.add(newColumn("Gender", Type.STRING, false));
        columnSchemas.add(newColumn("Photo", Type.STRING, false));

        // 创建schema
        Schema schema = new Schema(columnSchemas);
        // 创建表时提供的所有选项
        CreateTableOptions tableOptions = new CreateTableOptions();
        // 设置副本数
        tableOptions.setNumReplicas(1);
        // 设置范围分区的规则
        LinkedList<String> parcols = new LinkedList<String>();
        parcols.add("CompanyId");

        // hash分区
        tableOptions.addHashPartitions(parcols, 5);

        // range分区
        int count = 0;
        for (int i = 0; i < 10; i++) {
            PartialRow lower = schema.newPartialRow();
            lower.addInt("CompanyId", count);
            count += 10;

            PartialRow upper = schema.newPartialRow();
            upper.addInt("CompanyId", count);
            tableOptions.addRangePartition(lower, upper);
        }

        try {
            kuduClient.createTable("cat", schema, tableOptions);
        } catch (KuduException e) {
            e.printStackTrace();
        }
        kuduClient.close();
    }

    private ColumnSchema newColumn(String name, Type type, boolean b) {
        return new ColumnSchema.ColumnSchemaBuilder(name, type).key(b).build();
    }
}
