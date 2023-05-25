package cn.wangz.demo.parquet.user;

import cn.wangz.demo.parquet.user.avro.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

public class UserFilterTests {

    public static void main(String[] args) throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        // generate test_user.parquet
        Path path = new Path("test_user.parquet");
        Configuration conf = new Configuration();
        UserWriter writer = new UserWriter(path, 100000);
        writer.sort(Arrays.asList("id", "name", "age"));
        writer.write();
        writer.close();

        ParquetMetadata footer = ParquetFileReader.readFooter(conf, path);
        List<BlockMetaData> blockMetaDatas = footer.getBlocks();
        System.out.println("block size: " + blockMetaDatas.size());

        for (int i = 0; i < blockMetaDatas.size(); i++) {
            System.out.println("------ Block[" + i + "] ------");
            BlockMetaData blockMetaData = blockMetaDatas.get(i);
            System.out.println("row count: " + blockMetaData.getRowCount());

            List<ColumnChunkMetaData> columnChunkMetaDatas = blockMetaData.getColumns();
            for (int j = 0; j < columnChunkMetaDatas.size(); j++) {
                ColumnChunkMetaData columnChunkMetaData = columnChunkMetaDatas.get(j);
                System.out.println("------ Column[" + j + "](" + columnChunkMetaData.getPath().toDotString() + ") ------");
                System.out.println("Statistics: " + columnChunkMetaData.getStatistics());
                System.out.println("DictionaryPageOffset: " + columnChunkMetaData.getDictionaryPageOffset());
                System.out.println("BloomFilterOffset: " + columnChunkMetaData.getBloomFilterOffset());
                System.out.println("ColumnIndexReferenceOffset: " + columnChunkMetaData.getColumnIndexReference().getOffset());
                System.out.println("OffsetIndexReferenceOffset: " + columnChunkMetaData.getOffsetIndexReference().getOffset());
            }
        }

        // id = 100
        Operators.LongColumn idCol = FilterApi.longColumn("id");
        ParquetReader<User> reader = AvroParquetReader
                .<User>builder(path)
                .withFilter(FilterCompat.get(FilterApi.eq(idCol, 100L)))
                .build();
        User user = reader.read();
        System.out.println(user.toString());
        reader.close();

        // id < 100
        HadoopInputFile inputFile = HadoopInputFile.fromPath(path, conf);
        ParquetReadOptions readOptions = ParquetReadOptions.builder()
                .withRecordFilter(FilterCompat.get(FilterApi.and(FilterApi.gt(idCol, 1500L), FilterApi.ltEq(idCol, 2000L))))
                .useColumnIndexFilter()
                .build();
        ParquetFileReader fileReader = ParquetFileReader.open(inputFile, readOptions);
        System.out.println("Filter blocks: " + fileReader.getRowGroups().size());
        Method getRowRangesMethod = ParquetFileReader.class.getDeclaredMethod("getRowRanges", int.class);
        getRowRangesMethod.setAccessible(true);
        for (int i = 0; i < fileReader.getRowGroups().size(); i++) {
            ColumnChunkMetaData idColumnChunkMetaData = fileReader.getRowGroups().get(i).getColumns()
                    .stream().filter(c -> c.getPath().toDotString().equals("id")).findFirst().get();
            System.out.println("------------- Block[" + i + "] -----------");
            System.out.println("Block[" + i + "] Column[id] Statistics: " + idColumnChunkMetaData.getStatistics());
            ColumnIndexStore columnIndexStore = fileReader.getColumnIndexStore(i);
            System.out.println("Block[" + i + "] Column[id] ColumnIndex: \n" + columnIndexStore.getColumnIndex(ColumnPath.get("id")));
            System.out.println("Block[" + i + "] Column[id] OffsetIndex: \n" + columnIndexStore.getOffsetIndex(ColumnPath.get("id")));
            RowRanges rowRanges = (RowRanges) getRowRangesMethod.invoke(fileReader, i);
            System.out.println("Block[" + i + "] Column[id] rowRanges: " + rowRanges);
        }
        fileReader.close();

    }

}
