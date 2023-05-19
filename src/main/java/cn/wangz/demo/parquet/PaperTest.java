package cn.wangz.demo.parquet;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.example.Paper;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.BloomFilterReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.parquet.column.ValuesType.DEFINITION_LEVEL;
import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;
import static org.apache.parquet.column.ValuesType.VALUES;

public class PaperTest {
    public static void main(String[] args) throws IOException {

        String file = "paper.parquet";
        Path path = new Path(file);
        Configuration conf = new Configuration();
        MessageType schema = Paper.schema;

        // write file
        ParquetWriter<Group> writer = ExampleParquetWriter
                .builder(path)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(CompressionCodecName.GZIP)
                // .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .enableDictionaryEncoding()
                .withBloomFilterEnabled(true)
                .withType(schema)
                .build();
        writer.write(Paper.r1);
        writer.write(Paper.r2);
        for (int i = 0; i < 100; i++) {
            writer.write(Paper.r2);
        }
        writer.close();

        // read file
        ParquetReader<Group> reader = ParquetReader
                .builder(new GroupReadSupport(), path)
                .build();
        Group group = reader.read();
        System.out.println("-- paper data --");
        System.out.println(group);

        // read footer
        ParquetMetadata footer = ParquetFileReader.readFooter(conf, path);
        // fileMetaData
        System.out.println("-- fileMetaData --");
        FileMetaData fileMetaData = footer.getFileMetaData();
        System.out.println(fileMetaData.getSchema());
        System.out.println(fileMetaData.getCreatedBy());
        System.out.println(fileMetaData.getFileDecryptor());
        System.out.println(fileMetaData.getEncryptionType());
        System.out.println(fileMetaData.getKeyValueMetaData());
        // blockMetaData: rowGroup metadata
        System.out.println("-- blockMetaData: rowGroup metadata --");
        List<BlockMetaData> blocksMetaData = footer.getBlocks();
        BlockMetaData blockMetaData = blocksMetaData.get(0);
        System.out.println(blockMetaData.getRowCount());
        System.out.println(blockMetaData.getPath());
        System.out.println(blockMetaData.getStartingPos());
        System.out.println(blockMetaData.getRowIndexOffset());
        System.out.println(blockMetaData.getCompressedSize());
        System.out.println(blockMetaData.getTotalByteSize());
        System.out.println(blockMetaData.getOrdinal());
        // columnMetaData
        List<ColumnChunkMetaData> columnsMetaData = blockMetaData.getColumns();
        for (ColumnChunkMetaData columnMetaData: columnsMetaData) {
            System.out.println("-- columnMetaData --");
            System.out.println(columnMetaData.getPath());  // column name
            System.out.println(columnMetaData.getStatistics());  // statistics: min/max/num_nulls
            System.out.println(columnMetaData.getCodec());
            System.out.println("BloomFilterOffset: " + columnMetaData.getBloomFilterOffset());
            System.out.println("ColumnIndexReference: " + columnMetaData.getColumnIndexReference());
            System.out.println("dictionaryPageOffset:" + columnMetaData.getDictionaryPageOffset());
            System.out.println("hasDictionaryPage:" + columnMetaData.hasDictionaryPage());
            System.out.println(columnMetaData.getEncodings());
            System.out.println(columnMetaData.getEncodingStats());
            System.out.println(columnMetaData.getPrimitiveType());
            System.out.println(columnMetaData.getTotalSize());
            System.out.println(columnMetaData.getTotalUncompressedSize());
            System.out.println(columnMetaData.getValueCount());
        }


        ParquetFileReader parquetFileReader = ParquetFileReader.open(conf, path);

        // first block bloom filter
        BloomFilterReader bloomFilterReader = parquetFileReader.getBloomFilterDataReader(0);
        parquetFileReader.getFooter().getBlocks().get(0).getColumns().stream().forEach(columnChunkMetaData -> {
            if (columnChunkMetaData.getBloomFilterOffset() < 0) {
                return;
            }
            System.out.println("-- bloom filter: " + columnChunkMetaData.getPath().toDotString() + " --");
            BloomFilter bloomFilter = bloomFilterReader.readBloomFilter(columnChunkMetaData);
            ByteArrayOutputStream tempOutStream = new ByteArrayOutputStream();
            try {
                bloomFilter.writeTo(tempOutStream);
                byte[] serializedBitset = tempOutStream.toByteArray();
                // System.out.println(Arrays.toString(serializedBitset));
                System.out.println(serializedBitset.length);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        // first rowGroup
        DictionaryPageReadStore nextDictionaryReader = parquetFileReader.getNextDictionaryReader();
        PageReadStore pageReadStore = parquetFileReader.readNextRowGroup();

        // read column with ColumnReadStore
        System.out.println("-- column0 --");
        ColumnReadStoreImpl crStore = new ColumnReadStoreImpl(pageReadStore, new DummyGroupConverter(), schema, null);
        ColumnDescriptor column0 = schema.getColumns().get(0);
        switch (column0.getPrimitiveType().getPrimitiveTypeName()) {
            case INT64:
                System.out.println(crStore.getColumnReader(column0).getLong());
                break;
            case BINARY:
                System.out.println(new String(crStore.getColumnReader(column0).getBinary().getBytes()));
                break;
            default:
                throw new RuntimeException("Unsupported, type: " + column0.getPrimitiveType().getPrimitiveTypeName());
        }

        // read columns with page reader
        System.out.println("-- columns --");
        for (ColumnDescriptor column: schema.getColumns()) {
            System.out.println(Arrays.toString(column.getPath()));
            System.out.println(column.getPrimitiveType());
            System.out.println(column.getMaxDefinitionLevel());
            System.out.println(column.getMaxRepetitionLevel());
            PageReader pageReader = pageReadStore.getPageReader(column);

            // read dictionary
            DictionaryPage dictionaryPage = nextDictionaryReader.readDictionaryPage(column);
            Dictionary dict = null;
            if (dictionaryPage != null && dictionaryPage.getBytes() != null) {
                dict = readColumnDict(dictionaryPage, column);
            }

            // read dataPage
            DataPage dataPage = pageReader.readPage();
            if (dataPage instanceof DataPageV1) {
                readColumnWithPageV1((DataPageV1) dataPage, column, dict);
            }

            if (dataPage instanceof DataPageV2) {
                readColumnWithPageV2((DataPageV2) dataPage, column, dict);
            }
        }

        // close reader
        parquetFileReader.close();
    }

    private static Dictionary readColumnDict(DictionaryPage dictionaryPage, ColumnDescriptor column) throws IOException {
        System.out.println("-- dictionaryPage --");
        Dictionary dict = dictionaryPage.getEncoding().initDictionary(column, dictionaryPage);
        for (int i = 0; i <= dict.getMaxId(); i++) {
            switch (column.getPrimitiveType().getPrimitiveTypeName()) {
                case INT64:
                    System.out.println(dict.decodeToLong(i));
                    break;
                case BINARY:
                    System.out.println(new String(dict.decodeToBinary(i).getBytes()));
                    break;
                default:
                    throw new RuntimeException("Unsupported, type: " + column.getPrimitiveType().getPrimitiveTypeName());
            }
        }
        return dict;
    }

    private static void readColumnWithPageV1(DataPageV1 dataPageV1, ColumnDescriptor column, Dictionary dict) throws IOException {
            System.out.println("-- dataPageV1 --");
            // System.out.println(new String(dataPageV1.getBytes().toByteArray()));
            ValuesReader rlReader = dataPageV1.getRlEncoding().getValuesReader(column, REPETITION_LEVEL);
            ValuesReader dlReader = dataPageV1.getDlEncoding().getValuesReader(column, DEFINITION_LEVEL);
            ValuesReader dataReader;
            if (dataPageV1.getValueEncoding().usesDictionary()) {
                dataReader = dataPageV1.getValueEncoding().getDictionaryBasedValuesReader(column, VALUES, dict);
            } else {
                dataReader = dataPageV1.getValueEncoding().getValuesReader(column, VALUES);
            }

            int valueCount = dataPageV1.getValueCount();
            BytesInput bytes = dataPageV1.getBytes();
            ByteBufferInputStream in = bytes.toInputStream();

            rlReader.initFromPage(valueCount, in);
            dlReader.initFromPage(valueCount, in);
            dataReader.initFromPage(valueCount, in);

            // first column, schema:(required int64 DocId), first value: 10
            int row = 0;
            for (int i = 0; i < valueCount; i++) {
                int rl = rlReader.readInteger();
                int dl = dlReader.readInteger();
                System.out.println(rl);
                System.out.println(dl);

                if (rl == 0) { // new row
                    row++;
                    System.out.println("-- row: " +row + " --");
                }

                if (dl < column.getMaxDefinitionLevel()) { // null value
                    System.out.println(rl + "\t" + dl + "\t" + "null");
                } else {
                    switch (column.getPrimitiveType().getPrimitiveTypeName()) {
                        case INT64:
                            System.out.println(rl + "\t" + dl + "\t" + dataReader.readLong());
                            break;
                        case BINARY:
                            System.out.println(rl + "\t" + dl + "\t" + new String(dataReader.readBytes().getBytes()));
                            break;
                        default:
                            throw new RuntimeException("Unsupported, type: " + column.getPrimitiveType().getPrimitiveTypeName());
                    }
                }
            }
    }

    private static void readColumnWithPageV2(DataPageV2 dataPageV2, ColumnDescriptor column, Dictionary dict) throws IOException {
        System.out.println("-- dataPageV2 --");
        // System.out.println(new String(dataPageV1.getBytes().toByteArray()));

        RunLengthBitPackingHybridDecoder rlReader = new RunLengthBitPackingHybridDecoder(
                BytesUtils.getWidthFromMaxInt(column.getMaxRepetitionLevel()),
                dataPageV2.getRepetitionLevels().toInputStream());
        RunLengthBitPackingHybridDecoder dlReader = new RunLengthBitPackingHybridDecoder(
                BytesUtils.getWidthFromMaxInt(column.getMaxDefinitionLevel()),
                dataPageV2.getDefinitionLevels().toInputStream());

        ValuesReader dataReader;
        if (dataPageV2.getDataEncoding().usesDictionary()) {
            dataReader = dataPageV2.getDataEncoding().getDictionaryBasedValuesReader(column, VALUES, dict);
        } else {
            dataReader = dataPageV2.getDataEncoding().getValuesReader(column, VALUES);
        }

        int valueCount = dataPageV2.getValueCount();
        BytesInput bytes = dataPageV2.getData();
        ByteBufferInputStream in = bytes.toInputStream();

        dataReader.initFromPage(valueCount, in);

        // read column
        int row = 0;
        for (int i = 0; i < valueCount; i++) {
            int rl;
            if (column.getMaxRepetitionLevel() == 0) {
                rl = 0;
            } else {
                rl = rlReader.readInt();
            }
            int dl;
            if (column.getMaxDefinitionLevel() == 0) {
                dl = 0;
            } else {
                dl = dlReader.readInt();
            }
            System.out.println(rl);
            System.out.println(dl);

            if (rl == 0) { // new row
                row++;
                System.out.println("-- row: " +row + " --");
            }

            if (dl < column.getMaxDefinitionLevel()) { // null value
                System.out.println(rl + "\t" + dl + "\t" + "null");
            } else {
                switch (column.getPrimitiveType().getPrimitiveTypeName()) {
                    case INT64:
                        System.out.println(rl + "\t" + dl + "\t" + dataReader.readLong());
                        break;
                    case BINARY:
                        System.out.println(rl + "\t" + dl + "\t" + new String(dataReader.readBytes().getBytes()));
                        break;
                    default:
                        throw new RuntimeException("Unsupported, type: " + column.getPrimitiveType().getPrimitiveTypeName());
                }
            }
        }
    }


    private static final class DummyGroupConverter extends GroupConverter {
        @Override
        public void start() {
        }

        @Override
        public void end() {
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return new DummyConverter();
        }
    }

    private static final class DummyConverter extends PrimitiveConverter {
        @Override
        public GroupConverter asGroupConverter() {
            return new DummyGroupConverter();
        }
    }

}
