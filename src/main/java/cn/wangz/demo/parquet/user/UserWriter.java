package cn.wangz.demo.parquet.user;

import cn.wangz.demo.parquet.user.avro.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

public class UserWriter {

    private Path path;
    private long length;
    private UserGenerator generator;
    private ParquetWriter<User> writer;

    public UserWriter(Path path, long length) throws IOException {
        this.path = path;
        this.length = length;
        this.generator = new UserGenerator();
        this.writer = AvroParquetWriter
                .<User>builder(HadoopOutputFile.fromPath(path, new Configuration()))
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(CompressionCodecName.GZIP)
                .withRowGroupSize(256 * 1024L)
                .withDictionaryPageSize(8 * 1024)
                .withPageSize(8 * 1024)
                // .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .enableDictionaryEncoding()
                .withBloomFilterEnabled(true)
                .withSchema(User.getClassSchema())
                .build();
    }

    private List<String> sortFields = Collections.emptyList();
    public void sort(List<String> fields) {
        this.sortFields = fields;
    }

    private Stream<User> data = Stream.empty();
    private volatile Boolean generated = false;
    private void generate() {
        if (!generated) {
            synchronized (this) {
                if (!generated) {
                    List<User> list = new ArrayList<>();
                    for (int i = 0; i < length; i++) {
                        User user = generator.generate();
                        list.add(user);
                    }
                    if (sortFields.isEmpty()) {
                        data = list.stream().sorted((u1, u2) -> {
                            int v;
                            int i = 0;
                            do {
                                String field = sortFields.get(i++);
                                switch (field.toLowerCase(Locale.ROOT)) {
                                    case "id":
                                        v = Long.compare(u1.getId(), u2.getId());
                                        break;
                                    case "name":
                                        v = String.valueOf(u1.getName()).compareTo(String.valueOf(u2.getName()));
                                        break;
                                    case "age":
                                        v = Long.compare(u1.getAge(), u2.getAge());
                                        break;
                                    default:
                                        throw new RuntimeException("Invalid sort field: " + field);
                                }
                            } while (v == 0);
                            return v;
                        });
                    } else {
                        data = list.stream();
                    }
                    generated = true;
                }
            }
        }
    }

    public void write() {
        generate();
        data.forEach(u -> {
            try {
                writer.write(u);
            } catch (IOException e) {
                throw new RuntimeException("write data failed.", e);
            }
        });
    }

    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }

    public static void main(String[] args) throws IOException {
        Path path = new Path("test_user.parquet");
        UserWriter writer = new UserWriter(path, 1000);
        writer.sort(Arrays.asList("id"));
        writer.write();
        writer.close();

        ParquetReader<User> reader = AvroParquetReader
                .<User>builder(HadoopInputFile.fromPath(path, new Configuration()))
                .build();
        User user = reader.read();
        System.out.println(user.toString());
        reader.close();
    }
}
