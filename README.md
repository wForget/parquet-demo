## Apache Parquet

GitHub: [https://github.com/apache/parquet-mr/](https://github.com/apache/parquet-mr/)

Documentation: [https://parquet.apache.org/docs/](https://parquet.apache.org/docs/)

## 技术细节

+ Encodings: [Parquet encoding definitions](https://github.com/apache/parquet-format/blob/master/Encodings.md)
+ BloomFilter: [Parquet Bloom Filter](https://github.com/apache/parquet-format/blob/master/BloomFilter.md)
+ PageIndex (ColumnIndex): [ColumnIndex Layout to Support Page Skipping](https://github.com/apache/parquet-format/blob/master/PageIndex.md)

## Parquet 学习 Demo

### Paper

使用 Parquet 代码里面定义的 Paper 实例类型，写如 Parquet 文件，并读取 Parquet 文件内容和各层级元数据。

Demo: [PaperTest.java](src/main/java/cn/wangz/demo/parquet/PaperTest.java)

### User

引入 parquet-avro 模块，使用 Avro 序列化定义 User 类型: [user.avsc](src/main/avro/user.avsc)

[User Demo](src/main/java/cn/wangz/demo/parquet/user): 生成 User 对象写入 Parquet 文件，进行各种 Filter 类型测试，学习技术细节



