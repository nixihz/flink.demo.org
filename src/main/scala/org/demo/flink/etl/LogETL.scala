package org.demo.flink.etl

import java.time.ZoneId

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}

object LogETL {
    def main(args: Array[String]) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 使用 socket 模拟 kafka 接受数据
        val text = env.socketTextStream("localhost", 9999)


        // 方便起见，写入本地文件夹文件夹
        val bucketingSink = new BucketingSink[String]("/tmp/data")

        // 设置以yyyyMMdd的格式进行切分目录，类似hive的日期分区
        bucketingSink.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd", ZoneId.of("Asia/Shanghai")))
        // 设置文件块大小64kb，超过64kb会关闭当前文件，开启下一个文件
        bucketingSink.setBatchSize(64 * 1024L);
        // 设置1分钟翻滚一次
        bucketingSink.setBatchRolloverInterval(1 * 60 * 1000L);
        // 文件前缀
        bucketingSink.setPartPrefix("node")
        // 设置等待写入的文件前缀，默认是_
        bucketingSink.setPendingPrefix("");
        // 设置等待写入的文件后缀，默认是.pending
        bucketingSink.setPendingSuffix("");
        //设置正在处理的文件前缀，默认为_
        bucketingSink.setInProgressPrefix(".");

        text.addSink(bucketingSink)

        // execute program
        env.execute("Flink Streaming storage")
    }
}
