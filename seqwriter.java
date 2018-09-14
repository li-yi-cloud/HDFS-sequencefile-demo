package main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.Writer;

import org.apache.hadoop.io.serializer.WritableSerialization;

public class seqwriter {

    private static final String[] DATA = { "One, two, buckle my shoe",
            "Three, four, shut the door", "Five, six, pick up sticks",
            "Seven, eight, lay them straight", "Nine, ten, a big fat hen" };

    public static void main(String[] args) throws IOException {
        // String uri = args[0];
        String uri = "file:///home/cloud/B.txt";
        Configuration conf = new Configuration();
        conf.set("io.serializations",
                "org.apache.hadoop.io.serializer.WritableSerialization");
//                "org.apache.hadoop.io.serializer.JavaSerialization");
        conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        Path path = new Path(uri);

        Text key1  = new Text();
        BytesWritable value1 = new BytesWritable();

        SequenceFile.Writer writer = null;
        SequenceFile.Metadata metadata = new SequenceFile.Metadata();
        Writer.Option metadataOption = Writer.metadata(metadata);

        try {

            writer = SequenceFile.createWriter( conf,SequenceFile.Writer.file(path),
                    SequenceFile.Writer.keyClass(key1.getClass()),
                    SequenceFile.Writer.valueClass(value1.getClass()),Writer.metadata(new SequenceFile.Metadata()));

            for (int i = 0; i < 10; i++) {
                Long key = Long.valueOf(i+1);
//                key1.set(key);
                key1.set(key.toString());
                String mykey = String.valueOf(key);
                String value = DATA[i%DATA.length];
                value1.set(value.getBytes(),0,value.length());

                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key,
                        value);
                writer.append(key1,value1);
            }
            writer.close();
        } finally {
            IOUtils.closeStream(writer);
        }
    }
}

