package main;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class seqReader {
    public static void main(String args[]){

        String uri = "file:///home/cloud/B.txt";
        Configuration conf = new Configuration();
        conf.set("io.serializations",
                "org.apache.hadoop.io.serializer.WritableSerialization");
//                "org.apache.hadoop.io.serializer.JavaSerialization");
        conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        Reader reader = null;

        Path file = new Path(uri);
        try {

            reader = new Reader(conf, Reader.file(file));
            System.out.println(reader.getKeyClassName());
//            Writable key1 = (Writable) ReflectionUtils.newInstance(,conf);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(),conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(),conf);
            long position = reader.getPosition();
            while (reader.next(key,value)){
                String syncSeen = reader.syncSeen() ? "*":"";
//                System.out.pr,position,syncSeen,key.wd,value.toString());
                  System.out.println(key.toString() + ":" + value.toString());
                position = reader.getPosition();
            }


        }catch (IOException e){
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(reader);
        }
    }
}
