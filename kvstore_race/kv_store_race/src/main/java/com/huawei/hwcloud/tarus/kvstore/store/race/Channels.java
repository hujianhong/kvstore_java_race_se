package com.huawei.hwcloud.tarus.kvstore.store.race;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public class Channels {

    public static FileChannel createFileChannel(File file) throws IOException {
        return FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    public static FileChannel createFileChannel(String file) throws IOException {
        return createFileChannel(new File(file));
    }
}
