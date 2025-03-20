package com.cher.mymq.distributed.master;

import io.netty.channel.Channel;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class IndexedLogReader {
    // 存储每一行的文件偏移量（第0行为起点）
    private final List<Long> lineOffsets = new ArrayList<>();
    private final String logFilePath;

    public IndexedLogReader(String logFilePath) throws Exception {
        this.logFilePath = logFilePath;
        buildIndex();
    }

    // 构建文件索引
    private void buildIndex() throws Exception {
        try (RandomAccessFile file = new RandomAccessFile(logFilePath, "r")) {
            long offset = 0;
            String line;
            lineOffsets.add(offset);
            while ((line = file.readLine()) != null) {
                offset = file.getFilePointer();
                lineOffsets.add(offset);
            }
        }
    }

    // 根据行号读取日志行（行号从0开始）
    public String readLine(int lineNumber) throws Exception {
        if (lineNumber < 0 || lineNumber >= lineOffsets.size() - 1) {
            return null;
        }
        try (RandomAccessFile file = new RandomAccessFile(logFilePath, "r")) {
            file.seek(lineOffsets.get(lineNumber));
            return file.readLine();
        }
    }
}
