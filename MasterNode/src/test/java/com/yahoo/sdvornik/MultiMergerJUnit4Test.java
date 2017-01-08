package com.yahoo.sdvornik;

import com.yahoo.sdvornik.master.Merger;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.EnumSet;

public class MultiMergerJUnit4Test {


    private final int NUMBER_OF_KEYS_FOR_ONE_NODE = 2500;

    private final int ARRAY_SIZE = 500;

    private final String[] id = new String[]{"id1", "id2", "id3"};

    private final fj.data.List<String> idList = fj.data.List.iterableList(Arrays.asList(id));

    private Merger merger;

    private Path pathToResult;

    @Before
    public void initTest() throws IOException {

        pathToResult = Paths.get(".", Constants.TEST_OUTPUT_FILE_NAME);

        Files.deleteIfExists(pathToResult);
        Files.createFile(pathToResult);

        merger =  new Merger(
                idList,
                NUMBER_OF_KEYS_FOR_ONE_NODE*idList.length(),
                ARRAY_SIZE*idList.length(),
                pathToResult,
                null,
                null,
                null
        );
    }

    @After
    public void deleteTestFile() throws IOException {
        Files.deleteIfExists(pathToResult);
    }

    @Test
    public void testMultiMerger() throws Exception {

        Thread mainThread = new Thread(new Runnable() {
            @Override
            public void run() {
                merger.init();
                Thread t_1 = new Thread(new ArrayGenerator(0));
                t_1.start();
                Thread t_2 = new Thread(new ArrayGenerator(1));
                t_2.start();
                Thread t_3 = new Thread(new ArrayGenerator(2));
                t_3.start();
                try {
                    t_1.join();
                    t_2.join();
                    t_3.join();
                }
                catch(InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        mainThread.start();
        mainThread.join();
        Assert.assertTrue(checkSortedArray());
    }

    public class ArrayGenerator implements Runnable {
        private final int group;

        public ArrayGenerator(int group) {
            this.group = group;
        }
        @Override
        public void run() {
            int TOTAL_ARRAY_AMOUNT = NUMBER_OF_KEYS_FOR_ONE_NODE/ARRAY_SIZE;

            for(int numberOfChunk=0; numberOfChunk<TOTAL_ARRAY_AMOUNT; ++numberOfChunk) {
                long[] arr = new long[ARRAY_SIZE];
                for (int i = 0; i < ARRAY_SIZE; ++i) {
                    arr[i] = numberOfChunk*ARRAY_SIZE*idList.length() + idList.length()*(i+1) - group;
                }

                merger.putArrayInQueue(id[group], numberOfChunk, arr);
            }
        }
    }

    private boolean checkSortedArray() throws IOException {
        long[] sortedArr = null;

        final Path pathToFile = Paths.get(System.getProperty("user.home"), Constants.TEST_OUTPUT_FILE_NAME);

        try (SeekableByteChannel seekableByteChannel =
                Files.newByteChannel(pathToResult, EnumSet.of(StandardOpenOption.READ))) {
            int numberOfKeys = (int)seekableByteChannel.size() / Long.BYTES;
            sortedArr = new long[numberOfKeys];

            ByteBuffer buffer = ByteBuffer.allocate(numberOfKeys*Long.BYTES);
            seekableByteChannel.read(buffer);
            buffer.flip();

            for(int i = 0; i < sortedArr.length; ++i) {
                sortedArr[i] = buffer.getLong();
            }
        }

        boolean res = true;
        for(int i = 0; i < sortedArr.length-1; ++i) {
            res &= sortedArr[i] <= sortedArr[i+1];
        }
        return res;
    }
}
