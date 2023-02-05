package com.learnkafkastreams.topology;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.*;
import java.time.format.DateTimeFormatter;

@Slf4j
public class ExploreWindowTopology {

    public static final String WINDOW_WORDS = "windows-words";

    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();


        var wordsStream = streamsBuilder
                .stream(WINDOW_WORDS,
                        Consumed.with(Serdes.String(), Serdes.String()));

      //  wordsStream.print(Printed.<String,String>toSysOut().withLabel("words"));


        //tumblingWindow(wordsStream);
        //hoppingWindow(wordsStream);
       slidingWindow(wordsStream);

        return streamsBuilder.build();
    }

    private static void tumblingWindow(KStream<String, String> wordsStream) {
        Duration windowSize = Duration.ofSeconds(5);
        Duration graceWindowsSize = Duration.ofSeconds(2);

        TimeWindows hoppingWindow = TimeWindows.ofSizeWithNoGrace(windowSize);

        //TimeWindows hoppingWindow = TimeWindows.ofSizeAndGrace(windowSize, graceWindowsSize);

        wordsStream
                .groupByKey()
                .windowedBy(hoppingWindow)
                .count()
                .suppress(Suppressed
                        .untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull())
                )
                .toStream()
                .peek(((key, value) -> {
                    log.info("tumblingWindow : key : {}, value : {}", key, value);
                    printLocalDateTimes(key, value);
                }))
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel("tumblingWindow"));
        ;

    }

    private static void slidingWindow(KStream<String, String> wordsStream) {

        SlidingWindows slidingWindow = SlidingWindows
                .ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));

        wordsStream
                .groupByKey()
                .windowedBy(slidingWindow)
                .count()
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()))
                .toStream()
                .peek(((key, value) -> {
                    log.info("slidingWindow : key : {}, value : {}", key, value);
                    printLocalDateTimes(key, value);
                }))
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel("slidingWindow"));

    }

    private static void hoppingWindow(KStream<String, String> wordsStream) {
        Duration windowSize = Duration.ofSeconds(5);
        Duration advanceBySize = Duration.ofSeconds(3);

        TimeWindows hoppingWindow = TimeWindows.ofSizeWithNoGrace(windowSize)
                .advanceBy(advanceBySize);

         wordsStream
                .groupByKey()
                .windowedBy(hoppingWindow)
                .count()
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()))
                .toStream()
                .peek(((key, value) -> {
                    log.info("hoppingWindow : key : {}, value : {}", key, value);
                    printLocalDateTimes(key, value);
                }))

                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel("hoppingWindow"));

    }

    private static void printLocalDateTimes(Windowed<String> key, Long value) {
        var startTime = key.window().startTime();
        var endTime = key.window().endTime();

        LocalDateTime startLDT = LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
        LocalDateTime endLDT = LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
        log.info("startLDT : {} , endLDT : {}, Count : {}", startLDT, endLDT, value);
    }

    public static void main(String[] args) {


        //var instant = Instant.ofEpochMilli(1672237080000L);//2022-12-28T08:18:00
        var instant = Instant.ofEpochMilli(1672237110000L);//2022-12-28T08:18:30
        var zone = ZoneId.SHORT_IDS.get("CST");
        System.out.println("zone : " + zone);
        LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));

        System.out.println("ldt  : " + ldt);
        var formattedDate = ldt.format(DateTimeFormatter.ISO_DATE_TIME);
        System.out.println("formattedDate  : " + formattedDate);


    }

}
