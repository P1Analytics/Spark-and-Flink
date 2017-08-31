import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.io.File;
import org.apache.flink.streaming.api.windowing.time.Time;
import javax.annotation.Nullable;
import org.apache.flink.streaming.api.TimeCharacteristic;
import java.text.ParseException;
import java.util.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.io.File;


class WordCount_Region {


    public static void main(String[] args) throws Exception {
        long startTime = System.nanoTime();

        final StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        String path = "/Users/nanazhu/Documents/LosAlamos/BigDataComputing/Crimeblabla/";


        File dir = new File(path + "data");
        File[] directoryListing = dir.listFiles();
        if (directoryListing == null || directoryListing.length < 1)
            return;
        DataStream<String> stream = see.readTextFile(directoryListing[0].toString());
        for (int i = 1; i < directoryListing.length; stream = stream.union(see.readTextFile(directoryListing[i++].toString())));


        DataStream counts = stream
                .assignTimestampsAndWatermarks(new StreamTimestamp())
                .flatMap(new LineSplitter())
                .keyBy(0)
                .timeWindow(Time.days(7))
                .apply(new ApplyTrend1())
                .keyBy(0)
                .timeWindow(Time.days(7))
                .apply(new ApplyTrend2());


        String log = "log";
        deleteDirectory(new File(path + log));
        counts.writeAsText(path+ log).setParallelism(1);

        see.execute();
        long endTime = System.nanoTime();
        System.out.println("Took "+(endTime - startTime)/(double)1000000000 + " seconds");

    }

    private static class ApplyTrend1 implements WindowFunction<Tuple2<String, Integer>, Tuple2<String, Tuple2<String, Integer>>, Tuple, TimeWindow> {
        public void apply(Tuple tuple,
                          TimeWindow timeWindow,
                          Iterable<Tuple2<String, Integer>> iterable,
                          Collector<Tuple2<String, Tuple2<String, Integer>>> collector)
                throws Exception {
            int tot = 0;
            String country = null;
            ArrayList<Integer> values = new ArrayList<Integer>();
            for (Tuple2<String, Integer> t : iterable){
                if (country == null) {
                    country = t.f0;
                }
                values.add(new Integer(t.f1));
                tot += t.f1;
            }

            double threshold = 0.3;
            if ( (tot / (double) values.size()) > 1 / threshold ) {
                collector.collect(new Tuple2<String, Tuple2<String, Integer>>(timeWindow.toString(), new Tuple2<String, Integer>(country, tot)));

            }
        }
    }

    private static class ApplyTrend2 implements WindowFunction<Tuple2<String, Tuple2<String, Integer>>, Tuple2<String, List<Tuple2<String, Integer>>>, Tuple, TimeWindow> {

        public void apply(Tuple tuple,
                          TimeWindow timeWindow,
                          Iterable<Tuple2<String, Tuple2<String, Integer>>> iterable,
                          Collector<Tuple2<String, List<Tuple2<String, Integer>>>> collector)
                throws Exception {

            ReadableTimeStamp readableTimeStamp = new ReadableTimeStamp(timeWindow).invoke();
            String start_time = readableTimeStamp.getStart_time();
            String stop_time = readableTimeStamp.getStop_time();



            HashMap<String, Integer> map = new HashMap<>();
            for (Tuple2<String, Tuple2<String, Integer>> t : iterable){
                Tuple2<String, Integer> copy = new Tuple2(new String(t.f1.f0), new Integer(t.f1.f1));
                Integer val = map.get(copy.f0);
                map.put(copy.f0, (val == null) ? t.f1.f1 : val + t.f1.f1);
            }
            ArrayList<Tuple2<String, Integer>> top_k = new ArrayList();
            int k = 10;
            for (String key : map.keySet()) {
                if (top_k.size() < 10) {
                    top_k.add(new Tuple2<>(key, map.get(key)));
                }
                else {
                    minRemove(top_k, new Tuple2<>(key, map.get(key)));
                }
            }

            ArrayList<Tuple2<String, Integer>> events = new ArrayList<>();
            int totalVote = 0;
            int max = 0;
            String max_region = null ;
            for (Tuple2<String, Integer> t : top_k) {
                events.add(new Tuple2<>(t.f0, t.f1));
                totalVote += t.f1;
                if (max < t.f1){
                    max = t.f1;
                    max_region = t.f0;
                }
            }

            ArrayList<Tuple2<String, Integer>> result = new ArrayList<>();
            double threshold = 0.1;
            for (Tuple2<String, Integer> ev : events) {
                if ((ev.f1 / (double) totalVote) > threshold && totalVote > 100 && ev.f1 > 100) {
                    result.add(ev);
                }
            }
            if (!result.isEmpty()) {
                collector.collect(new Tuple2<String, List<Tuple2<String, Integer>>>(start_time+stop_time, result));
            }
        }
        private void minRemove(ArrayList<Tuple2<String, Integer>> list, Tuple2<String, Integer> newT) {
            int idx = 0;
            Tuple2<String, Integer>  min = list.get(0);
            for (int i = 1; i < 5; ++i) {
                if (list.get(i).f1 < min.f1) {
                    min = list.get(i);
                    idx = i;
                }
            }

            if (min.f1 < newT.f1) {
                list.remove(idx);
                list.add(newT);
            }
        }
    }



    public static boolean deleteDirectory(File directory) {
        if(directory.exists()){
            File[] files = directory.listFiles();
            if(null != files){
                for(int i = 0; i < files.length; i++) {
                    if(files[i].isDirectory()) {
                        deleteDirectory(files[i]);
                    }
                    else {
                        files[i].delete();
                    }
                }
            }
        }
        return(directory.delete());
    }


    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {

            String[] tokens = value.toLowerCase().split("\t");
            if (!(tokens.length == 1 || tokens[50].equals("") || tokens[50] == null )) {
                if (tokens[28].equals("17") || tokens[28].equals("18") ||
                        tokens[28].equals("19") || tokens[28].equals("20")) {
                    try {
                        Integer m = Integer.parseInt(tokens[31]);
                        if (m != null) {
                            out.collect(new Tuple2<>(tokens[50], m));
                        }
                    } catch (Exception e) {
                        out.collect(new Tuple2<>(tokens[50], 1));
                    }
                }
            }
        }
    }


    private static class StreamTimestamp implements AssignerWithPeriodicWatermarks<String> {
        public long extractTimestamp(String elem, long l) {
            DateFormat format = new SimpleDateFormat("yyyyMMdd", Locale.ENGLISH);
            Date date = null;
            try {
                String string = "";
                if (elem.split("\t").length<2){
                    string = "19000101";
                }
                else{
                    string = elem.split("\t")[1];
                }
                date = format.parse(string);
            } catch (ArrayIndexOutOfBoundsException e){
                System.out.println("OHHHHH SOMEThiNG EMPTYYYY " );
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return date.getTime();
        }
        @Nullable
        public Watermark getCurrentWatermark() {
            return null;
        }
    }
    public static class ReadableTimeStamp {
        private TimeWindow timeWindow;
        private String start_time;
        private String stop_time;

        public ReadableTimeStamp(TimeWindow timeWindow) {
            this.timeWindow = timeWindow;
        }

        public String getStart_time() {
            return start_time;
        }

        public String getStop_time() {
            return stop_time;
        }

        public ReadableTimeStamp invoke() {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(timeWindow.getStart());
            int sYear = calendar.get(Calendar.YEAR);
            int sMonth = calendar.get(Calendar.MONTH);
            sMonth = sMonth+1;
            int sDay = calendar.get(Calendar.DAY_OF_MONTH);
            start_time = new String(sYear + "_" + sMonth + "_" + sDay + "/");
            Calendar calendar_E = Calendar.getInstance();
            calendar_E.setTimeInMillis(timeWindow.getEnd());
            int eYear = calendar_E.get(Calendar.YEAR);
            int eMonth = calendar_E.get(Calendar.MONTH);
            eMonth = eMonth+1;
            int eDay = calendar_E.get(Calendar.DAY_OF_MONTH);
            stop_time = new String(eYear + "_" + eMonth + "_" + eDay);
            return this;
        }
    }

}
