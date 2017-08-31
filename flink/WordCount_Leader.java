import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.io.File;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class WordCount_Leader {


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
            ReadableTimeStamp readableTimeStamp = new ReadableTimeStamp(timeWindow).invoke();
            String start_time = readableTimeStamp.getStart_time();
            String stop_time = readableTimeStamp.getStop_time();

            int tot = 0;
            String key = null;
            ArrayList<Integer> values = new ArrayList<Integer>();
            for (Tuple2<String, Integer> t : iterable){
                if (key == null) {
                    key = t.f0;
                }
                values.add(new Integer(t.f1));
                tot += t.f1;
            }
            collector.collect(new Tuple2<>(start_time+stop_time, new Tuple2<>(key, tot)));
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
            int k = 5;
            for (String key : map.keySet()) {
                if (top_k.size() < k) {
                    top_k.add(new Tuple2<>(key, map.get(key)));
                }
                else {
                    minRemove(top_k, new Tuple2<>(key, map.get(key)));
                }
            }
            System.out.println(top_k.toString());


            ArrayList<Tuple2<String, Integer>> events = new ArrayList<>();
            int totalVote = 0;
            for (Tuple2<String, Integer> t : top_k) {
                events.add(new Tuple2<>(t.f0, t.f1));
                totalVote += t.f1;
            }

            ArrayList<Tuple2<String, Integer>> result = new ArrayList<>();
            double threshold = 0.05;
            for (Tuple2<String, Integer> ev : events) {
                if ((ev.f1 / (double) totalVote) > threshold ) {
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

            if (!(tokens.length < 2)) {
                String[] leaders = new String[]{
                        "ahmadzai", "abdullah", "nishani", "rama", "bouteflika", "tebboune", "marti",
                        "dos santos", "williams", "browne", "salman", "macri", "sargsyan", "karapetyan",
                        "cosgrove", "turnbull", "van der bellen", "kern", "eliyev", "rasizade", "pindling",
                        "minnis", "isa", "salman", "hamid", "sheikh hasina", "belgrave", "stuart",
                        "michel", "young", "barrow", "talon", "jigme khesar namgyel wangchuck", "tobgay", "lukasenko",
                        "kabjakou", "htin kyaw", "san suu kyi", "morales", "inzko", "ivanic", "covic",
                        "izetbegovic", "zvizdic", "khama", "temer", "bolkiah", "radev", "borisov",
                        "kabore", "kaba thieba", "nkurunziza", "sihamoni", "hun sen", "biya", "yang",
                        "johnston", "trudeau", "fonseca", "ulisses correia silva", "zeman", "sobotka", "faustin-archange touadera",
                        "sarandji", "deby", "albert pahimi padacke", "bachelet", "xi jinping", "li keqiang", "anastasiades",
                        "papa francesco", "parolin", "bertello", "santos", "assoumani", "denis sassou nguesso", "mouamba",
                        "kabila", "tshibala", "jong-un", "yong-nam", "pak pong-ju", "moon jae-in", "nak-yeon",
                        "ouattara", "gon coulibaly", "solis", "grabar-kitarovic", "plenkovic", "castro", "lokke rasmussen",
                        "savarin", "skerrit", "medina", "moreno", "al-fattah al-sisi", "sherif", "ceren",
                        "zayed nahyan", "mohammed rashid maktum", "afewerki", "kaljulaid", "ratas", "teshome", "desalegn",
                        "konousi konrote", "bainimarama", "duterte", "niinisto", "sipila", "macron", "philippe",
                        "bongo ondimba", "issoze-ngondet", "barrow", "margvelashvili", "kvirikashvili", "steinmeier", "merkel",
                        "nana akufo-addo", "allen", "shinzo abe", "guelleh", "abdoulkader kamil", "allah ii", "hani mulqi",
                        "pavlopoulos", "tsipras", "la grenade", "mitchell", "morales", "conde", "youla",
                        "jose mario vaz", "umaro sissoco embalo", "obiang nguema mbasogo", "asue", "granger", "nagamootoo", "moise",
                        "lafontant", "hernandez", "mukherjee", "modi", "widodo", "khamenei", "rouhani",
                        "fu'ad ma'sum", "haydar al-'abadi", "higgins", "kenny", "guoni thorlacius johannesson", "bjarni benediktsson", "rivlin",
                        "netanyahu", "mattarella", "gentiloni", "abisulı nazarbaev", "sagintayev", "kenyatta", "atambayev",
                        "jeenbekov", "mamau", "al-ahmad al-jabir sabah", "jaber al-mubarak al-hamad al-sabah", "vorachith", "sisoulith", "mosisili",
                        "vejonis", "kucinskis", "aoun", "hariri", "sirleaf", "fayez al-sarraj", "hasler",
                        "grybauskaite", "skvernelis", "bettel", "ivanov", "dimitriev", "rajaonarimampianina", "solonandrasana",
                        "mutharika", "yameen", "razak", "boubacar keita", "maiga", "coleiro preca", "muscat",
                        "saadeddine el othmani", "heine", "abdel aziz", "yahya ould hademine", "gurib-fakim", "jugnauth", "pena nieto",
                        "dodon", "filip", "telle", "elbegdorz", "erdenebat", "vujanovic", "markovic",
                        "nyusi", "do rosario", "geingob", "kuugongelwa-amadhila", "waqa", "bidhya devi bhandari", "pushpa kamal dahal",
                        "ortega", "issoufou", "rafini", "buhari", "solberg", "reddy", "qabus",
                        "rutte", "hussain", "sharif", "remengesau", "abbas", "hamdallah", "varela",
                        "dadae", "o'neill", "cartes", "kuczynski", "zavala", "duda", "szydlo",
                        "rebelo sousa", "costa", "tamim al-thani", "abdullah nasser thani", "may", "iohannis", "grindeanu",
                        "kagame", "murekezi", "putin", "medvedev", "harris", "weymouth tapley seaton", "ballantyne",
                        "gonsalves", "kabui", "sogavare", "o le ao o le malo", "tuiatua tupua tamasese efi", "tuilaepa aiono sailele malielegaoi", "zavoli",
                        "d'ambrosio", "louisy", "chastanet", "carvalho", "trovoada", "sall", "dionne",
                        "nikolic", "vucic", "faure", "bai koroma", "tan", "hsien loong", "assad",
                        "khamis", "kiska", "fico", "pahor", "cerar", "abdullahi farmajo", "ali khayre",
                        "rajoy", "sirisena", "wickremesinghe", "trump", "zuma", "hasan ahmad al-bashir", "bakri saleh",
                        "salva kiir mayardit", "bouterse", "lofven", "leuthard", "sibusiso dlamini", "rahmon", "rasulzoda",
                        "magufuli", "majaliwa", "chan-ocha", "guterres", "araujo", "gnassingbe", "komi selom klassou",
                        "pohiva", "carmona", "rowley", "beji caid essebsi", "yussef al-shahed", "erdogan", "yıldırım",
                        "berdimuhammedow", "italeli", "sopoaga", "oleksijovyc porosenko", "groysman", "museveni", "rugunda",
                        "ader", "orban", "vazquez", "mirziyoyev", "aripov", "lonsdale", "salwai",
                        "maduro", "tran dai quang", "nguyen xuan phuc", "rabbih mansur hadi", "ahmed obeid daghr", "lungu", "mugabe",
                        "renzi", "obama", "hollande", "cameron"
                };

                String act1 = tokens[6];
                String act2 = tokens[16];
                int v = 0;


                for (String l : leaders) {
                    if ((act1).contains(l) && !act1.contains("for")) {
                        out.collect(new Tuple2<String, Integer>(tokens[6], Integer.parseInt(tokens[31])));
                        if (++v == 2)
                            break;
                    }
                    else if ((act2).contains(l) && !act2.contains("for")) {
                            out.collect(new Tuple2<String, Integer>(tokens[16], Integer.parseInt(tokens[31])));
                            if (++v == 2)
                                break;
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
