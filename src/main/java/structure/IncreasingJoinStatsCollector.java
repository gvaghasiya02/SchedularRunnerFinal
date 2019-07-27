package structure;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Set;

/**
 * Created by shiva on 3/13/18.
 */
public class IncreasingJoinStatsCollector extends StatsCollector {

    int xmin=1;
    int xmax=10;

    public IncreasingJoinStatsCollector(String statsFile, int ignore) {
        super(statsFile, ignore);
    }

    @Override public void report() {
        super.chartReport(0,statsFile);
        partialReport(0,statsFile);
    }
    private void partialReport(int startRound, String fileName){
        try {
            PrintWriter pw = new PrintWriter(new File("./files/output/avg/avg_"+Thread.currentThread().getName()+fileName));
            if (startRound != 0) {
                ignore = -1;
            }
            DateFormat df = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss");
            Date dateobj = new Date();
            String currentTime = df.format(dateobj);
            StringBuffer avgsb = new StringBuffer(currentTime);
            Set<Pair> keys = qvToStat.keySet();
            Pair[] qvs = keys.toArray(new Pair[keys.size()]);
            Arrays.sort(qvs);
            for (Pair p : qvs) {
                QueryStat qs = qvToStat.get(p);
                double partialAvg = -1;
                double partialSTD = -1;
                if (avgsb != null) {
                    partialAvg = qs.getAverageRT(ignore);
                    partialSTD = qs.getSTD(ignore,partialAvg);
                    avgsb.append("Q").append(p.toString()).append("\t").append("avgRT: ").append(partialAvg).append(" STD: " ).append(partialSTD).append("\n");
                }
            }
            if (avgsb != null) {
                pw.println(avgsb.toString());
                pw.println("\n");
            }
            pw.close();

        } catch (IOException e) {
            System.err.println("Problem in creating report in StatsCollector !");
            e.printStackTrace();
        }
    }
}
