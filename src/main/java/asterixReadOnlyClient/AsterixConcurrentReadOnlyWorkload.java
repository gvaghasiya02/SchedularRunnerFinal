package asterixReadOnlyClient;

import client.AbstractReadOnlyClientUtility;
import structure.Pair;
import structure.Query;
import workloadGenerator.ReadOnlyWorkloadGenerator;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class AsterixConcurrentReadOnlyWorkload extends AsterixClientReadOnlyWorkload {

    private ExecutorService executorService;
    AtomicInteger totalResTime = new AtomicInteger(0);
    AtomicInteger count_all_queries = new AtomicInteger(0);

    private Map<Integer, ReadOnlyWorkloadGenerator> rwgMap;

    private Map<Integer, AbstractReadOnlyClientUtility> clUtilMap;

    private int numReaders;

    private List<Long> readerSeeds;

    public AsterixConcurrentReadOnlyWorkload(String cc, String dvName, int iter, String qGenConfigFile, String
            qIxFile, String statsFile, int ignore, String qSeqFile, String resDumpFile, long seed, long minUserId,long maxUsrId,
            int numReaders, String server) {
        super();
        this.ccUrl = cc;
        this.dvName = dvName;
        this.iterations = iter;
        this.numReaders = numReaders;
        clUtilMap = new HashMap<>();
        initReaderSeeds(seed);
        setClientUtil(qIxFile, qGenConfigFile, statsFile, ignore, qSeqFile, resDumpFile, server);
        initReadOnlyWorkloadGen(seed, minUserId,maxUsrId);
        initExecutors();
        execQuery = true;
        //super(cc, dvName, iter, qGenConfigFile, qIxFile, statsFile, ignore, qSeqFile, resDumpFile, seed, maxUsrId);
    }

    private void shutDownExecutors() {
        try {
            executorService.shutdown();
            executorService.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS); // TODO: Is this necessary?

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            if (!executorService.isTerminated()) {
                System.out.println("canceling all pending tasks");
            }
            System.out.println("Finished at: "+System.currentTimeMillis());
            executorService.shutdownNow();

            System.out.println("count :"+count_all_queries.doubleValue());
            System.out.println("total time: "+totalResTime.doubleValue());
            System.out.println("AVG Resp Time: "+ totalResTime.doubleValue()/count_all_queries.doubleValue());
            System.out.println("Shutdown complete!");
        }
    }

    private void initReaderSeeds(long seed) {
        readerSeeds = new ArrayList<>();
        Random rand = new Random(seed);
        IntStream.range(0, numReaders).forEach(x -> {
            readerSeeds.add(rand.nextLong());
        });
    }

    private void initExecutors() {
        executorService = Executors.newFixedThreadPool(this.numReaders);
    }

    @Override
    public void initReadOnlyWorkloadGen(long seed, long minUserId,long maxUsrId) {
        rwgMap = new HashMap<>();
        IntStream.range(0, numReaders).forEach(x -> {
            rwgMap.put(x, new ReadOnlyWorkloadGenerator(clUtilMap.get(x).getQIxFile(), clUtilMap.get(x)
                    .getQGenConfigFile(), readerSeeds.get(x), minUserId,maxUsrId));
        });
    }

    @Override
    public void setClientUtil(String qIxFile, String qGenConfigFile, String statsFile, int ignore,
            String qSeqFile, String resultsFile, String server) {
        //TODO: Append the result and other stat files with threadIds.
        IntStream.range(0, numReaders).forEach(x -> {
            String statsF = statsFile.contains(".txt")?statsFile.split(".txt")[0]+x+".txt":statsFile+x+".txt";
            String resF = resultsFile.contains(".txt")?resultsFile.split(".txt")[0]+x+".txt":resultsFile+x+".txt";
            try {
                clUtilMap.put(x, new AsterixReadOnlyClientUtility(ccUrl, qIxFile, qGenConfigFile, statsF, ignore, qSeqFile,
                        resF, server));
            } catch (IOException e) {
                e.printStackTrace();
            }
            clUtilMap.get(x).init();
        });
    }

    @Override
    public void execute() {
        System.out.println("started at : " +System.currentTimeMillis());
        IntStream.range(0, numReaders).forEach(readerId -> {
            executorService.submit(() -> {
                long iteration_start = 0l;
                long iteration_end = 0l;
                for (int i = 0; i < iterations; i++) {
                    Thread.currentThread().setName("Client-"+readerId);
                    System.out.println("\nAsterixDB Client - Read-Only Workload - Starting Iteration " + i + " in " +
                            "thread: " + readerId + " (" + Thread.currentThread().getName() + ")");
                    iteration_start = System.currentTimeMillis();
                    for (Pair qvPair : clUtilMap.get(readerId).qvids) {
                        int qid = qvPair.getQId();
                        int vid = qvPair.getVId();
                        Query q = rwgMap.get(readerId).nextQuery(qid, vid);
                        if (q == null) {
                            continue; //do not break, if one query is not found
                        }
                        long q_start = System.currentTimeMillis();

                        if (execQuery) {
                            try {
                                clUtilMap.get(readerId).executeQuery(qid, vid, q.aqlPrint(dvName));
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        long q_end = System.currentTimeMillis();
                        System.out.println("Iteration "+i+" Thread "+ Thread.currentThread().getName()+" Q"+qvPair.getQId()+" version "+qvPair.getVId()+"\t"+(q_end-q_start));
                        int diff= (int)(q_end-q_start);
                        totalResTime.addAndGet(diff);
                        count_all_queries.addAndGet(1);
                    }
                    iteration_end = System.currentTimeMillis();

                    System.out.println("Total time for iteration " + i + " :\t" + (iteration_end - iteration_start) +
                            " ms in thread: " + readerId + " (" + Thread.currentThread().getName() + ")");
                    int diff= (int)(iteration_end-iteration_start);
                    totalResTime.addAndGet(diff);
                    count_all_queries.addAndGet(1);
                }
                clUtilMap.get(readerId).terminate();
            });
        });

        shutDownExecutors();
    }

    @Override
    public void setDumpResults(boolean b) {
        IntStream.range(0, numReaders).forEach(x -> {

        });
    }

    @Override
    public void generateReport() {
        IntStream.range(0, numReaders).forEach(x -> clUtilMap.get(x).generateReport());
    }
    public void setxyMap(){

    }
}
