# DSAPIAdvPart2ScorePy

Źródło klasy `ScoreEventArrayStreamSourceFunction`

```Java
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;

public class ScoreEventArrayStringSource implements SourceFunction<String> {

    private final String[] jsonStrings;
    private final int intervalMillis;
    private volatile boolean isRunning = true;

    public ScoreEventArrayStringSource(String[] jsonStrings, int intervalMillis) {
        this.jsonStrings = jsonStrings;
        this.intervalMillis = intervalMillis;
    }

    public ScoreEventArrayStringSource(String[] jsonStrings) {
        this(jsonStrings, 1000);
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        for (String json : jsonStrings) {
            if (!isRunning) {
                break;
            }
            // emitujemy surowy JSON
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(json);
            }
            Thread.sleep(intervalMillis);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
```