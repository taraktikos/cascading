package cascading;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.AssertionLevel;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.assertion.AssertExpression;
import cascading.operation.regex.RegexParser;
import cascading.operation.text.DateParser;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tap.local.PartitionTap;
import cascading.tap.partition.DelimitedPartition;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import lombok.extern.slf4j.Slf4j;

import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;

@Slf4j
public class Application {
    public static void main(String[] args) {
        String inputPath = "/home/taras/projects/cascading/helloworld/data/NASA_access_log_Aug95.txt";
        String outputPath = "/home/taras/projects/cascading/helloworld/data/out";

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, Application.class);
        LocalFlowConnector flowConnector = new LocalFlowConnector(properties);
        FileTap inTap = new FileTap(new TextLine(), inputPath);
        FileTap outTap = new FileTap(new TextDelimited(true, "\t"), outputPath, SinkMode.REPLACE);

        Fields apacheFields = new Fields("ip", "time", "request", "response", "size");
        // Define the regular expression used to parse the log file
        String apacheRegex = "^([^ ]*) \\S+ \\S+ \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) ([^ ]*).*$";
        // Declare the groups from the above regex. Each group will be given a field name from 'apacheFields'
        int[] allGroups = {1, 2, 3, 4, 5};

        RegexParser regexParser = new RegexParser(apacheFields, apacheRegex, allGroups);

        Pipe processPipe = new Each("processPipe", new Fields("line"), regexParser, Fields.RESULTS);

        AssertExpression assertExpr = new AssertExpression("response != 404", Long.class);
        processPipe = new Each(processPipe, AssertionLevel.VALID, assertExpr);

        DateParser dateParser = new DateParser(new Fields("time"), "dd/MMM/yyyy:HH:mm:ss Z");
        processPipe = new Each(processPipe, new Fields("time"), dateParser, Fields.REPLACE);

        processPipe = new Each(processPipe, new Fields("time"), new DayForTimestamp(), Fields.ALL);
        processPipe = new GroupBy(processPipe, new Fields("day"));

        Scheme scheme = new TextDelimited(new Fields("day", "ip", "time", "request", "size"), true, "\t");

        DelimitedPartition partition = new DelimitedPartition(new Fields("day"), "-");

        Tap daysTap = new PartitionTap(outTap, partition, SinkMode.REPLACE);
        Tap trapTap = new FileTap(new TextDelimited(true, "\t"), outputPath + "/trap", SinkMode.REPLACE);

        FlowDef flowDef = FlowDef.flowDef().setName("part 1")
                .addSource(processPipe, inTap)
                .addTailSink(processPipe, daysTap)
                .addTrap("processPipe", trapTap);

        Flow wcFlow = flowConnector.connect(flowDef);
        flowDef.setAssertionLevel(AssertionLevel.VALID);
        wcFlow.complete();
        log.info("Finished");
    }

    // Breaks down the time in days for further analysis
    private static class DayForTimestamp extends BaseOperation implements Function {
        DayForTimestamp() {
            super(1, new Fields("day"));
        }

        public DayForTimestamp(Fields fieldDeclaration) {
            super(1, fieldDeclaration);
        }

        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            // Gets the arguments TupleEntry
            TupleEntry arguments = functionCall.getArguments();

            // Creates a Tuple to hold our result values
            Tuple result = new Tuple();

            Calendar calendar = Calendar.getInstance();
            calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
            calendar.setTimeInMillis(arguments.getLong(0));
            int day = calendar.get(java.util.Calendar.DAY_OF_MONTH);

            // Adds the day value to the result Tuple
            result.add(day);

            // Returns the result Tuple
            functionCall.getOutputCollector().add(result);
        }
    }
}
