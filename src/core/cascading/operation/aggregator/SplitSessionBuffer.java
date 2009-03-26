package cascading.operation.aggregator;

import cascading.flow.FlowProcess;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.Tuples;
import java.util.Iterator;

public class SplitSessionBuffer extends BaseOperation implements Buffer
  {

  public static final Double THRESHOLD = 10*60.0;

  public SplitSessionBuffer()
    {
    super( 1, new Fields( "session" ) );
    }

  public SplitSessionBuffer( Fields fieldDeclaration )
    {
    super( 1, fieldDeclaration );
    }

  public void operate( FlowProcess flowProcess, BufferCall bufferCall )
    {
    // init the count and sum
    TupleEntry last = null;

    // get all the current argument values for this grouping
    Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();

    String currentSession = "";

    while(arguments.hasNext()) {
        TupleEntry cur = new TupleEntry(arguments.next());
        String ctlr_action = cur.getString(1);
        if (last != null) {
            Double diff = cur.getDouble(0) - last.getDouble(0);
            if (diff > THRESHOLD) {
               bufferCall.getOutputCollector().add(new Tuple(currentSession));
               currentSession = ctlr_action;
            } else {
               currentSession += " > " + ctlr_action;
            }
        } else {
            currentSession = ctlr_action;
        }
        last = cur;
      }
      bufferCall.getOutputCollector().add(new Tuple(currentSession));
    }
  }
