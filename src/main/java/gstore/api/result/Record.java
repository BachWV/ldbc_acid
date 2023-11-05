package gstore.api.result;

import org.apache.commons.math3.util.Pair;

import java.util.*;
import java.util.stream.IntStream;

public class Record {

    private String[] vars;

    private Value[] values;

    private List<Pair<String, Value>> fields;

    public Record(String[] vars, Value[] values) {
        this.vars = vars;
        this.values = values;
        this.fields = new ArrayList<>();
        for (int i = 0; i < this.vars.length; i++) {
            this.fields.add(new Pair<String, Value>(vars[i], values[i]));
        }
    }

    public int index(String var) {
        OptionalInt indexOpt = IntStream.range(0, vars.length).filter(i->var.equals(vars[i])).findFirst();
        if (indexOpt.isPresent()) {
            return indexOpt.getAsInt();
        } else {
            throw new NoSuchElementException("Unknown key: " + var);
        }
    }

    public Value get(int idx) {
        return this.values[idx];
    }

    public List<Value> values() {
        return Arrays.asList(this.values);
    }

    public List<String> vars() {
        return Arrays.asList(this.vars);
    }
}
