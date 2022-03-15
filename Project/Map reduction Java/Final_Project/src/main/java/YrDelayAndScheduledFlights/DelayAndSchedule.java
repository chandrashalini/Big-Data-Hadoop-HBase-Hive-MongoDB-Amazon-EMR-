package YrDelayAndScheduledFlights;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DelayAndSchedule implements Writable {
    int scheduled;
    int delayed;

    public DelayAndSchedule() {
        super();
    }

    public int getScheduled() {
        return scheduled;
    }

    public void setScheduled(int scheduled) {
        this.scheduled = scheduled;
    }

    public int getDelayed() {
        return delayed;
    }

    public void setDelayed(int delayed) {
        this.delayed = delayed;
    }

    public DelayAndSchedule(int scheduled, int delayed) {
        super();
        this.scheduled = scheduled;
        this.delayed = delayed;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(scheduled);
        dataOutput.writeInt(delayed);
    }

    public void readFields(DataInput dataInput) throws IOException {
        scheduled = dataInput.readInt();
        delayed = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "scheduled: " + scheduled +"--- delayed: "+ delayed;
    }
}
