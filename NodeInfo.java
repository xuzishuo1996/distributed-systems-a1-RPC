public class NodeInfo {
    // TODO: add hostname and postname or not?
    private boolean busy;
    private double load;    // because of the return val of Math.pow() is double.

    public NodeInfo() {
        busy = false;
        this.load = 0;
    }

    public boolean isBusy() {
        return busy;
    }

    public void setBusy(boolean busy) {
        this.busy = busy;
    }

    public double getLoad() {
        return load;
    }

    public void addLoad(int numOfPasswords, short logRounds) {
        load += numOfPasswords * Math.pow(2, logRounds);
        busy = true;
    }

    public void subLoad(int numOfPasswords, short logRounds) {
        load -= numOfPasswords * Math.pow(2, logRounds);
        if (load == 0) {
            busy = false;
        }
    }
}
