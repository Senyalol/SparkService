package spark;

public class RfmUpdateResult {
    final RFMState state;
    final boolean applied;

    RfmUpdateResult(RFMState state, boolean applied) {
        this.state = state;
        this.applied = applied;
    }
}
