package learn.avinash.spark;

public class IntegerWithSquareRoot {
    private int originalNumber;
    private double squareroot;
    public IntegerWithSquareRoot(int i) {
        this.originalNumber =i;
        this.squareroot = Math.sqrt(originalNumber);
    }
}
