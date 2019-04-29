package myutils.util.parsetree;


public interface NumberFactory {
    Number fromString(String str) throws NumberFormatException;
}
