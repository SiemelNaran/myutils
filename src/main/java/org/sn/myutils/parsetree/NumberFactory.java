package org.sn.myutils.parsetree;


public interface NumberFactory {
    Number fromString(String str) throws NumberFormatException;
}
