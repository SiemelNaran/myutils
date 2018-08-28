package test;

// renames getValue to getIntegerValue
// renames getSix to getNumber

public class Thing {
    private static void test1() {
        Thing thing = new Thing();
        
        int a = thing.getIntegerValue() + 1;
        System.out.println(a);

        int b = thing.getIntegerValue();
        int c = thing.getIntegerValue();
        System.out.println(b + c);

        int d = thing.getIntegerValue() + thing.getIntegerValue();
        System.out.println(d);
    }

    private static void test2() {
        Thing thing = new Thing();
        
        int a = thing.getNumber() + 1;
        System.out.println(a);

        int b = thing.getNumber();
        int c = thing.getNumber();
        System.out.println(b + c);

        int d = thing.getNumber() + thing.getNumber();
        System.out.println(d);
    }

    public static void main(String[] args) {
        test1();
        test2();
    }

    public int getIntegerValue() {
        return 5;
    }

    public int getNumber() {
        return 7;
    }
}

