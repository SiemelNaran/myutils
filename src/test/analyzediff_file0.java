package test;

public class Thing {
    private static void test1() {
        Thing thing = new Thing();
        
        int a = thing.getValue() + 1;
        System.out.println(a);

        int b = thing.getValue();
        int c = thing.getValue();
        System.out.println(b + c);

        int d = thing.getValue() + thing.getValue();
        System.out.println(d);
    }

    private static void test2() {
        Thing thing = new Thing();
        
        int a = thing.getSeven() + 1;
        System.out.println(a);

        int b = thing.getSeven();
        int c = thing.getSeven();
        System.out.println(b + c);

        int d = thing.getSeven() + thing.getSeven();
        System.out.println(d);
    }

    public static void main(String[] args) {
        test1();
        test2();
    }

    public int getValue() {
        return 5;
    }

    public int getSeven() {
        return 7;
    }
}
