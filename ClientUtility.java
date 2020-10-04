import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ClientUtility {
    private static final Random rand = new Random();

    public static List<String> genPasswords(int lenOfCharacters, int numOfPasswords) {
        List<String> res = new ArrayList<>();
        for (int i = 0; i < numOfPasswords; ++i) {
            res.add(genPassword(lenOfCharacters));
        }
        return res;
    }

    public static String genPassword(int lenOfCharacters) {
        byte[] bytes = new byte[lenOfCharacters];
        rand.nextBytes(bytes);
        return new String(bytes);
    }
}
