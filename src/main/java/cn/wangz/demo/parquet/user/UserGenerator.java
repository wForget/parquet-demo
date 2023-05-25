package cn.wangz.demo.parquet.user;

import cn.wangz.demo.parquet.user.avro.User;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class UserGenerator {

    private AtomicLong id;

    public UserGenerator() {
        this.id = new AtomicLong(0L);
    }

    public User generate() {
        User user = new User();
        user.setId(id.getAndIncrement());
        user.setAge(randomInt(100));
        user.setName(generateString(8));
        user.setHobbies(generateStringList(8));
        user.setProperties(generateStringMap(20));
        return user;
    }

    private List<CharSequence> generateStringList(int lengthBound) {
        int length = randomInt(lengthBound);
        List<CharSequence> list = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            list.add(generateString(8));
        }
        return list;
    }

    private Map<CharSequence, CharSequence> generateStringMap(int lengthBound) {
        int length = randomInt(lengthBound);
        Map<CharSequence, CharSequence> map = new HashMap<>();
        for (int i = 0; i < length; i++) {
            map.put(generateString(8), generateString(16));
        }
        return map;
    }


    private String generateString(int lengthBound) {
        int length = randomInt(lengthBound) + 1;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(randomChar());
        }
        return sb.toString();
    }

    private char randomChar() {
        // 65: A, 97: a
        int start;
        if (randomInt(2) == 0){
            start = 65;
        } else {
            start = 97;
        }
        return (char) (randomInt(26) + start);
    }

    private int randomInt(int bound) {
        return new Random().nextInt(bound);
    }

}
