package com.flame;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 生成数据的工具类
 */
public class Producer {

    private String start = "2019-01-01";
    private String end = "2020-01-01";

    private List<String> phoneNum = new ArrayList<String>();
    private Map<String, String> phoneName = new HashMap<String, String>();

    private DecimalFormat df = new DecimalFormat("0000");
    private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private void init() {
        phoneNum.add("19251212343");
        phoneNum.add("15961260091");
        phoneNum.add("17130206814");
        phoneNum.add("18682499648");
        phoneNum.add("15361960968");
        phoneNum.add("18356645821");
        phoneNum.add("17818674361");
        phoneNum.add("14266298447");
        phoneNum.add("13141904126");
        phoneNum.add("13157770954");
        phoneNum.add("19460860743");
        phoneNum.add("14016550401");
        phoneNum.add("14314302040");
        phoneNum.add("17457157786");
        phoneNum.add("15108090007");
        phoneNum.add("15882276699");
        phoneNum.add("19694998088");
        phoneNum.add("18264427294");
        phoneNum.add("17245432318");
        phoneNum.add("16705495586");
        phoneNum.add("16705495586");

        phoneName.put("19251212343", "李雁");
        phoneName.put("15961260091", "卫艺");
        phoneName.put("17130206814", "仰莉");
        phoneName.put("18682499648", "陶欣悦");
        phoneName.put("15361960968", "施梅梅");
        phoneName.put("18356645821", "金虹霖");
        phoneName.put("17818674361", "魏明艳");
        phoneName.put("14266298447", "华贞");
        phoneName.put("13141904126", "华啟倩");
        phoneName.put("13157770954", "仲采绿");
        phoneName.put("19460860743", "卫丹");
        phoneName.put("14016550401", "戚丽红");
        phoneName.put("14314302040", "何翠柔");
        phoneName.put("17457157786", "钱溶艳");
        phoneName.put("15108090007", "钱琳");
        phoneName.put("15882276699", "缪静欣");
        phoneName.put("19694998088", "焦秋菊");
        phoneName.put("18264427294", "吕访琴");
        phoneName.put("17245432318", "沈丹");
        phoneName.put("16705495586", "褚美丽");
    }

    private String productLog() throws ParseException {
        String caller;
        String callee;
        String buildTime;
        int dura;
        //1. 随机生成两个不同的电话号
        int callerIndex = (int) (Math.random() * phoneNum.size());
        caller = phoneNum.get(callerIndex);

        while (true) {
            int calleeIndex = (int) (Math.random() * phoneNum.size());
            callee = phoneNum.get(calleeIndex);
            if (callerIndex == calleeIndex) break;
        }
        //2. 随机生成通话建立时间
        buildTime = randomBuildTime(start, end);

        //3. 随机生成通话时长
        dura = (int) (Math.random() * 30 * 60) + 1;
        String duration = df.format(dura);
        return caller + "," + callee + "," + buildTime + "," + duration + "\n";
    }

    // 随机生成通话时间 (yyyy-MM-dd HH:mm:ss)
    private String randomBuildTime(String start, String end) throws ParseException {
        long startPoint = sdf1.parse(start).getTime();
        long endPoint = sdf1.parse(end).getTime();

        long resultTS = startPoint + (long) (Math.random() * (endPoint - startPoint));
        return sdf2.format(new Date(resultTS));
    }

    private void writeLog(String path) throws IOException, ParseException, InterruptedException {
        FileOutputStream fos = new FileOutputStream(path);
        OutputStreamWriter osw = new OutputStreamWriter(fos);

        while (true) {
            String log = productLog();
            System.out.println(log);
            osw.write(log);
            osw.flush();
            Thread.sleep(500);
        }
    }

    public static void main(String[] args) throws ParseException, InterruptedException, IOException {
        if (args.length <= 0) {
            System.out.println("没有参数");
            System.exit(0);
        }
        Producer producer = new Producer();
        producer.init();
        producer.writeLog(args[0]);
    }
}
