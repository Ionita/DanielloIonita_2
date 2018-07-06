package dataInjection;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

class KafkaBenchmark {

    private static KafkaBenchmark instance = new KafkaBenchmark();
    private long totalTime = 0L;
    private int totalMessages = 0;
    private int bytePerMessage = 0;
    private int nMessages = 0;
    private long startTime = 0L;
    private long endtime = 0L;
    private boolean stopAll = false;
    private Integer messagesThatHasToBeSent;


    private KafkaBenchmark(){}

    static KafkaBenchmark getInstance(){
        return instance;
    }

    void startTime(){
        startTime = System.currentTimeMillis();
    }

    void stopAll(){
        totalMessages += nMessages;
        System.out.println("Final: Packets sent: \t" + totalMessages);
        System.out.println("Final_:Time spent in seconds: \t" + totalTime/1000);
        if(totalTime != 0)
            System.out.println("Final: Average rate: \t" + (totalMessages*bytePerMessage/1000)/totalTime + " MB/s");
        stopAll = true;
    }

    void setBytePerMessage(int bytePerMessage){
        this.bytePerMessage = bytePerMessage;
    }

    void setnMessages(int nMessages) {
        this.nMessages += nMessages;
    }

    void startThread(){
        Thread thread1 = new Thread(() -> {
            while(!stopAll) {
                try {
                    Thread.sleep(5000);
                    if(!stopAll){
                        if (nMessages != 0) {
                            if (startTime != 0L) {
                                endtime = System.currentTimeMillis();
                                System.out.println("Byte per message: \t" + bytePerMessage);
                                System.out.println("Throughput Kafka (Bytes): \t" + ((nMessages * bytePerMessage)) / (endtime - startTime) + " KB/s");
                                System.out.println("Throughput Kafka: (Messages): \t" + ((nMessages)*1000) / (endtime - startTime) + " Messages/s");
                                totalMessages += nMessages;
                                totalTime += endtime - startTime;
                                System.out.println("Packets sent: \t" + totalMessages);
                                if(totalTime / 1000 < 60)
                                    System.out.println("Time spent in seconds: \t" + totalTime / 1000);
                                else
                                    System.out.println("Time spent in minutes: \t" + (totalTime / 1000)/60);
                                System.out.println("Average rate: \t" + (totalMessages * bytePerMessage) / totalTime + " KB/s");
                                System.out.println("CPU load: \t" + getProcessCpuLoad() + "%");
                                startTime = System.currentTimeMillis();
                                nMessages = 0;
                                System.out.println("\n\n");
                            } else {
                                startTime = System.currentTimeMillis();
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        thread1.start();
    }

    private double getProcessCpuLoad(){

        try {
            MBeanServer mbs    = ManagementFactory.getPlatformMBeanServer();
            ObjectName name    = ObjectName.getInstance("java.lang:type=OperatingSystem");
            AttributeList list = mbs.getAttributes(name, new String[]{ "ProcessCpuLoad" });

            if (list.isEmpty())     return Double.NaN;

            Attribute att = (Attribute)list.get(0);
            Double value  = (Double)att.getValue();

            // usually takes a couple of seconds before we get real values
            if (value == -1.0)      return Double.NaN;
            // returns a percentage value with 1 decimal point precision
            return ((int)(value * 1000) / 10.0);
        }
        catch(Exception e) {
            return Double.NaN;
        }
    }

}
