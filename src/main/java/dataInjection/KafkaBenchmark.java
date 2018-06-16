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


    private KafkaBenchmark(){}

    static KafkaBenchmark getInstance(){
        return instance;
    }

    void startTime(){
        startTime = System.currentTimeMillis();
    }

    void stopAll(){
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
                    if (nMessages != 0) {
                        if (startTime != 0L){
                            endtime = System.currentTimeMillis();
                            System.out.println("Byte per message: " + bytePerMessage);
                            System.out.println("Throughput Kafka (Bytes): " + ((nMessages*bytePerMessage)/1000)/(endtime-startTime) + " MB/s");
                            System.out.println("Throughput Kafka: (Messages)" + (nMessages)/(endtime-startTime) + "Messages/ms");
                            totalMessages += nMessages;
                            totalTime += endtime-startTime;
                            System.out.println("Packets sent: " + totalMessages);
                            System.out.println("Time spent in seconds: " + totalTime/1000);
                            System.out.println("Average rate: " + (totalMessages/1000)/totalTime + "MB/s");
                            System.out.println("CPU load: " + getProcessCpuLoad() + "%");
                            startTime = System.currentTimeMillis();
                            nMessages = 0;
                            System.out.println("\n\n");
                        }
                        else {
                            startTime = System.currentTimeMillis();
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
