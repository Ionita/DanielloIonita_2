package controllers;

import entities.Message;

public class Monitor {

    private Integer hour = -1;
    private Integer day = 0;
    private Integer week = 0;
    private Integer lifetime = 0;
    private Integer currentHour = -1;
    private Integer currentDay = -1;
    private Integer currentWeek = -1;
    private Integer currentYear = -1;



    private Long right = 0L;
    private Long left = 0L;

    public void rotation (Message m) {

        int isRight = 1;
        if (hour == -1) {
            //arriva il primo dato
            currentDay = m.getDay();
            currentHour = m.getHour();
            currentWeek = m.getWeek();
            currentYear = m.getYear();
            hour = 0;
        }

        if ((currentHour.equals(m.getHour()) && (currentDay.equals(m.getDay())) && currentWeek.equals(m.getWeek())) && currentYear.equals(m.getYear())) {
            //amicizia nella stessa ora dello stesso giorno
            hour += m.getCount().intValue();
            day += m.getCount().intValue();
            week += m.getCount().intValue();
            lifetime += m.getCount().intValue();
        }

        else if ((m.getHour() > currentHour) && currentDay.equals(m.getDay()) && currentWeek.equals(m.getWeek()) && currentYear.equals(m.getYear())) {
            //abbiamo scalato riga
            hour = 0;
            currentHour = m.getHour();
            hour += m.getCount().intValue();
            day += m.getCount().intValue();
            week += m.getCount().intValue();
            lifetime += m.getCount().intValue();
        }

        else if ((m.getHour() < currentHour) && (currentDay.equals(m.getDay()))) {
            isRight = 0;
            System.out.println("error \t" + "currentHour: " + currentHour + ", getHour: " + m.getHour());
        }

        if ((m.getDay() > currentDay) && currentWeek.equals(m.getWeek()) && currentYear.equals(m.getYear()))  {
            hour = 0;
            day = 0;
            currentHour = m.getHour();
            currentDay = m.getDay();
            hour += m.getCount().intValue();
            day += m.getCount().intValue();
            week += m.getCount().intValue();
            lifetime += m.getCount().intValue();
        }

        else if ((m.getDay() < currentDay) && currentWeek.equals(m.getWeek()) && currentYear.equals(m.getYear())){
            isRight = 0;
            System.out.println("error \t" + "currentDay: " + currentDay + ", getDay: " + m.getDay());
        }

        if ((m.getWeek() > currentWeek) && currentYear.equals(m.getYear())) {
            //settimana dopo
            hour = 0;
            day = 0;
            week = 0;
            currentHour = m.getHour();
            currentDay = m.getDay();
            currentWeek = m.getWeek();
            hour += m.getCount().intValue();
            day += m.getCount().intValue();
            week += m.getCount().intValue();
            lifetime += m.getCount().intValue();

        }

        else if ((m.getWeek() < currentWeek) && (currentYear.equals(m.getYear()))){
            isRight = 0;
            System.out.println("settimane non in sequenza");
        }

        if (m.getYear() > currentYear) {
            hour = 0;
            day = 0;
            week = 0;
            currentHour = m.getHour();
            currentDay = m.getDay();
            currentWeek = m.getWeek();
            currentYear = m.getYear();
            hour += m.getCount().intValue();
            day += m.getCount().intValue();
            week += m.getCount().intValue();
            lifetime += m.getCount().intValue();
        }
        if (isRight == 1)
            right++;
        else
            left++;
        printable(isRight);

    }

    public Monitor(){
        KafkaConsumer kc = new KafkaConsumer();
        kc.setAttributes(this);
        kc.runConsumer("monitor2");
    }

    private void printable (int isLeft) {
        //System.out.println("Ora: " + currentHour + "\t, giorno: " + currentDay + "\t,settimana: " + currentWeek + "\t, anno: " + currentYear);
        if(isLeft == 0)
            System.out.println(hour + "\t" + day  + "\t" + week + "\t" + lifetime + "\t: right packets: " + right + "\t: left packets" + left );

    }

    private int chour = -1;
    private int cday = -1 ;
    private int cweek = -1;
    private int cyear = -1;

    public void makeCheck(Message m){

        //initialization
        if (chour == -1){
            chour = m.getHour();
            cday = m.getDay();
            cweek = m.getWeek();
            cyear = m.getYear();
        }

        //same hour, day, week, year
        if(m.getHour().equals(chour) && m.getDay().equals(cday) && m.getWeek().equals(cweek)){
            sumAll(0, m);
        }
        // next hour, same day, week, year
        else if(m.getHour() > chour && m.getDay().equals(cday) && m.getWeek().equals(cweek)){
            sumAll(1, m);
        }
        //next day, same week, year
        else if(m.getDay() > cday && m.getWeek().equals(cweek)){
            sumAll(2, m);
        }
        // next week, same year
        else if(m.getWeek() > cweek){
            sumAll(3, m);
        }
        else if (m.getWeek() < cweek) {
            chour = m.getHour();
            cday = m.getDay();
            cweek = m.getWeek();
            cyear = m.getYear();
            sumAll(0, m);
        }
        //error
        else
            System.out.println("currentHour: " + chour + ", m.hour: " + m.getHour() + ", currentDay: " + cday + ", m.day: " + m.getDay() + ", currentWeek: " + cweek+ ", m.week: "+ m.getWeek()+ ", currentYear: " + cyear + ", m.year: " + m.getYear());


    }

    private void sumAll(int type, Message m){
        if(type > 0) {
            hour = 0;
            chour = m.getHour();
        }
        if (type > 1) {
            day = 0;
            cday = m.getDay();
        }
        if (type > 2) {
            week = 0;
            cweek = m.getWeek();
        }

        hour += m.getCount().intValue();
        day += m.getCount().intValue();
        week += m.getCount().intValue();
        lifetime += m.getCount().intValue();
    }




    /*private ArrayList<Integer[]> list = new ArrayList<>();

    private Integer numberOfYears = 0;
    private Integer startingYear;

    public Monitor(){

        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(80000);
                int i = 0;
                for (Integer[] z : list) {
                    saveToCsv(z, i);
                    i++;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        thread.start();

        KafkaConsumer kc = new KafkaConsumer();
        kc.setAttributes(this);
        kc.runConsumer("monitor");
    }

    private void saveElement(Message st) {

        if (numberOfYears == 0)
            startingYear = st.getYear();

        if (st.getYear() > startingYear + numberOfYears - 1) {
            numberOfYears++;
            list.add(new Integer[365]);
            Arrays.fill(list.get(st.getYear() - startingYear), 0);
        }

        list.get(st.getYear() - startingYear)[st.getDay() - 1] += st.getCount().intValue();
    }



    public void printReceivedMessage(Message st) {
        saveElement(st);
        *//*for (Integer[] i: list) {
            for (Integer j: i)
                System.out.println("posizione: "+ i + ",valore: " + j + "\n");
        }*//*


     *//*System.out.println("Day: " + st.getDay() + "\n" +
                            "Week:  " + st.getWeek() + "\n" +
                            "Year: " + st.getYear() + "\n" +
                            "value: " + st.getCount() + "\n\n");
    }*//*

    }

    public void saveToCsv (Integer[] deNitto, Integer position) {
        try {
            BufferedWriter br = new BufferedWriter(new FileWriter("denitto" + "_" + position + ".csv"));
            StringBuilder sb = new StringBuilder();
            int i = 0;
            for (Integer element: deNitto) {
                sb.append(i+1);
                sb.append(",");
                sb.append(element);
                sb.append(System.lineSeparator());
                i++;
            }
            br.write(sb.toString());
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/

}