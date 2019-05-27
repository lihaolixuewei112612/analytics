package com.dtc.analytics.Struct;

import java.util.ArrayList;
import java.util.List;

public class data {
    static List<String> list = new ArrayList<>();
       public static List<String> getList(){
           return list;
       }
       public static void init(){
           list.clear();
       }
       public static boolean isNull() {
           if(list.size()==0)return  true;
           return false;

       }
    }


