package com.shang.sparkproject.test;


public class SingleTon {

    private SingleTon(){}

    private static volatile SingleTon singleTon;

    public static SingleTon getInstance(){
        if(singleTon==null){
            synchronized (SingleTon.class){
                if(singleTon==null){
                    singleTon=new SingleTon();
                }
            }
        }
        return singleTon;
    }

}
