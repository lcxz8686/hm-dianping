package com.hmdp.utils;

import com.hmdp.dto.UserDTO;

/**
 * ThreadLocal 为变量在每个线程中创建了一个副本，这样每个线程都可以访问自己内部的副本变量。
 */
public class UserHolder {
    private static final ThreadLocal<UserDTO> tl = new ThreadLocal<>();

    public static void saveUser(UserDTO user){
        tl.set(user);
    }

    public static UserDTO getUser(){
        return tl.get();
    }

    public static void removeUser(){
        tl.remove();
    }
}
