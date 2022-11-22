# Redis黑马视频教程项目

## 介绍
本项目是根据黑马程序员的BiliBili视频课进行编写的！完全按照视频的顺序进行编写，主要是基于SpringBoot项目的Redis解决策略，大致涉及到Redis缓存穿透、雪崩、击穿等问题；Redis分布式锁、Redis消息队列、分布式缓存、多级缓存、Redis底层原理。

## 博客地址
本人的博客路径(含代码解析)免费！： **https://blog.csdn.net/weixin_43715214/article/details/125505311** 

BiliBili视频的路径：https://www.bilibili.com/video/BV1cr4y1671t

## 项目部署
1. 这里是列表文本先将nginx-1.80-zip解压，静态页面资源都在里面了，双击nginx.exe文件（弹出一个小黑框（闪一下））代表静态资源启动成功；

2. 使用navicat运行hmdq.sql

3. 部署Redis，修改application.yml里面的mysql地址、redis地址。

## 软件架构
使用Spring、SpringMVC、MyBatis框架，基于SpringBoot快速构建。

## 仓库结构
本文完全依照黑马程序员课程目录，依次用Redis实现：
![黑马程序员课程目录](images/image.png)
1. Redis基础知识&&数据结构
1. 短信登入
1. 商户查询缓存
1. 优惠券秒杀
1. 达人探店
1. 好友关注
1. 附近商户
1. 用户签到
1. UV统计



