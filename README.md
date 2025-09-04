# daletou
Python 爬取大乐透数据，从第一期至今

MySQL 表结构创建脚本





CREATE TABLE IF NOT EXISTS dlt_draws (    


    period VARCHAR(20) NOT NULL,     
  
    draw_date DATE NOT NULL,                 -- 开奖日期（YYYY-MM-DD）
  
    red1 TINYINT UNSIGNED NOT NULL,
  
    red2 TINYINT UNSIGNED NOT NULL,
  
    red3 TINYINT UNSIGNED NOT NULL,
  
    red4 TINYINT UNSIGNED NOT NULL,
  
    red5 TINYINT UNSIGNED NOT NULL,          -- 前区 5 个号码（01-35）
  
    blue1 TINYINT UNSIGNED NOT NULL,
  
    blue2 TINYINT UNSIGNED NOT NULL,         -- 后区 2 个号码（01-12）
  
    sales BIGINT DEFAULT NULL,               -- 销售额（可选，单位：元）
  
    jackpot BIGINT DEFAULT NULL,             -- 奖池（可选，单位：元）
  
    first_prize_count INT DEFAULT NULL,      -- 一等奖注数（可选）
  
    first_prize_amount BIGINT DEFAULT NULL,  -- 一等奖单注奖金（可选，元）
  
    source VARCHAR(255) DEFAULT NULL,        -- 数据来源 URL / 站点
  
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  
    PRIMARY KEY (period),
  
    INDEX (draw_date) 
) 
  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



执行效果部分：

[OK ] 写入 10014 2010-02-01 [18, 19, 30, 33, 35]+[2, 4]

[OK ] 写入 10013 2010-01-30 [18, 21, 22, 26, 34]+[9, 12]

[OK ] 写入 10012 2010-01-27 [20, 24, 30, 31, 33]+[5, 12]

[OK ] 写入 10011 2010-01-25 [2, 3, 11, 27, 32]+[8, 11]

[OK ] 写入 10010 2010-01-23 [15, 22, 24, 32, 35]+[2, 9]

[OK ] 写入 10009 2010-01-20 [2, 5, 13, 27, 32]+[3, 11]

[OK ] 写入 10008 2010-01-18 [11, 23, 25, 31, 32]+[5, 11]

[OK ] 写入 10007 2010-01-16 [7, 23, 26, 29, 32]+[8, 11]

[OK ] 写入 10006 2010-01-13 [3, 13, 19, 30, 34]+[2, 5]

[OK ] 写入 10005 2010-01-11 [4, 17, 19, 22, 30]+[2, 3]

[OK ] 写入 10004 2010-01-09 [3, 6, 21, 24, 34]+[4, 6]

[OK ] 写入 10003 2010-01-06 [5, 14, 23, 27, 30]+[2, 8]

[OK ] 写入 10002 2010-01-04 [4, 23, 25, 26, 30]+[7, 10]

[OK ] 写入 10001 2010-01-02 [2, 6, 7, 12, 27]+[2, 8]

[INFO] 发现 stop period 10001，结束抓取（已写入本期）。

[INFO] 总共写入 2360 条记录。
