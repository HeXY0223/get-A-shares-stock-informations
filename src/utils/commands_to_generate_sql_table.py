generator = {
'stock_basic_info': '''
                    CREATE TABLE `{table_name}`
                    (
                        `ts_code`       VARCHAR(15) NOT NULL COMMENT 'Tushare''s unique code (Primary Key)',
                        `symbol`        VARCHAR(10)  DEFAULT NULL COMMENT 'Trading symbol',
                        `name`          VARCHAR(50)  DEFAULT NULL COMMENT 'Stock name',
                        `area`          VARCHAR(50)  DEFAULT NULL COMMENT 'Geographical area',
                        `industry`      VARCHAR(50)  DEFAULT NULL COMMENT 'Industry classification',
                        `market`        VARCHAR(10)  DEFAULT NULL COMMENT 'Market category (e.g., Main Board, ChiNext, STAR Market, CDR, BSE)',
                        `list_status`   VARCHAR(1)   DEFAULT NULL COMMENT 'Listing status',
                        `list_date`     DATE         DEFAULT NULL COMMENT 'Listing date',
                        `delist_date`   DATE         DEFAULT NULL COMMENT 'Delisting date',
                        `main_business` VARCHAR(700) DEFAULT NULL COMMENT 'Company''s main business and products',
                        PRIMARY KEY (`ts_code`),
                        INDEX `idx_industry` (`industry`),
                        INDEX `symbol` (`symbol`)
                    ) ENGINE = InnoDB COMMENT ='Stock basic information table';
''',
'index_basic_info': '''
                    create table `{table_name}`
                    (
                        `ts_code`     varchar(15)   not null comment 'Tushare''s unique code (Primary Key)',
                        `name`        varchar(50)   default null comment 'Index name',
                        `market`      varchar(10)   default null comment 'Market category (e.g., MSCI, CSI, SSE, CICC, SW, OTH)',
                        `publisher`   varchar(20)   default null comment 'Publisher',
                        `category`    varchar(20)   default null comment 'Category',
                        `base_date`   date          default null comment 'base date',
                        `base_point`  float         default null comment 'base point',
                        `exp_date`    date          default null comment 'Expire date',
                        `description`  varchar(300) default null comment 'Description',
                        primary key (`ts_code`)
                    )engine = innodb comment ='Index basic information table';
''',
'stock_daily':'''
                    CREATE TABLE `{table_name}` (
                      `ts_code` VARCHAR(15) NOT NULL COMMENT '股票代码，例如 600519.SH',
                      `trade_date` DATE NOT NULL COMMENT '交易日期',
                    
                      -- 不复权/除权数据 (Raw Data) --
                      `open` DECIMAL(10, 2) NULL COMMENT '开盘价 (不复权)',
                      `high` DECIMAL(10, 2) NULL COMMENT '最高价 (不复权)',
                      `low` DECIMAL(10, 2) NULL COMMENT '最低价 (不复权)',
                      `close` DECIMAL(10, 2) NULL COMMENT '收盘价 (不复权)',
                    
                      -- 通用数据 (Common Data) --
                      `volume` BIGINT NULL COMMENT '成交量 (手)',
                      `amount` DECIMAL(20, 2) NULL COMMENT '成交额 (千元)',
                    
                      -- 前复权数据 (Forward-Adjusted) --
                      `open_qfq` DECIMAL(10, 2) NULL COMMENT '开盘价 (前复权)',
                      `high_qfq` DECIMAL(10, 2) NULL COMMENT '最高价 (前复权)',
                      `low_qfq` DECIMAL(10, 2) NULL COMMENT '最低价 (前复权)',
                      `close_qfq` DECIMAL(10, 2) NULL COMMENT '收盘价 (前复权)',
                      `pre_close_qfq` DECIMAL(10, 2) NULL COMMENT '昨收价 (前复权)',
                      `price_change_qfq` DECIMAL(10, 2) NULL COMMENT '涨跌额 (前复权)',
                    
                      -- 后复权数据 (Backward-Adjusted) --
                      `open_hfq` DECIMAL(10, 2) NULL COMMENT '开盘价 (后复权)',
                      `high_hfq` DECIMAL(10, 2) NULL COMMENT '最高价 (后复权)',
                      `low_hfq` DECIMAL(10, 2) NULL COMMENT '最低价 (后复权)',
                      `close_hfq` DECIMAL(10, 2) NULL COMMENT '收盘价 (后复权)',
                      `pre_close_hfq` DECIMAL(10, 2) NULL COMMENT '昨收价 (后复权)',
                      `price_change_hfq` DECIMAL(10, 2) NULL COMMENT '涨跌额 (后复权)',
                    
                    
                      -- 主键定义 (Primary Key) --
                      PRIMARY KEY (`ts_code`, `trade_date`)
                    ) ENGINE=InnoDB COMMENT='股票日线行情数据表';
                    
''',
'index_daily':'''
                    CREATE TABLE `{table_name}` (
                      `ts_code` VARCHAR(15) NOT NULL COMMENT 'TS指数代码',
                      `trade_date` DATE NOT NULL COMMENT '交易日',
                    
                      -- 指数行情数据 --
                      `open` DECIMAL(10, 2) NULL COMMENT '开盘点位',
                      `high` DECIMAL(10, 2) NULL COMMENT '最高点位',
                      `low` DECIMAL(10, 2) NULL COMMENT '最低点位',
                      `close` DECIMAL(10, 2) NULL COMMENT '收盘点位',
                      `pre_close` DECIMAL(10, 2) NULL COMMENT '昨日收盘点位',
                      `change` DECIMAL(10, 2) NULL COMMENT '涨跌点',
                      `pct_chg` DECIMAL(8, 4) NULL COMMENT '涨跌幅（%）',
                      `vol` BIGINT NULL COMMENT '成交量（手）',
                      `amount` DECIMAL(20, 2) NULL COMMENT '成交额（千元）',
                    
                      -- 主键定义 --
                      PRIMARY KEY (`ts_code`, `trade_date`)
                    ) ENGINE=InnoDB COMMENT='指数日行情数据表';

'''


                    }
