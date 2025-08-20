# 存放生成 SQL表的命令，这些命令带主键、外键、注释。
# Here saves instructions to generate SQL tables, which contains primary key, foreign key commands and comments.

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
                      `adj_factor` DECIMAL(10, 5) NULL COMMENT '复权因子',
                    
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

''',

# =================================================================
#  表 1: 因子元数据表 (factor_metadata)
#  用途: 存储每个因子的定义、类别等静态信息。
# =================================================================
'factor_metadata':'''
                    CREATE TABLE `{table_name}` (
                      `factor_name` VARCHAR(50) NOT NULL COMMENT '因子名称，如“市盈率（PE）”',
                      `category` VARCHAR(20) NOT NULL COMMENT '因子类别，如“价值类”、“成长类”',
                      `definition` TEXT COMMENT '因子的计算公式或详细定义',
                      PRIMARY KEY (`factor_name`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='因子元数据定义表';
''',
# =================================================================
#  表 2: 因子面板数据表 (factor_panel_data)
#  用途: 存储所有股票在不同交易日的具体因子值，采用窄表结构。
# =================================================================
'factor_panel_data':'''
                    CREATE TABLE `{table_name}` (
                      `ts_code` VARCHAR(15) NOT NULL COMMENT '股票代码，如“600519.SH”',
                      `trade_date` DATE NOT NULL COMMENT '交易日期',
                      `factor_name` VARCHAR(50) NOT NULL COMMENT '因子名称，关联到 factor_metadata 表',
                      `factor_value` DOUBLE DEFAULT NULL COMMENT '因子值 (允许为NULL以处理缺失值)',
            
                      PRIMARY KEY (`ts_code`, `trade_date`, `factor_name`),
                      

                      -- 查询某个因子在某个交易日的所有股票表现
                      INDEX `idx_factor_date` (`factor_name`, `trade_date`),
                      

                      CONSTRAINT `fk_factor_panel_to_metadata`
                        FOREIGN KEY (`factor_name`)
                        REFERENCES `factor_metadata` (`factor_name`)
                        ON DELETE RESTRICT  -- 不允许删除一个还在被引用的因子
                        ON UPDATE CASCADE   -- 如果主表中的因子名称更新了，这张表也自动更新
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='因子面板数据表';


''',
'factor_panel_data_without_foreign_key': '''
                    CREATE TABLE `{table_name}`
                    (
                        `ts_code`     VARCHAR(15) NOT NULL COMMENT '股票代码，如“600519.SH”',
                        `trade_date`  DATE        NOT NULL COMMENT '交易日期',
                        `factor_name` VARCHAR(50) NOT NULL COMMENT '因子名称，关联到 factor_metadata 表',
                        `factor_value` DOUBLE DEFAULT NULL COMMENT '因子值 (允许为NULL以处理缺失值)',

                        PRIMARY KEY (`ts_code`, `trade_date`, `factor_name`),

                        -- 查询某个因子在某个交易日的所有股票表现
                        INDEX         `idx_factor_date` (`factor_name`, `trade_date`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='因子面板数据表';


'''

                    }
