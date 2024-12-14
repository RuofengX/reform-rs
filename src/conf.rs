use std::collections::HashSet;

use anyhow::bail;
use polars::prelude::*;
use serde_derive::{Deserialize, Serialize};

type Any<T> = &'static [T];
pub const NULL_MARK: Any<&'static str> = &["-", "_"];

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct Entity {
    pub col_id: &'static str,
    pub col_id_alter: Option<&'static str>,
    pub col_bank_id: Option<&'static str>,
    pub col_name: Option<&'static str>,
}
impl Entity {
    pub fn id_expr(&self) -> Expr {
        if let Some(alter) = self.col_id_alter {
            coalesce(&[col(self.col_id), col(alter)])
        } else {
            col(self.col_id)
        }
    }
    pub fn bank_id_expr(&self) -> Expr {
        if let Some(x) = self.col_bank_id {
            col(x)
        } else {
            lit(NULL)
        }
    }

    pub fn name_expr(&self) -> Expr {
        if let Some(x) = self.col_name {
            col(x)
        } else {
            lit(NULL)
        }
    }

    pub fn all_columns(&self) -> Vec<&'static str> {
        let mut ret = vec![];
        ret.push(self.col_id);
        if let Some(col_name) = self.col_name {
            ret.push(col_name);
        }
        if let Some(alter) = self.col_id_alter {
            ret.push(alter);
        }
        if let Some(bank_id) = self.col_bank_id {
            ret.push(bank_id);
        }
        ret
    }
}

#[derive(Clone, Copy)]
pub enum Trans {
    /// 财付通等无方向账单
    Simple {
        col_amount: &'static str,
        from: Entity,
        to: Entity,
        prime_id: Option<&'static str>, // 查询主体列名
    },
    /// 有向账单设置可详细设置方向列和方向字符串
    Directed {
        /// 金额
        col_amount: &'static str,

        /// 查询主体
        one: Entity,

        /// 交易对方
        other: Entity,

        /// 方向标记列
        col_direct: &'static str,

        /// 借、出等
        value_out: &'static str,

        /// one字段是否是查询主体
        ///
        /// 在极少部份账单中，账单是混杂的，
        /// 即：出、入方向不是相较主体，而是随机的。
        /// 这种情况这个字段就是false
        ///
        /// 正常情况one的主体就是查询的主体
        /// 这个字段设置为true
        one_is_prime: bool,
    },
}
impl Trans {
    pub fn all_columns(&self) -> Vec<&'static str> {
        let mut ret = vec![];
        match self.clone() {
            Self::Directed {
                col_amount,
                one,
                other,
                col_direct,
                value_out: _,
                one_is_prime: _,
            } => {
                ret.push(col_amount);
                ret.extend(one.all_columns());
                ret.extend(other.all_columns());
                ret.push(col_direct);
            }
            Self::Simple {
                col_amount,
                from,
                to,
                prime_id,
            } => {
                ret.push(col_amount);
                ret.extend(from.all_columns());
                ret.extend(to.all_columns());
                if let Some(prime_id) = prime_id {
                    ret.push(prime_id)
                }
            }
        }
        ret
    }
}

#[derive(Clone, Copy)]
pub struct Time {
    pub col: &'static str,
    pub fmt: &'static str,
    pub fmt_alter: Option<&'static str>,
}

#[derive(Clone, Copy)]
pub struct Config {
    /// 配置名
    pub name: &'static str,
    /// 去重字段
    pub col_id: Option<&'static str>,
    pub time: Time,
    pub trans: Trans,
}

impl Config {
    fn all_columns(&self) -> HashSet<&'static str> {
        let mut ret = vec![self.time.col];
        ret.extend(self.trans.all_columns());
        HashSet::from_iter(ret.into_iter())
    }

    pub fn check(&self, sig: &[String]) -> bool {
        self.all_columns()
            .iter()
            .map(|x| x.to_string())
            .all(|x| sig.contains(&x))
    }
}

impl Config {
    pub fn auto_detect(sig: &[String]) -> anyhow::Result<Self> {
        for &i in Self::ALL {
            if i.check(sig) {
                return Ok(i);
            }
        }
        bail!("未定义的文件表头 - {:?}", sig)
    }
    pub const ALL: &[Config] = &[
        Self::GF_BANK,
        Self::GF_3,
        Self::JASS,
        Self::JZ,
        Self::QQ,
        Self::GENERIC1,
    ];
    pub const GF_BANK: Config = Config {
        name: "国反-银行",
        col_id: None,
        time: Time {
            col: "交易时间",
            fmt: "%Y%m%d%H%M%S",
            fmt_alter: Some("%Y%m%d"),
        },
        trans: Trans::Directed {
            col_amount: "金额",
            one: Entity {
                col_id: "查询账号",
                col_id_alter: None,
                col_bank_id: Some("查询账号"),
                col_name: Some("查询账号姓名"),
            },
            other: Entity {
                col_id: "对方账号卡号",
                col_id_alter: None,
                col_bank_id: Some("对方账号卡号"),
                col_name: Some("对方账号姓名"),
            },
            col_direct: "借贷标志",
            value_out: "借",
            one_is_prime: true,
        },
    };
    pub const GF_3: Config = Config {
        name: "国反-三方",
        col_id: Some("支付订单号"),
        time: Time {
            col: "交易时间",
            fmt: "%Y%m%d%H%M%S",
            fmt_alter: Some("%Y%m%d"),
        },
        trans: Trans::Simple {
            col_amount: "交易金额",
            from: Entity {
                col_id: "付款方的支付帐号",
                col_id_alter: None,
                col_bank_id: Some("付款方银行卡所属银行卡号"),
                col_name: None,
            },
            to: Entity {
                col_id: "收款方的支付帐号",
                col_id_alter: Some("收款方的商户号"),
                col_bank_id: Some("收款方银行卡所属银行卡号"),
                col_name: Some("收款方的商户名称"),
            },
            prime_id: Some("支付帐号"),
        },
    };
    pub const JASS: Config = Config {
        name: "银联JASS",
        // 银联很多字段都是反的
        col_id: None,
        time: Time {
            col: "所属发卡银行机构代码",
            fmt: "%Y-%m-%d %H:%M:%S",
            fmt_alter: None,
        },
        trans: Trans::Directed {
            col_amount: "受理机构代码",
            one: Entity {
                col_id: "银行卡号（交易卡号）",
                col_id_alter: None,
                col_bank_id: Some("银行卡号（交易卡号）"),
                col_name: None,
            },
            other: Entity {
                col_id: "交易地点",
                col_id_alter: None,
                col_bank_id: None,
                col_name: Some("交易地点"),
            },
            col_direct: "交易渠道",
            value_out: "消费",
            one_is_prime: true,
        },
    };
    pub const JZ: Config = Config {
        name: "经侦",
        col_id: Some("交易流水号"),
        time: Time {
            col: "交易时间",
            fmt: "%Y%m%d%H%M%S",
            fmt_alter: None,
        },
        trans: Trans::Directed {
            col_amount: "金额",
            one: Entity {
                col_id: "查询账号",
                col_id_alter: None,
                col_bank_id: Some("查询账号"),
                col_name: None,
            },
            other: Entity {
                col_id: "对方账号卡号",
                col_id_alter: None,
                col_bank_id: Some("对方账号卡号"),
                col_name: Some("对方账号姓名"),
            },
            col_direct: "借贷标志",
            value_out: "借",
            one_is_prime: true,
        },
    };
    pub const QQ: Config = Config {
        name: "QQ钱包",
        col_id: None, //有一列支付订单号，但是均为空
        time: Time {
            col: "交易时间",
            fmt: "%Y-%m-%d %H:%M:%S",
            fmt_alter: None,
        },
        trans: Trans::Simple {
            col_amount: "交易金额",
            from: Entity {
                col_id: "付款方支付账号",
                col_id_alter: None,
                col_bank_id: Some("付款银行卡号"),
                col_name: Some("付款方开户名"),
            },
            to: Entity {
                col_id: "收款支付帐号",
                col_id_alter: None,
                col_bank_id: Some("收款银行卡号"),
                col_name: Some("收款方的商户名称"),
            },
            prime_id: None, // QQ钱包调证没有查询主体这一列
        },
    };
    pub const GENERIC1: Config = Config {
        name: "通用格式-1",
        col_id: None,
        time: Time {
            col: "交易日期",
            fmt: "%Y-%m-%d %H:%M:%S",
            fmt_alter: None,
        },
        trans: Trans::Directed {
            col_amount: "交易金额",
            one: Entity {
                col_id: "查询账户",
                col_id_alter: None,
                col_bank_id: None,
                col_name: Some("查询姓名"),
            },
            other: Entity {
                col_id: "对方账户",
                col_id_alter: None,
                col_bank_id: None,
                col_name: Some("对方姓名"),
            },
            col_direct: "借贷标识",
            value_out: "出",
            one_is_prime: false, // 奇葩账单
        },
    };
}
