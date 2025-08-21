from __future__ import annotations
"""
utils/tushare_router.py

通过“业务字段”自动选择并调用 Tushare 接口。
- 读取 JSON 注册表 (tushare_registry.json)，把业务字段映射到具体 endpoint 与字段名。
- 支持多端点抓取并自动合并。
- 行情用 ts.pro_bar，以支持 adj=['qfq','hfq','none'] 的复权方式。
- 复用项目内的 logger 与 easyPro 初始化。

用法示例：
    from utils.tushare_router import TushareRouter
    router = TushareRouter("utils/tushare_registry.json")

    # 1) 静态信息：
    df_basic = router.fetch(fields=["name","exchange","curr_type","list_date"], ts_code="600519.SH")

    # 2) 价格（可指定复权）：
    df_px = router.fetch_price(ts_code="600519.SH", start_date="20240101", end_date="20240630",
                               fields=["trade_date","open","close"], adj="qfq")

    # 3) 混合：自动拆分到多个接口并合并：
    df_mix = router.fetch(fields=["name","exchange","trade_date","pe","pb","total_mv"],
                          ts_code="600519.SH", start_date="20240101", end_date="20240630")
"""


import os, json
from typing import Dict, List, Optional, Literal, Any
import pandas as pd

# 你的项目内工具：日志 & Tushare 初始化
try:
    from utils.logger_config import app_logger as logger
except Exception:
    import logging
    logger = logging.getLogger("tushare_router_fallback")
    if not logger.handlers:
        h = logging.StreamHandler()
        logger.addHandler(h)
    logger.setLevel(logging.INFO)

try:
    from utils.utils import easyPro
except Exception:
    easyPro = None

# Tushare 库（pro_bar 需要直接引用）
try:
    import tushare as ts  # type: ignore
except Exception as e:
    ts = None


class Registry:
    """读取/管理（业务字段 -> endpoint/字段）映射。"""
    def __init__(self, path: str):
        self.path = path
        with open(path, "r", encoding="utf-8") as f:
            self.data = json.load(f)
        self._reverse = self._build_reverse_index()

    def _build_reverse_index(self) -> Dict[str, List[str]]:
        rev: Dict[str, List[str]] = {}
        for ep_name, info in self.data.items():
            if ep_name.startswith("_"):
                continue
            fmap = info.get("field_map", {})
            for biz_field in fmap.keys():
                rev.setdefault(biz_field, []).append(ep_name)
        return rev

    def find_endpoints_for_fields(self, fields: List[str]) -> List[str]:
        """简单贪心：每轮选择覆盖剩余字段最多的 endpoint。"""
        remaining = set(fields)
        selected: List[str] = []
        while remaining:
            best, cover = None, 0
            for ep_name, info in self.data.items():
                if ep_name.startswith("_"):
                    continue
                this_cover = len(remaining & set(info.get("field_map", {}).keys()))
                if this_cover > cover:
                    cover, best = this_cover, ep_name
            if not best or cover == 0:
                break
            selected.append(best)
            remaining -= set(self.data[best]["field_map"].keys())
        return selected

    def get(self, ep_name: str) -> Dict[str, Any]:
        return self.data[ep_name]


class TSClient:
    """统一 pro_api 与 pro_bar 的最小封装。"""
    def __init__(self, pro=None):
        if pro is not None:
            self.pro = pro
        elif easyPro is not None:
            self.pro = easyPro()
        else:
            if ts is None:
                raise ImportError("未安装 tushare，且未提供 easyPro。请先 pip install tushare")
            ts.set_token(os.getenv("API_KEY", ""))
            self.pro = ts.pro_api()

    def query(self, endpoint: str, params: Dict[str, Any]) -> pd.DataFrame:
        if endpoint == "pro_bar":
            if ts is None:
                raise ImportError("未安装 tushare，无法调用 pro_bar")
            # pro_bar 参数：ts_code/start_date/end_date/adj/asset/freq...
            # 不能直接传 fields；我们抓全再按需要裁剪
            adj = params.pop("adj", None)
            df = ts.pro_bar(api=self.pro, adj=adj, **params)  # type: ignore
            if df is None:
                return pd.DataFrame()
            return pd.DataFrame(df).reset_index(drop=True)

        # 标准接口
        logger.trace(params)
        df = self.pro.query(endpoint, **params)
        if df is None:
            return pd.DataFrame()
        return df.reset_index(drop=True)


class TushareRouter:
    def __init__(self, registry_path: str):
        self.registry = Registry(registry_path)
        self.client = TSClient()
        logger.info(f"✅ 已加载字段路由注册表：{registry_path}")

    def fetch(
        self,
        fields: List[str],
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        extra_params: Optional[Dict[str, Any]] = None,
        how: Literal["outer","inner","left","right"] = "outer"
    ) -> pd.DataFrame:
        """
        输入业务字段，自动选择 endpoint 取数并合并；列名最终保持为业务字段名。
        """
        extra_params = extra_params or {}
        endpoints = self.registry.find_endpoints_for_fields(fields)
        if not endpoints:
            logger.error(f"无法根据字段推断 endpoint：{fields}")
            return pd.DataFrame()

        pieces: List[pd.DataFrame] = []
        key_candidates = [["ts_code","trade_date"], ["ts_code"]]  # 合并键候选

        for ep_name in endpoints:
            info = self.registry.get(ep_name)
            endpoint = info["endpoint"]
            fmap = info.get("field_map", {})
            id_param = info.get("id_param")
            date_field = info.get("date_field")
            default_params = info.get("default_params", {})

            wanted = [f for f in fields if f in fmap]  # 该 endpoint 能提供的业务字段
            ts_fields = [fmap[f] for f in wanted]      # 映射到接口字段

            params = dict(default_params)
            # 标准接口：fields 逗号分隔；pro_bar 不在此处限制
            if ts_fields and endpoint != "pro_bar":
                keep = sorted(set(ts_fields + ([id_param] if id_param else []) + ([date_field] if date_field else [])))
                params["fields"] = ",".join([c for c in keep if c])

            if ts_code and id_param:
                params[id_param] = ts_code
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
            params.update(extra_params)

            df = self.client.query(endpoint, params)
            if df.empty:
                logger.warning(f"[{ep_name}] 无数据: params={params}")
                continue

            # 将接口字段名回映射为业务字段名
            rename_back = {fmap[b]: b for b in wanted if fmap.get(b) in df.columns}
            df = df.rename(columns=rename_back)

            pieces.append(df)

        if not pieces:
            return pd.DataFrame()

        # 自动合并
        out = pieces[0]
        for df in pieces[1:]:
            merged = False
            for keys in key_candidates:
                if all(k in out.columns for k in keys) and all(k in df.columns for k in keys):
                    out = out.merge(df, how=how, on=keys)
                    merged = True
                    break
            if not merged:
                commons = [c for c in out.columns if c in df.columns]
                if commons:
                    out = out.merge(df, how=how, on=commons)
                else:
                    out = pd.concat([out.reset_index(drop=True), df.reset_index(drop=True)], axis=1)

        return out

    def fetch_price(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
        fields: List[str] = ("trade_date","open","close"),
        adj: Literal["qfq","hfq","none","None"] = "none",
        extra_params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """用 pro_bar 拉行情，支持复权，返回列为业务字段名。"""
        info = self.registry.get("daily_bar")
        fmap = info["field_map"]
        params = dict(info.get("default_params", {}))
        params.update({
            "ts_code": ts_code,
            "start_date": start_date,
            "end_date": end_date,
        })
        # 复权参数
        if adj in ["none", "None", None]:
            params["adj"] = None
        else:
            params["adj"] = adj

        if extra_params:
            params.update(extra_params)

        df = self.client.query("pro_bar", params)
        if df.empty:
            return df

        # 只保留我们有映射的列，并回映射为业务字段名
        keep = [v for v in fmap.values() if v in df.columns]
        df = df[keep]
        inv = {v: k for k, v in fmap.items()}
        df = df.rename(columns=inv)

        # 最终按用户需求截列
        final_cols = [c for c in fields if c in df.columns]
        return df[final_cols] if final_cols else df
