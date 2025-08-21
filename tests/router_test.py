from utils.tushare_router import TushareRouter
router = TushareRouter("../src/utils/tushare_registry.json")
df_basic = router.fetch(fields=["name","exchange","curr_type","list_date"], ts_code="600519.SH")
print(df_basic)