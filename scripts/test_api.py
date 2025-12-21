import tushare as ts
import os,sys
from dotenv import load_dotenv
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()   


token = os.getenv("TUSHARE_TOKEN")
pro = ts.pro_api(token)
df = ts.pro_bar(
    ts_code="000001.SZ",
    start_date="20251201",
    adj='qfq',
    end_date="20251221",
    freq="D",
    factors=["tor","vr"],
    adjfactor = True,
    api=pro
)
df.to_csv("df.csv")
print(df)