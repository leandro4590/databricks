from datetime import datetime, date

BASE_URL = "https://merchant-api.ifood.com.br"
TOKEN = "SEU_ACCESS_TOKEN"
MERCHANT_ID = "SEU_MERCHANT_ID"

END_SALES_DATE = datetime.now().strftime("%Y-%m-%d")
BEGIN_SALES_DATE = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
PAGE = 1