import requests


def buscar_vendas_ifood(base_url, token, merchant_id, begin_sales_date, end_sales_date, page=1):
    url = f"{base_url}/financial/v1.0/sales"

    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json"
    }

    params = {
        "merchantId": merchant_id,
        "beginSalesDate": begin_sales_date,
        "endSalesDate": end_sales_date,
        "page": page
    }

    response = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=30
    )

    response.raise_for_status()
    return response.json()