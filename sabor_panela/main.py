import pandas as pd
from config import BASE_URL, TOKEN, MERCHANT_ID, BEGIN_SALES_DATE, END_SALES_DATE, PAGE
from api_client import buscar_vendas_ifood


def main():
    dados = buscar_vendas_ifood(
        base_url=BASE_URL,
        token=TOKEN,
        merchant_id=MERCHANT_ID,
        begin_sales_date=BEGIN_SALES_DATE,
        end_sales_date=END_SALES_DATE,
        page=PAGE
    )

    print("Resposta da API:")
    print(dados)

    registros = dados.get("sales", [])

    df = pd.json_normalize(registros)

    print("\nDataFrame:")
    print(df.head())

    df.to_csv("vendas_ifood.csv", index=False, encoding="utf-8-sig")
    print("\nArquivo vendas_ifood.csv salvo com sucesso.")


if __name__ == "__main__":
    main()