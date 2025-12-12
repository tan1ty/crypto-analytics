import requests

def fetch_price():
    url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
    response = requests.get(url)
    return response.json()

if __name__ == "__main__":
    print(fetch_price())
