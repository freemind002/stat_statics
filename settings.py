from pathlib import Path

src_path = Path(__file__).parent.joinpath("src")
src_path.mkdir(parents=False, exist_ok=True)

cat_dict = {
    "inbound": {
        "src_path": src_path,
        "cat": "inbound",
        "url": "https://stat.taiwan.net.tw/statistics/month/inbound/residence",
        "api_url": "https://stat.taiwan.net.tw/data/api/statistics/inbound/month/residence?year={}&monthStart={}&monthEnd={}&customCountry=-1",
    },
    "outbound": {
        "src_path": src_path,
        "cat": "outbound",
        "url": "https://stat.taiwan.net.tw/statistics/month/outbound/destination",
        "api_url": "https://stat.taiwan.net.tw/data/api/statistics/outbound/month/destination?year={}&monthStart={}&monthEnd={}&customCountry=-1",
    },
}
