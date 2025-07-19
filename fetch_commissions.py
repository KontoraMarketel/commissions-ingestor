import aiohttp
import json

async def fetch_commissions(api_token: str) -> tuple[str, str]:
    url = "https://common-api.wildberries.ru/api/v1/tariffs/commission"
    headers = {"Authorization": api_token}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                text = await response.text()
                raise Exception(f"WB API error: {response.status} — {text}")
            data = await response.json()

    return json.dumps(data, indent=2)
