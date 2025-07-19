import aiohttp
import json


async def fetch_commissions(api_token: str) -> tuple[str, str]:
    url = "https://common-api.wildberries.ru/api/v1/tariffs/commission"
    headers = {"Authorization": api_token}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            data = await response.json()

    return json.dumps(data, indent=2)
