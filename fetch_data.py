import aiohttp
import json


async def fetch_data(api_token: str) -> tuple[str, str]:
    url = "https://common-api.wildberries.ru/api/v1/tariffs/commission"
    headers = {"Authorization": api_token}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            data = await response.json()
            report = data['report']

    return json.dumps(report, indent=2, ensure_ascii=False)
