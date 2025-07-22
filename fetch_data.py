import asyncio
import logging

import aiohttp
import json


async def fetch_data(api_token: str) -> tuple[str, str]:
    url = "https://common-api.wildberries.ru/api/v1/tariffs/commission"
    headers = {"Authorization": api_token}

    async with aiohttp.ClientSession() as session:

        while True:  # retry loop
            async with session.get(url, headers=headers) as response:
                if response.status == 429:
                    retry_after = int(response.headers['X-Ratelimit-Retry'])
                    logging.warning(
                        f"Rate limited (429). Retrying after {retry_after} seconds...")
                    await asyncio.sleep(retry_after)
                    continue  # retry

                response.raise_for_status()
                data = await response.json()
                report = data['report']
                break

    return json.dumps(report, indent=2, ensure_ascii=False)
