import aiohttp


async def run_request(endpoint: str, **kwargs) -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.post(endpoint, **kwargs) as response:
            response_json = await response.json()

            return {"response": response, "response_json": response_json}
