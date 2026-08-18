import asyncio
import json
import sys

from mcp import Client
from mcp.client.subscriptions import ResourceUpdated
from server_modules.contract import RESOURCE_EVENT_LATEST, RESOURCE_SYSTEM_INFO


SERVER_URL = "http://127.0.0.1:8000/mcp"


async def main():
    print("Connecting to MCP server...")

    async with Client(SERVER_URL) as client:

        print("Connected")
        print("Protocol version:", client.protocol_version)

        print()
        print("Listing available tools...")
        tools = await client.list_tools()
        for tool in tools.tools:
            print("  -", tool.name, ":", (tool.description or "").split("\n")[0].strip())

        print()
        print("Listing available resources...")
        resources = await client.list_resources()
        for resource in resources.resources:
            print("  -", resource.uri)

        print()
        print("Opening MCP event subscription...")
        print("Watching:", RESOURCE_EVENT_LATEST)

        event_count = 0

        async with client.listen(
            resource_subscriptions=[RESOURCE_EVENT_LATEST]
        ) as subscription:

            print("Subscription active")
            print("Waiting for MCP events...")
            print("(Press CTRL+C to stop)")
            print()

            async for event in subscription:

                if isinstance(event, ResourceUpdated):

                    event_count += 1
                    print("=" * 60)
                    print("MCP EVENT #{0}".format(event_count))
                    print("Resource:", event.uri)

                    # Read the latest data after receiving notification
                    result = await client.read_resource(RESOURCE_EVENT_LATEST)
                    data = json.loads(result.contents[0].text)

                    print()
                    print("  id      :", data.get("id"))
                    print("  type    :", data.get("type"))
                    print("  source  :", data.get("source"))
                    print("  timestamp:", data.get("timestamp"))
                    print("  data    :", json.dumps(data.get("data", {}), ensure_ascii=False))

                    print("=" * 60)
                    print()
                    print("Waiting for next event...")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nClient stopped.")
        sys.exit(0)
