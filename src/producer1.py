import xmlrpc.client


client = xmlrpc.client.ServerProxy("http://localhost:8000/")

client.produce(("item1", 10, "set"))


