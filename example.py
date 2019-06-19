from trade_api.poe_trade_interface import POETradeInterface

interface = POETradeInterface()
#search_args = {"type" : ["Bow"], "pdps_min" : "300"}
search_args = {"name":"Carcass Jack"}
url = interface.get_query_url(search_args)
interface.get_cheapest_query_results(url)
