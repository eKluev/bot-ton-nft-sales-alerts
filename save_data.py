import requests
import time
from threading import Thread
from core import connector as conn

class Daemon(Thread):
    def __init__(self, name, delay):
        Thread.__init__(self)
        self.name = name
        self.delay = delay

    def run(self):
        self.royalty_wallet = "EQCLJAm49cgXcGe0coOpG2rHOs9efTQeY5Gfm_hlArTHvm8z"
        self.marketplaces = {"Getgems": "0:a3935861f79daf59a13d6d182e1640210c02f98e3df18fda74b8f5ab141abf18",
                             "Disintar": "0:eb2eaf97ea32993470127208218748758a88374ad2bbd739fc75c9ab3a3f233d",
                             "Getgems_old": "0:584ee61b2dff0837116d0fcb5078d93964bcbe9c05fd6a141b1bfca5d6a43e18"}
        while True:
            self.save_data()
            time.sleep(self.delay)

    def save_data(self):
        data = requests.get(f"https://tonapi.io/v1/blockchain/getTransactions?account={self.royalty_wallet}&minLt={self.get_max_lt()}")

        if data.status_code != 200:
            return None
        else:
            data: dict = data.json()
            
            if not data['transactions']:
                return None
        
        for transaction in reversed(data['transactions']):
            if 'in_msg' in transaction and 'source' in transaction['in_msg'] and 'address' in transaction['in_msg']['source']:
                if self.check_transaction_type(transaction['in_msg']['source']['address']) or transaction['in_msg']['msg_data'] == 'AAAAAE9mZmVyIHJveWFsaWVz':
                    result = self.get_nft_sale_data(transaction['in_msg']['source']['address'])
                    
                    if transaction['in_msg']['msg_data'] == 'AAAAAE9mZmVyIHJveWFsaWVz':
                        result['nft_sale_type'] = 'auction'
                    else:
                        result['nft_sale_type'] = 'instant buy'
                        
                    result['utime'] = transaction['utime']
                    result['lt'] = transaction['lt']
                    
                    q = f"INSERT IGNORE INTO sales(nft_address, name, marketplace, price, sale_type,  owner_address, attr_theme, attr_type, collection_address, collection_name, image, utime, ltime) VALUES ('{result['nft_address']}','{result['name']}','{result['marketplace']}',{result['price']},'{result['nft_sale_type']}','{result['owner_address']}','{result['theme']}','{result['type']}','{result['collection']['address']}','{result['collection']['name']}','{result['image']}',{result['utime']},{result['lt']})"
                    conn.make_query(q, commit=True)
                    time.sleep(15)
                else:
                    continue


    def check_transaction_type(self, address) -> bool:
        data = requests.get(f"https://tonapi.io/v1/account/getInfo?account={address}")

        if data.status_code != 200:
            time.sleep(5)
            return self.check_transaction_type(address)
        else:
            data: dict = data.json()
            
        if 'interfaces' not in data or data['interfaces'] is None:
            return False
        elif 'nft_sale_get_gems' in data['interfaces'] or 'nft_sale' in data['interfaces']:
            return True
        else:
            return False
                

    def get_nft_sale_data(self, nft_sale_address) -> dict:
        data = requests.get(f"https://tonapi.io/v1/blockchain/getTransactions?account={nft_sale_address}")

        if data.status_code != 200:
            time.sleep(5)
            return self.get_nft_sale_data(nft_sale_address)
        else:
            data: dict = data.json()
        
        result = dict()    
        transaction_from = list()
        transaction_to = list()
        
        for transaction in data['transactions']:
            if transaction['in_msg'] and 'source' in transaction['in_msg']:
                transaction_from.append(transaction['in_msg']['source']['address'])
            if transaction['out_msgs']:
                for t_out in transaction['out_msgs']:
                    transaction_to.append(t_out['destination']['address'])
                    
        transaction_from = list(set(transaction_from) - set(self.marketplaces.values()))
        transaction_to = list(set(transaction_to) - set(self.marketplaces.values()))
                
        nft_address = list(set(transaction_to) & set(transaction_from))
        owner_address = list(set(transaction_from) - set(nft_address))
        
        result['nft_address'] = self.get_bounceable_address(nft_address[0])
        result['owner_address'] = self.get_bounceable_address(owner_address[0])
        result.update(self.get_nft_data(result['nft_address']))

        result['price'] = 0
        for transaction in data['transactions']:
            if 'source' in transaction['in_msg'] and transaction['in_msg']['source']['address'] == owner_address[0]:
                price: float = round(float(int(transaction['in_msg']['value']) / 10 ** 9 - 1), 2)
                if result['price'] < price:
                    result['price'] = price
            
            if len(transaction['out_msgs']) == 4:
                marketplace = 'Unknown'
                for transaction in transaction['out_msgs']:
                    if transaction['source']['address'] in self.marketplaces.values():
                        marketplace = list(self.marketplaces.keys())[list(self.marketplaces.values()).index(transaction['source']['address'])]
                    elif transaction['destination']['address'] in self.marketplaces.values():
                        marketplace = list(self.marketplaces.keys())[list(self.marketplaces.values()).index(transaction['destination']['address'])]

                    result['marketplace'] = marketplace
                
            
        return result


    def get_nft_data(self, nft_address: str) -> dict:
        data = requests.get(f"https://tonapi.io/v1/nft/getItems?addresses={nft_address}")

        if data.status_code != 200:
            time.sleep(5)
            return self.get_nft_data(nft_address)
        else:
            data: dict = data.json()
            
        if 'nft_items' not in data or len(data['nft_items']) == 0:
            return self.get_nft_data(nft_address)
            
        for nft in data['nft_items']:
            nft['collection']['address'] = self.get_bounceable_address(nft['collection']['address'])
            collection: dict = nft['collection']
            name: str = nft['metadata']['name']
            image: str = nft['metadata']['image']
            
            for attr in nft['metadata']['attributes']:
                if attr['trait_type'] == 'type':
                    attr_type: str = attr['value']
                    
                if attr['trait_type'] == 'theme':
                    attr_theme: str = attr['value']
                    
        return {"collection": collection, "name": name, "image": image, "type": attr_type, "theme": attr_theme}
            
            
    def get_bounceable_address(self, base64_address:str) -> str:
        data = requests.get(f"https://tonapi.io/v1/account/getInfo?account={base64_address}")

        if data.status_code != 200:
            time.sleep(5)
            return self.get_bounceable_address(base64_address)
        else:
            data: dict = data.json()
            return data['address']['bounceable']
            
    def get_max_lt(self):
        q = "select max(ltime) as ltime from sales"
        return int(conn.make_query(q)[0]['ltime'])
