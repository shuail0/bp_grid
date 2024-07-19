import base64
import json
import time
import requests
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization
from loguru import logger
from urllib.parse import urlencode
from functools import wraps
import time


def retry(max_retries=3, delay=1, exceptions=(Exception,)):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    logger.warning(f"重试 {func.__name__} ({retries}/{max_retries})，原因：{e}")
                    time.sleep(delay)
            # 最后一次尝试，如果还失败就让异常抛出
            return func(*args, **kwargs)
        return wrapper
    return decorator

class BpxClient:
    url = 'https://api.backpack.exchange/'
    private_key: ed25519.Ed25519PrivateKey

    def __init__(self):
        self.debug = False
        self.proxies = {
            'http': '',
            'https': ''
        }
        self.api_key = ''
        self.api_secret = ''
        self.window = 5000

    def init(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.private_key = ed25519.Ed25519PrivateKey.from_private_bytes(
            base64.b64decode(api_secret)
        )
        self.verifying_key = self.private_key.public_key()
        self.verifying_key_b64 = base64.b64encode(
            self.verifying_key.public_bytes(
                encoding=serialization.Encoding.Raw,
                format=serialization.PublicFormat.Raw
            )
        ).decode()

    # capital
    @retry(max_retries=3, delay=5, exceptions=(requests.exceptions.RequestException,))
    def balances(self):
        res = requests.get(url=f'{self.url}api/v1/capital', proxies=self.proxies,
                           headers=self.sign('balanceQuery', {}))
        if str(res.status_code) == "200":
            return res.json()
    @retry(max_retries=3, delay=5, exceptions=(requests.exceptions.RequestException,))
    def deposits(self):
        return requests.get(url=f'{self.url}wapi/v1/capital/deposits', proxies=self.proxies,
                            headers=self.sign('depositQueryAll', {})).json()
    @retry(max_retries=3, delay=5, exceptions=(requests.exceptions.RequestException,))
    def depositAddress(self, chain: str):
        params = {'blockchain': chain}
        return requests.get(url=f'{self.url}wapi/v1/capital/deposit/address', proxies=self.proxies, params=params,
                            headers=self.sign('depositAddressQuery', params)).json()
    @retry(max_retries=3, delay=5, exceptions=(requests.exceptions.RequestException,))
    def withdrawals(self, limit: int, offset: int):
        params = {'limit': limit, 'offset': offset}
        return requests.get(url=f'{self.url}wapi/v1/capital/withdrawals', proxies=self.proxies, params=params,
                            headers=self.sign('withdrawalQueryAll', params)).json()

    # history
    @retry(max_retries=3, delay=5, exceptions=(requests.exceptions.RequestException,))
    def orderHistoryQuery(self, symbol: str, limit: int, offset: int):
        params = {'symbol': symbol, 'limit': limit, 'offset': offset}
        return requests.get(url=f'{self.url}wapi/v1/history/orders', proxies=self.proxies, params=params,
                            headers=self.sign('orderHistoryQueryAll', params)).json()
    
    @retry(max_retries=3, delay=5, exceptions=(requests.exceptions.RequestException,))
    def fillHistoryQuery(self, symbol: str, limit: int, offset: int):
        params = {'limit': limit, 'offset': offset}
        if len(symbol) > 0:
            params['symbol'] = symbol
        return requests.get(url=f'{self.url}wapi/v1/history/fills', proxies=self.proxies, params=params,
                            headers=self.sign('fillHistoryQueryAll', params)).json()
    

    @retry(max_retries=3, delay=5, exceptions=(requests.exceptions.RequestException,))
    def ExeOrder(self, cid, symbol, side, orderType, timeInForce, quantity, price):
        params = {
            'clientId': cid,
            'symbol': symbol,
            'side': side,
            'orderType': orderType,
            'timeInForce': timeInForce,
            'quantity': quantity,
            'price': price
        }
        res = requests.post(url=f'{self.url}api/v1/order', proxies=self.proxies, data=json.dumps(params),
                            headers=self.sign('orderExecute', params))
        if str(res.status_code) == "200":
            return res.json()
        elif str(res.status_code) == "202":  # 订单提交了，但是未执行
            o = res.json()
            return {
                'clientId': cid,
                'createdAt': None,
                'executedQuantity': '0',
                'executedQuoteQuantity': '0',
                'id': o.get("id"),
                'orderType': orderType,
                'postOnly': False,
                'price': str(price),
                'quantity': str(quantity),
                'selfTradePrevention': 'RejectTaker',
                'side': side,
                'status': 'New',
                'symbol': symbol,
                'timeInForce': timeInForce,
                'triggerPrice': None
            }
        else:
            raise f"订单提交失败: {res.text}"

    # 获取挂单信息
    @retry(max_retries=3, delay=5, exceptions=(requests.exceptions.RequestException,))
    def getOpenOrder(self, symbol, orderId):
        params = {
            'symbol': symbol,
            'orderId': orderId,
        }
        res = requests.get(url=f'{self.url}api/v1/order', proxies=self.proxies, params=params,
                            headers=self.sign('orderQuery', params))
        if str(res.status_code) == "200":
            return res.json()
        elif str(res.status_code) == "404":  # 成交或者取消了就是404
            return None
        else:
            logger.error(f"订单查询失败: {res.text}，重试")
            time.sleep(5)

        # 取消未完成订单
    def cancelOrder(self, symbol, orderId):
        params = {
            'symbol': symbol,
            'orderId': orderId,
        }
        res = requests.delete(url=f'{self.url}api/v1/order', proxies=self.proxies, data=json.dumps(params),
                                headers=self.sign('orderCancel', params))
        if str(res.status_code) == "200":
            return res.json()
        elif str(res.status_code) == "202":  # 订单取消了，但是未执行
            return {
                'id': orderId
            }
        else:
            logger.error(f"订单取消失败: {res.text}，重试")
    
    # 获取所有未完成订单
    @retry(max_retries=3, delay=5, exceptions=(requests.exceptions.RequestException,))
    def getAllOpenOrders(self, symbol=None):
        params = {}
        if symbol:
            params = {'symbol': symbol}
        return requests.get(url=f'{self.url}api/v1/orders', proxies=self.proxies, params=params,
                            headers=self.sign('orderQueryAll', params)).json()

    # 取消所有未完成订单
    @retry(max_retries=3, delay=5, exceptions=(requests.exceptions.RequestException,))
    def cancelAllOpenOrders(self, symbol):
        params = {'symbol': symbol}
        return requests.delete(url=f'{self.url}api/v1/orders', proxies=self.proxies, data=json.dumps(params),
                               headers=self.sign('orderCancelAll', params)).json()

    # 获取历史订单
    @retry(max_retries=3, delay=5, exceptions=(requests.exceptions.RequestException,))
    def getHistoryOrders(self, symbol, limit=10, offset=0):
        params = {'symbol': symbol, 'limit': limit, 'offset': offset}
        return requests.get(url=f'{self.url}wapi/v1/history/orders', proxies=self.proxies, params=params,
                            headers=self.sign('orderHistoryQueryAll', params)).json()

    # 获取历史成交订单
    @retry(max_retries=3, delay=5, exceptions=(requests.exceptions.RequestException,))
    def getHistoryFilledOrders(self, symbol=None):
        params = {'symbol': symbol}
        return requests.get(url=f'{self.url}wapi/v1/history/fills', proxies=self.proxies, params=params,
                            headers=self.sign('fillHistoryQueryAll', params)).json()

    def sign(self, instruction: str, params: dict = None):
        timestamp = str(int(time.time() * 1000))
        window = '5000'
        body = {
            'instruction': instruction,
            **dict(sorted((params or {}).items())),
            'timestamp': timestamp,
            'window': window,
        }
        message = urlencode(body)
        signature = self.private_key.sign(message.encode())
        signature_b64 = base64.b64encode(signature).decode()
        return {
            'X-API-KEY': self.verifying_key_b64,
            'X-TIMESTAMP': timestamp,
            'X-WINDOW': window,
            'Content-Type': 'application/json',
            'X-SIGNATURE': signature_b64
        }
