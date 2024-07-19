import json
import time
import threading
import random
import string
import logging
import base64
from websocket import (
    ABNF,
    create_connection,
    WebSocketException,
    WebSocketConnectionClosedException
)
from nacl.signing import SigningKey
from nacl.encoding import Base64Encoder
from loguru import logger

from bpx.bpx import *
from bpx.bpx_pub import *
from datetime import datetime
import random
import string

class SpotGrid(threading.Thread):
    def __init__(self, api_key, secret, symbol, max_price, min_price, gap_percent, price_precision, quantity, quantity_precision, strategy_prefix):
        threading.Thread.__init__(self)
        self.api_key = api_key
        self.secret = secret
        self.symbol = symbol
        self.max_price = max_price
        self.min_price = min_price
        self.gap_percent = gap_percent
        self.price_precision = price_precision
        self.quantity = quantity
        self.quantity_precision = quantity_precision
        self.strategy_prefix = strategy_prefix
        self.buy_order = None
        self.sell_order = None
        self.bid_price = None
        self.ask_price = None
        self.grid_size = (self.max_price - self.min_price) * self.gap_percent  # 定义 grid_size
        self.stream_url = "wss://ws.backpack.exchange/"
        self.ws = None
        self.depth = {"asks": [], "bids": []}  # 深度数据
        self.logger = logger  # 初始化日志记录器
        self.bpx = BpxClient()
        self.bpx.init(api_key, secret)
        self.create_ws_connection()
 
        
    def get_client_id(self, size=6, chars=string.digits):
        """生成客户端订单号

        Args:
            size (int, optional): 位数(可选),默认为6.
            chars (_type_, optional): _description_. Defaults to string.digits.

        Returns:
            _type_: 返回策略唯一编号+随机数
        """
        id = "".join(random.choice(chars) for _ in range(size))
        return int(f"{self.strategy_prefix}{id}")
    
    def get_balance(self):
        """获取交易对的余额"""

        b = self.bpx.balances()
        if b:
            s = self.symbol.split("_")
            b1 = float(b.get(s[0], {}).get("available", 0.0))
            b2 = float(b.get(s[1], {}).get("available", 0.0))
            return b1, b2
        else:
            return None, None
    
    def get_bid_ask_price(self):
        """获取买卖价格"""
        self.depth = Depth(self.symbol)
        if self.depth:
            self.depth['asks'] = [[float(price), float(amount)] for price, amount in self.depth['asks']]
            self.depth['bids'] = [[float(price), float(amount)] for price, amount in self.depth['bids']]
            self.depth['lastUpdateId'] = int(self.depth['lastUpdateId'])
            return float(self.depth['bids'][-1][0]), float(self.depth['asks'][0][0])
        else:
            return None, None

    def generate_signature(self, timestamp, window):
        message = f'instruction=subscribe&timestamp={timestamp}&window={window}'
        signing_key = SigningKey(self.secret, encoder=Base64Encoder)
        signed = signing_key.sign(message.encode('utf-8'))
        signature = base64.b64encode(signed.signature).decode('utf-8')
        verifying_key = base64.b64encode(signing_key.verify_key.encode()).decode('utf-8')
        return verifying_key, signature

    def get_client_id(self, size=6, chars=string.digits):
        id = "".join(random.choice(chars) for _ in range(size))
        return int(f"{self.strategy_prefix}{id}")
    
    def round_to(self, number, precision):
        return float(f'{number:.{precision}f}')
    

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            raise f"Failed to decode message as JSON, message: {message}"

        # 确保 'data' 键存在且其值不是 None
        if 'data' not in data or data['data'] is None:
            raise "'data' field is missing or None in the message, message: {message}"

        event = data['data'].get('e')
        if event is None:
            logger.error("'e' field is missing or None in the 'data'")
            raise "'e' field is missing or None in the 'data'"
        if event == 'depth':
            self.update_depth(data['data'])
        elif event == 'orderFill':  # 订单成交处理
            # logger.info(f"Order Fill : {data['data']}")
            self.handle_order_fill(data['data'])
        elif event == 'orderCancelled':
            # logger.info(f"Order Cancelled : {data['data']}")
            pass
        elif event == 'orderAccepted':
            self.handle_order_accepted(data['data'])
        elif event == 'orderExpired':
            logger.info(f"Order Expired : {data['data']}")
        else:
            logger.warning(f"Unhandled event type: {event}")
            logger.debug(f"Message: {data}")

    def on_ping(self, ws, message):
        logger.debug("Received ping, sending pong")
        self.ws.send_frame(ABNF.create_frame(message, ABNF.OPCODE_PONG))

    def on_error(self, ws, error):
        error_message = str(error)  # 将异常对象转换为字符串
        if '余额不足' in error_message:
            logger.error(f"Error 余额不足: {error}, 程序将撤销所有，并停止运行。")
        logger.error(f"WebSocket error: {error}")
        self.bpx.cancelAllOpenOrders(self.symbol)
        exit()

    def on_close(self, ws):
        logger.warning("WebSocket closed")
        self.ws.close()

    def on_open(self, ws):
        timestamp = int(time.time() * 1000)
        window = 5000
        verifying_key, signature = self.generate_signature(timestamp, window)
        self.bid_price, self.ask_price = self.get_bid_ask_price()
        auth_message = {
            "method": "SUBSCRIBE",
            "params": [f"depth.{self.symbol}", f"account.orderUpdate.{self.symbol}"],
            "signature": [verifying_key, signature, str(timestamp), str(window)]
        }
        self.send_message(json.dumps(auth_message))
        logger.info(f"WebSocket connection opened and subscribed to {auth_message['params']}")
        self.place_fist_order()
    
    def place_fist_order(self):

        while True:
            s = Status()  # 获取系统状态
            # 如果返回不是Ok，说明系统维护中，等待10秒后再次请求
            if s and s.get('status') != "Ok":
                logger.info("系统维护中...")
                time.sleep(10)
                continue
            else:
                break
        logger.info(f"当前价格: {self.bid_price} ~ {self.ask_price}")
        logger.info(f"网格区间: {self.min_price} ~ {self.max_price}")
        # 取消所有挂单
        self.bpx.cancelAllOpenOrders(self.symbol)
        mid_price = (self.bid_price + self.ask_price) / 2
        if self.bid_price > 0 and  self.ask_price > 0:
               # 创建新卖单
            buy_price = self.round_to(mid_price * (1 - float(self.gap_percent)), self.price_precision)
            self.buy_order = self.create_order(self.symbol, "Bid", "Limit", "GTC", self.quantity, buy_price)
            logger.info(f"创建新买单: clientId:{self.buy_order['clientId']}, id: {self.buy_order['id']}, price:{self.buy_order['price']}, quantity:{self.buy_order['quantity']}, side:{self.buy_order['side']}")
                # 创建新买单.
            sell_price = self.round_to(mid_price * (1 + float(self.gap_percent)), self.price_precision)
            self.sell_order = self.create_order(self.symbol, "Ask", "Limit", "GTC", self.quantity, sell_price)  # 下卖单
            logger.info(f"创建新卖单: clientId:{self.sell_order['clientId']}, id: {self.sell_order['id']}, price:{self.sell_order['price']}, quantity:{self.sell_order['quantity']}, side:{self.sell_order['side']}")


    
    def update_depth(self, data):
        update_id = int(data.get('u'))
 
        def update_side(side, old_data, updates):
            for update in updates:
                price, qty = float(update[0]), float(update[1])
                now_data = [entry for entry in old_data if entry[0] != price]
                if qty != 0:
                    now_data.append([price, qty])
            now_data.sort(key=lambda x: x[0], reverse=(side == "bids"))
            return now_data
        
        
        if update_id > self.depth.get('lastUpdateId'):
            self.depth["lastUpdateId"] = update_id
            if data['a']:
                self.depth["asks"] = update_side("asks", self.depth["asks"], data["a"])

            if data['b']:
                self.depth["bids"] = update_side("bids",self.depth["bids"], data["b"])
            
            self.ask_price = self.depth['asks'][0][0]
            self.bid_price = self.depth["bids"][0][0]
        
    def handle_order_fill(self, order):
        order_id = order.get('i')
        order_price = order.get('p')
        order_side = order.get('S')
        logger.success(f"订单成交, 成交时间: {datetime.now()}, 订单id:{order_id}, 订单类型:{order_side}, 价格: {order_price}, 数量: {order.get('l')}")
        r = self.bpx.cancelAllOpenOrders(self.symbol)
        # logger.debug(f'取消未成交订单, 结果: {r}')
        if order_id == self.buy_order.get("id"):  # 买单成交
            sell_price = self.round_to(float(order_price) * (1 + float(self.gap_percent)), self.price_precision)
            buy_price = self.round_to(float(order_price) * (1 - float(self.gap_percent)), self.price_precision)
            
        elif order_id == self.sell_order.get("id"):  # 卖单成交
            buy_price = self.round_to(float(order_price) * (1 - float(self.gap_percent)), self.price_precision)
            sell_price = self.round_to(float(order_price) * (1 + float(self.gap_percent)), self.price_precision)
       
        # 重新下买单和卖单
        if buy_price > self.bid_price > 0:
            buy_price = self.round_to(self.bid_price, self.price_precision)
        if 0 < sell_price < self.ask_price:
            sell_price = self.round_to(self.ask_price, self.price_precision)
        
        new_buy_order = self.create_order(symbol=self.symbol, side="Bid", orderType="Limit", timeInForce="GTC", quantity=self.quantity, price=buy_price)  # 下买单
        if new_buy_order:
            self.buy_order = new_buy_order
            logger.info(f"创建新买单: clientId:{self.buy_order['clientId']}, id: {self.buy_order['id']}, price:{self.buy_order['price']}, quantity:{self.buy_order['quantity']}, side:{self.buy_order['side']}")
        new_sell_order = self.create_order(symbol=self.symbol, side="Ask", orderType="Limit", timeInForce="GTC", quantity=self.quantity, price=sell_price)  # 下卖单
        if new_sell_order:
            self.sell_order = new_sell_order
            logger.info(f"创建新卖单: clientId:{self.sell_order['clientId']}, id: {self.sell_order['id']}, price:{self.sell_order['price']}, quantity:{self.sell_order['quantity']}, side:{self.sell_order['side']}")
    
    def handle_order_accepted(self, order):
        orderInfo = {
                        'clientId': order.get('c'),
                        'createdAt': None,
                        'executedQuantity': '0',
                        'executedQuoteQuantity': '0',
                        'id': order.get('i'),
                        'orderType': order.get('o'),
                        'postOnly': False,
                        'price': str(order.get('p')),
                        'quantity': str(order.get('q')),
                        'selfTradePrevention': 'RejectTaker',
                        'side': order.get('S'),
                        'status': 'New',
                        'symbol': order.get('s'),
                        'timeInForce': order.get('f'),
                        'triggerPrice': None
                    }
        if order.get('S') == 'Bid' and order.get('X') == 'New':
            self.buy_order = orderInfo
        elif order.get('S') == 'Ask' and order.get('X') == 'New':
            self.sell_order = orderInfo
        else:
            raise (f'收到未知订单类型: {order}')
    
    def create_order(self, symbol, side, orderType, timeInForce, quantity, price):
        """创建订单

        Args:
            symbol (_type_): 交易对
            side (_type_): 买卖方向
            orderType (_type_): 订单类型
            timeInForce (_type_): 有效期
            quantity (_type_): 数量
            price (_type_): 价格

        Returns:
            _type_: 返回订单信息
        """
        b1, b2 = self.get_balance()
        
        if price < self.min_price or price > self.max_price:
            logger.info(f"当前价格{price}不在网格下单范围内({self.min_price} ~ {self.max_price})，不下单")
            raise Exception(f"当前价格{price}不在网格下单范围内({self.min_price} ~ {self.max_price})，不下单")
        
        if side == 'Ask' and b1 < quantity:
            logger.error("卖单余额不足...")
            # 抛出异常
            raise Exception("卖单余额不足")
        
        if side == "Bid" and b2 < quantity * price:
            logger.error("买单余额不足...")
            raise Exception("买单余额不足")

        return self.bpx.ExeOrder(cid=self.get_client_id(), symbol=symbol, side=side, orderType=orderType, 
                              timeInForce=timeInForce, quantity=quantity, price=price)
    

    def create_ws_connection(self):
        self.logger.debug(f"Creating connection with WebSocket Server: {self.stream_url}")
        self.ws = create_connection(self.stream_url)
        self.logger.debug(f"WebSocket connection has been established: {self.stream_url}")
        self._callback(self.on_open)
        self.read_data()  # 开始读取数据

    def send_message(self, message):
        self.logger.debug(f"Sending message to WebSocket Server: {message}")
        self.ws.send(message)

    def read_data(self):
        while True:
            try:
                op_code, frame = self.ws.recv_data_frame(True)
            except WebSocketException as e:
                if isinstance(e, WebSocketConnectionClosedException):
                    self.logger.error("Lost websocket connection")
                else:
                    self.logger.error(f"Websocket exception: {e}")
                raise e
            except Exception as e:
                self.logger.error(f"Exception in read_data: {e}")
                raise e

            if op_code == ABNF.OPCODE_CLOSE:
                self.logger.warning("CLOSE frame received, closing websocket connection")
                self._callback(self.on_close)
                break
            elif op_code == ABNF.OPCODE_PING:
                self._callback(self.on_ping, frame.data)
                self.ws.send_frame(ABNF.create_frame(frame.data, ABNF.OPCODE_PONG))
                self.logger.debug("Received Ping; PONG frame sent back")
            elif op_code == ABNF.OPCODE_PONG:
                self.logger.debug("Received PONG frame")
                self._callback(self.on_pong)
            else:
                data = frame.data
                if op_code == ABNF.OPCODE_TEXT:
                    data = data.decode("utf-8")
                self._callback(self.on_message, data)

    def close(self):
        if not self.ws.connected:
            self.logger.warn("Websocket already closed")
        else:
            self.ws.send_close()

    def _callback(self, callback, *args):
        if callback:
            try:
                callback(self, *args)
            except Exception as e:
                self.logger.error(f"Error from callback {callback}:")
                self.logger.error(f"{e}")
                if self.on_error:
                    self.on_error(self, e)

if __name__ == "__main__":
    # 使用传入的参数创建SpotGrid实例
    grid = SpotGrid(
        api_key="lO5JHu2Js4EcyIIYdca5VkA9GDpC7iGudo1b6/yGABk=",
        secret="hYEdYmgWHGWWIn2nsyX5A8gVNxxQ6B8afGWkvaI1v6U=",
        symbol="SOL_USDC",
        max_price=130, # 网格上限
        min_price=120, # 网格下限
        gap_percent=0.0005, # 等比网格， 比率
        price_precision=2, # 价格精度
        quantity=0.01, # 每次下单数量
        quantity_precision=2, # 数量精度
        strategy_prefix="1" #  策略唯一编号，取值保守的话可以1~40，保证每个策略这个不同就行，这样可以运行多个网格
    )
    grid.start()

    while True:
        time.sleep(1)