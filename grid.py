from bpx.bpx import *
from bpx.bpx_pub import *
from datetime import datetime
import random
import string
from loguru import logger


api_key = "q3W9CxUuuDuWTa7eogoramAONlrm4ABTPmjhRe3XDqk="
secret = "epOkp5y3LwAZkXPJUWPQNCXcqyzP2atFiMEPD3Nd1FI="


class SpotGrid:
    def __init__(self) -> None:
        self.symbol = "SOL_USDC"
        self.max_price = 135  # 网格上限
        self.min_price = 125  # 网格下限
        self.gap_percent = 0.0001  # 等比网格，比率
        self.price_precision = 2  # 价格精度
        self.quantity = 0.01  # 每次下单数量
        self.quantity_precision = 2  # 数量精度

        self.depth = None  # 深度数据
        self.strategy_prefix = "2"  # 策略唯一编号，取值保守的话可以1~40，保证每个策略这个不同就行，这样可以运行多个网格

        self.buy_order = None
        self.sell_order = None
        self.bpx = BpxClient()
        self.bpx.init(api_key, secret)

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
            return float(self.depth['bids'][-1][0]), float(self.depth['asks'][0][0])
        else:
            return None, None
        
    def round_to(self, number, precision):
        return float(f'{number:.{precision}f}')
    
    def getOrderInfo(self, orderId):
        orders = self.bpx.getHistoryOrders(self.symbol)  # 获取历史订单
        for o in orders:
            if o.get("id") == orderId:
                return o
        return None
    
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
            return None
        
        if side == 'Ask' and b1 < quantity:
            logger.error("卖单余额不足...")
            return None
        
        if side == "Bid" and b2 < quantity * price:
            logger.error("买单余额不足...")
            return None
        return self.bpx.ExeOrder(cid=self.get_client_id(), symbol=symbol, side=side, orderType=orderType, 
                              timeInForce=timeInForce, quantity=quantity, price=price)
    
    def start_grid(self):
        """启动网格"""
        quantity = self.round_to(float(self.quantity), self.quantity_precision)
        logger.info(f"订单下单量调整为{quantity}")
        while True:
            try:
                s = Status()  # 获取系统状态
                # 如果返回不是Ok，说明系统维护中，等待10秒后再次请求
                if s and s.get('status') != "Ok":
                    logger.info("系统维护中...")
                    time.sleep(10)
                    continue
                
                # 获取价格
                bid_price, ask_price = self.get_bid_ask_price()
                logger.info(f"当前价格: {bid_price} ~ {ask_price}")
                if not bid_price or not ask_price:
                    logger.error("获取价格失败...")
                    time.sleep(5)
                    continue                
                # 判断有买有买单，没有买单则下买单
                if not self.buy_order:
                    price = self.round_to(bid_price * (1 - float(self.gap_percent)), self.price_precision)
                    buy_order = self.create_order(self.symbol, "Bid", "Limit", "GTC", quantity, price)
                    if buy_order:
                        self.buy_order = buy_order
                        logger.info(f"创建新买单: {self.buy_order}")
                    else:
                        logger.error(f"创建买单失败")
                        continue
                # 判断有卖单，没有卖单则下卖单
                # 没有卖单的时候.
                if not self.sell_order:
                    if ask_price > 0:
                        price = self.round_to(ask_price * (1 + float(self.gap_percent)), self.price_precision)
                        sell_order = self.create_order(self.symbol, "Ask", "Limit", "GTC", quantity, price)  # 下卖单
                        if sell_order:
                            self.sell_order = sell_order
                            logger.info(f"创建新卖单: {self.sell_order}")
                            time.sleep(1)
                        else:
                            logger.error("创建新卖单失败...")
                            continue
                # 查看买单信息
                check_buy_order = self.bpx.getOpenOrder(self.symbol, self.buy_order.get("id"))
                if not check_buy_order:
                    logger.error("买单已成交或者取消, 在历史订单中查找")
                    check_buy_order = self.getOrderInfo(self.buy_order.get("id"))
                
                if check_buy_order:
                    if check_buy_order.get('status') == 'Cancelled':  # 如果买单被取消了,将self.buy_order置为None,等待下一轮下单
                        self.buy_order = None
                        logger.info(f"买单 {buy_order.get('id')} 已取消，状态: {check_buy_order.get('status')}")
                    elif check_buy_order.get('status') == 'Filled':  # 如果买单已成交,下卖单
                        logger.info(f"买单成交时间: {datetime.now()}, 价格: {check_buy_order.get('price')}, 数量: {check_buy_order.get('quantity')}")
                        self.buy_order = None
                        
                        # 取消原有的卖单
                        r = self.bpx.cancelOrder(self.symbol, self.sell_order.get("id"))
                        logger.info(f"取消卖单: {self.sell_order.get('id')}, 结果: {r}")
                        self.sell_order
                        time.sleep(1)
                        
                        # 重新下买单和卖单
                        bid_price, ask_price = self.get_bid_ask_price()
                        sell_price = self.round_to(float(check_buy_order.get("price")) * (1 + float(self.gap_percent)), self.price_precision)

                        if 0 < sell_price < ask_price:
                            sell_price = self.round_to(ask_price, self.price_precision)

                        new_sell_order = self.create_order(symbol=self.symbol, side="Ask", orderType="Limit", 
                                                                            timeInForce="GTC", quantity=quantity, price=sell_price)  # 下卖单
                        if new_sell_order:
                            self.sell_order = new_sell_order
                            logger.info(f"创建新卖单: {self.sell_order}")
                        
                        buy_price = self.round_to(float(check_buy_order.get("price")) * (1 - float(self.gap_percent)),
                                        self.price_precision)
                        if buy_price > bid_price > 0:
                            buy_price = self.round_to(bid_price, self.price_precision)

                        new_buy_order = self.create_order(symbol=self.symbol, side="Bid", orderType="Limit", 
                                                        timeInForce="GTC", quantity=quantity, price=buy_price)  # 下买单
                        if new_buy_order:
                            self.buy_order = new_buy_order
                            logger.info(f"创建新买单: {self.buy_order}")
                else:
                    logger.error("买单查询失败")
                    # 抛出错误，等待5秒
                    
                            
                # 查看卖单信息
                check_sell_order = self.bpx.getOpenOrder(self.symbol, self.sell_order.get("id"))
                if not check_sell_order:
                    logger.error("卖单已成交或者取消, 在历史订单中查找")
                    check_sell_order = self.getOrderInfo(self.sell_order.get("id"))
                if check_sell_order:
                    if check_sell_order.get('status') == 'Cancelled':
                        self.sell_order = None
                        logger.info(f"卖单 {sell_order.get('id')} 已取消，状态: {check_sell_order.get('status')}")
                    elif check_sell_order.get('status') == "Filled":
                        logger.info(f"卖单成交时间: {datetime.now()}, 价格: {check_sell_order.get('price')}, 数量: {check_sell_order.get('quantity')}")
                        self.sell_order = None

                        # 取消买单
                        r = self.bpx.cancelOrder(self.symbol, self.buy_order.get("id"))
                        logger.info(f"开始取消买单，取消结果 {r}")
                        self.buy_order = None
                        time.sleep(1)

                        bid_price, ask_price = self.get_bid_ask_price()

                        # 卖单成交，先下买单.
                        buy_price = self.round_to(float(check_sell_order.get("price")) * (1 - float(self.gap_percent)), self.price_precision)
                        if buy_price > bid_price > 0:
                            buy_price = self.round_to(bid_price, self.price_precision)

                        new_buy_order = self.create_order(symbol=self.symbol, side="Bid", orderType="Limit", 
                                                        timeInForce="GTC", quantity=quantity, price=buy_price)  # 下买单
                        
                        if new_buy_order:
                            self.buy_order = new_buy_order
                            logger.info(f"创建新买单: {self.buy_order}")

                        sell_price = self.round_to(float(check_sell_order.get("price")) * (1 + float(self.gap_percent)), self.price_precision)

                        if 0 < sell_price < ask_price:
                            sell_price = self.round_to(ask_price, self.price_precision)

                        new_sell_order = self.create_order(symbol=self.symbol, side="Ask", orderType="Limit", 
                                                                            timeInForce="GTC", quantity=quantity, price=sell_price)  # 下卖单
                        if new_sell_order:
                            self.sell_order = new_sell_order
                            logger.info(f"创建新卖单: {self.sell_order}")
                else:
                    logger.error("卖单查询失败")
                    # 抛出错误，等待5秒
                    
                time.sleep(5)
            

            except Exception as ex:
                logger.error(f"异常了 {ex}")
                time.sleep(5)
                
    def test_order(self):
        """启动网格"""
        quantity = self.round_to(float(self.quantity), self.quantity_precision)
        logger.info(f"订单下单量调整为{quantity}")
        while True:
            try:
                s = Status()  # 获取系统状态
                # 如果返回不是Ok，说明系统维护中，等待10秒后再次请求
                if s and s.get('status') != "Ok":
                    logger.info("系统维护中...")
                    time.sleep(10)
                    continue
                
                # 获取价格
                bid_price, ask_price = self.get_bid_ask_price()
                logger.info(f"当前价格: {bid_price} ~ {ask_price}")
                if not bid_price or not ask_price:
                    logger.error("获取价格失败...")
                    time.sleep(5)
                    continue                
                # 判断有买有买单，没有买单则下买单
                if not self.buy_order:
                    price = self.round_to(bid_price * (1 - float(self.gap_percent)), self.price_precision)
                    buy_order = self.create_order(self.symbol, "Bid", "Limit", "GTC", quantity, price)
                    if buy_order:
                        self.buy_order = buy_order
                        logger.info(f"创建新买单: {self.buy_order}")
                    else:
                        logger.error(f"创建买单失败")
                        continue
                # 判断有卖单，没有卖单则下卖单
                # 没有卖单的时候.
                if not self.sell_order:
                    if ask_price > 0:
                        price = self.round_to(ask_price * (1 + float(self.gap_percent)), self.price_precision)
                        sell_order = self.create_order(self.symbol, "Ask", "Limit", "GTC", quantity, price)  # 下卖单
                        if sell_order:
                            self.sell_order = sell_order
                            logger.info(f"创建新卖单: {self.sell_order}")
                            time.sleep(1)
                        else:
                            logger.error("创建新卖单失败...")
                            continue
                # 查看买单信息
                logger.info(f"买单原始信息: {self.buy_order}")
                check_buy_order = self.bpx.getOpenOrder(self.symbol, self.buy_order.get("id"))
                if not check_buy_order:
                    logger.error("买单已成交或者取消, 在历史订单中查找")
                    check_buy_order = self.getOrderInfo(self.buy_order.get("id"))
                
                if check_buy_order:
                    logger.info(f"买单状态: 买单查询成功:{check_buy_order}")
                else:
                    logger.error("买单查询失败")
                    exit()
                    # 抛出错误，等待5秒
                    
                            
                # 查看卖单信息
                logger.info(f"卖单原始信息: {self.sell_order}")
                check_sell_order = self.bpx.getOpenOrder(self.symbol, self.sell_order.get("id"))
                if not check_sell_order:
                    logger.error("卖单已成交或者取消, 在历史订单中查找")
                    check_sell_order = self.getOrderInfo(self.sell_order.get("id"))
                if check_sell_order:
                    logger.info(f"卖单状态: 卖单查询成功:{check_sell_order}")
                else:
                    logger.error("卖单查询失败")
                    exit()
                    # 抛出错误，等待5秒
                    
                time.sleep(5)
            

            except Exception as ex:
                logger.error(f"异常了 {ex}")
                time.sleep(5)
    def test_order2(self):
        quantity = 0.01  # 每次下单数量
        price = 130
        order_info = self.bpx.ExeOrder(cid=self.get_client_id(), symbol=self.symbol, side="Bid", orderType="Limit", 
                              timeInForce="GTC", quantity=quantity, price=price)
        order_id = order_info.get("id")
        logger.info(f"下单信息: {order_info}")
        logger.info(f"订单ID: {order_id}")
        
        while True:
            order = self.getOrderInfo(order_id)
            if order:
                logger.info(f"订单信息: {order}")
                break
            else:
                logger.error("订单不存在")
                logger.info("等待10秒重试")
                time.sleep(10)

if __name__ == "__main__":
    grid = SpotGrid()
    # grid.start_grid()
    # grid.test_order()
    grid.test_order2()
    
   
