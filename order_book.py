from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from models import Base, Order
engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()


def process_order(order):
    # Insert the order
    order_obj = Order(sender_pk=order['sender_pk'], receiver_pk=order['receiver_pk'],
                      buy_currency=order['buy_currency'], sell_currency=order['sell_currency'],
                      buy_amount=order['buy_amount'], sell_amount=order['sell_amount'], creator_id=order.get('creator_id'))

    result = session.query(Order).filter(Order.filled == None, Order.buy_currency == order['sell_currency'], Order.sell_currency == order['buy_currency'],
                                          Order.sell_amount/Order.buy_amount >= order['buy_amount']/order['sell_amount']).first()
    if result == None:
        session.add(order_obj)
        session.commit()
        return

    order_obj.filled = datetime.now()
    order_obj.counterparty_id = result.id

    session.add(order_obj)
    session.commit()

    result.filled = datetime.now()
    result.counterparty_id = order_obj.id
    session.commit()

    # print(result.id, result.counterparty_id, order_obj.id, order_obj.counterparty_id)

    if order_obj.buy_amount > result.sell_amount:
        new_buy_amount = order_obj.buy_amount - result.sell_amount
        new_sell_amount = new_buy_amount * result.buy_amount / result.sell_amount

        new_order = {'buy_currency': order_obj.buy_currency, 'sell_currency': order_obj.sell_currency,
                     'buy_amount': new_buy_amount, 'sell_amount': new_sell_amount, 'sender_pk': order_obj.sender_pk,
                     'receiver_pk': order_obj.receiver_pk, 'creator_id': order_obj.id}
        # print(new_buy_amount, new_sell_amount)
        process_order(new_order)

    if order_obj.buy_amount < result.sell_amount:
        new_sell_amount = result.sell_amount - order_obj.buy_amount
        new_buy_amount = new_sell_amount * result.buy_amount / result.sell_amount

        new_order = {'buy_currency': result.buy_currency, 'sell_currency': result.sell_currency,
                     'buy_amount': new_buy_amount, 'sell_amount': new_sell_amount, 'sender_pk': result.sender_pk,
                     'receiver_pk': result.receiver_pk, 'creator_id': result.id}
        # print(new_buy_amount, new_sell_amount)
        process_order(new_order)

    pass