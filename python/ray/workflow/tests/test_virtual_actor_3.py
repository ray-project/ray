import pytest

import ray
from ray import workflow
from typing import Optional, Dict, Tuple, List


@ray.workflow.virtual_actor
class InventoryPrice:
    def __init__(self):
        self._prices: Dict[str, float] = {}

    @ray.workflow.virtual_actor.readonly
    def get_price(self, name) -> Optional[float]:
        return self._prices.get(name)

    def update_price(self, name: str, price: float) -> None:
        self._prices[name] = price

    @ray.workflow.virtual_actor.readonly
    def total_value(self, items) -> Tuple[float, List[str]]:
        unknown_items = []
        value = 0.0
        for (name, num) in items.items():
            price = self._prices.get(name)
            if price is None:
                unknown_items.append(name)
            else:
                value += price * num
        return value, unknown_items

    def __setstate__(self, prices):
        self._prices = prices

    def __getstate__(self):
        return self._prices


@ray.workflow.virtual_actor
class UserAccount:
    def __init__(self, inventory_id: str):
        self._goods: Dict[str, int] = {}
        self._balance: float = 0
        self._inventory_id = inventory_id

    def add_money(self, amount: float) -> float:
        if amount < 0:
            raise ValueError("amount can't be negative")
        self._balance += amount
        return self._balance

    def withdraw_money(self, amount: float) -> float:
        if amount < 0:
            raise ValueError("amount can't be negative")
        if amount > self._balance:
            raise ValueError("Withdraw more money than balance")
        self._balance -= amount
        return self._balance

    def buy(self, name: str, price: float):
        if price > self._balance:
            raise ValueError("Not enough balance")
        self._balance -= price
        if name not in self._goods:
            self._goods[name] = 0
        self._goods[name] += 1
        actor = workflow.get_actor(self._inventory_id)
        actor.update_price.run(name, price)
        return self._balance

    def sell(self, name: str, price: float):
        if name not in self._goods:
            raise ValueError("No such item")
        self._goods[name] -= 1
        if self._goods[name] == 0:
            self._goods.pop(name)
        self._balance += price
        actor = workflow.get_actor(self._inventory_id)
        actor.update_price.run(name, price)
        return self._balance

    def __setstate__(self, state):
        self._goods, self._balance, self._inventory_id = state

    def __getstate__(self):
        return self._goods, self._balance, self._inventory_id

    @workflow.virtual_actor.readonly
    def goods_value(self):
        actor = workflow.get_actor(self._inventory_id)
        return actor.total_value.run(self._goods)

    @workflow.virtual_actor.readonly
    def balance(self):
        return self._balance


@pytest.mark.parametrize(
    "workflow_start_regular",
    [
        {
            "num_cpus": 4,  # increase CPUs to add pressure
        }
    ],
    indirect=True,
)
def test_writer_actor_pressure_test(workflow_start_regular):
    inventory_actor = InventoryPrice.get_or_create("inventory")
    ray.get(inventory_actor.ready())
    user = UserAccount.get_or_create("user", "inventory")
    ray.get(user.ready())

    balance_1 = user.add_money.run_async(100)
    balance_2 = user.buy.run_async("item_1", 10)
    balance_3 = user.buy.run_async("item_1", 10)
    balance_4 = user.sell.run_async("item_1", 5)
    # get the result out of order
    assert ray.get(balance_1) == 100
    assert ray.get(balance_4) == 85
    assert ray.get(balance_2) == 90
    assert ray.get(balance_3) == 80
    assert user.balance.run() == 85
    assert user.goods_value.run() == (5, [])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
